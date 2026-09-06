from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg,  lag, countDistinct, first, row_number, to_date, radians, sin, cos, sqrt, atan2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import pyspark.sql.functions as F
from config import CLICKHOUSE_URL, CLICKHOUSE_PROPS, CLICKHOUSE_TABLE, POSTGRES_URL, POSTGRES_PROPS, POSTGRES_TABLE


def main():


    #################################################################################
    ##                                                                             ##
    ##                       DATABASE CONNECTION AND RETRIEVE                      ##
    ##                                                                             ##
    #################################################################################

    spark = (
        SparkSession.builder.appName("running-population-analysis")
        .getOrCreate()
    )

    # Register both JDBC drivers explicitly in the driver JVM.
    spark._jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    spark._jvm.java.lang.Class.forName("org.postgresql.Driver")

    # set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    bounds_query = f"""
        (SELECT
            coalesce(min(athlete_id), 0) AS min_athlete_id,
            coalesce(max(athlete_id), 0) AS max_athlete_id
        FROM {CLICKHOUSE_TABLE}) AS athlete_bounds
    """

    bounds = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", bounds_query)
        .option("user", CLICKHOUSE_PROPS["user"])
        .option("password", CLICKHOUSE_PROPS["password"])
        .option("driver", CLICKHOUSE_PROPS["driver"])
        .load()
        .first()
    )

    num_partizioni = 4
    atleta_min = bounds["min_athlete_id"]
    atleta_max = bounds["max_athlete_id"]

    df = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", "allenamenti")
        .option("user", CLICKHOUSE_PROPS["user"])
        .option("password", CLICKHOUSE_PROPS["password"])
        .option("driver", CLICKHOUSE_PROPS["driver"])
        .option("partitionColumn", "athlete_id")
        .option("lowerBound", atleta_min)
        .option("upperBound", atleta_max)
        .option("numPartitions", num_partizioni)
        .load()
    )

    df_postgres = (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver", POSTGRES_PROPS["driver"])
        .option("partitionColumn", "athlete_id")
        .option("lowerBound", atleta_min)
        .option("upperBound", atleta_max)
        .option("numPartitions", num_partizioni)
        .load()
    )

    #################################################################################
    ##                                                                             ##
    ##                              START OF DATA PROCESSING                       ##
    ##                                                                             ##
    #################################################################################

    df.show(5)
    df_postgres.show(5)


    df_ordinato = df.orderBy(col("athlete_id"), col("nome_esercizio"))
    df_ordinato = df_ordinato.filter(col("peso").isNotNull())
    df_ordinato.show(5)

    window_spec = Window.partitionBy("athlete_id", "allenamento_id", "nome_esercizio") \
        .orderBy(F.col("peso").desc(),F.col("ripetizioni").desc())


    df_ordinato = df_ordinato.withColumn("rank", F.rank().over(window_spec)) \
        .filter(col("rank") == 1) \
        .drop("rank")

    df_ordinato.show(5)

    
    

    df_organizzato = df_ordinato.withColumn("massimale_teorico", F.col("peso") * (1 + F.col("ripetizioni_allenamento") / 30))

    df_organizzato.show(5)

    df_organizzato = df_organizzato.drop("ripetizioni_allenamento","serie_allenamento","recupero_allenamento")

    df_organizzato.show(5)


    #################################################################################################################
    ##                                                                                                             ##
    ##                              START OF TEMPORAL WINDOW PROCESSING                                            ##
    ##                                                                                                             ##
    #################################################################################################################

    df_a = df_organizzato.alias("a")
    df_b = df_organizzato.alias("b")

    df_joined_12 = df_a.join(df_b,
                           (col("a.athlete_id") == col("b.athlete_id"))
                           & (col("a.allenamento_id") == col("b.allenamento_id")) 
                           & (col("a.nome_esercizio") == col("b.nome_esercizio"))
                           & (col("b.data_allenamento") >= F.expr("add_months(a.data_allenamento, -12)")),
                           "inner")

    df_varianze_mobili = df_joined_12.groupBy("a.athlete_id", "a.allenamento_id", "a.nome_esercizio","a.data_allenamento","a.massimale_teorico") \
        .agg(F.variance("b.massimale_teorico").alias("varianza_12_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.expr("add_months(a.data_allenamento, -6)"),
                               col("b.massimale_teorico"))).alias("varianza_6_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.expr("add_months(a.data_allenamento, -3)"),
                               col("b.massimale_teorico"))).alias("varianza_3_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.expr("add_months(a.data_allenamento, -1)"),
                               col("b.massimale_teorico"))).alias("varianza_1_mese"))
    

    df_varianze_mobili.show(5)

    df_preprocessing = df_varianze_mobili.select(
        "a.athlete_id",
        "a.allenamento_id",
        "a.nome_esercizio",
        "a.data_allenamento",
        "a.massimale_teorico",
        F.expr("stack(4, 'varianza_12_mesi', varianza_12_mesi, 'varianza_6_mesi', varianza_6_mesi, 'varianza_3_mesi', varianza_3_mesi, 'varianza_1_mese', varianza_1_mese) as (periodo, valore)")
    )
    df_preprocessing = df_preprocessing.withColumn("features_name", F.concat_ws("_", F.col("nome_esercizio"), F.col("periodo")))

    df_preprocessing = df_preprocessing.groupBy(
        "a.athlete_id",
        "a.data_allenamento",
    ).pivot("features_name") \
     .agg(F.first("valore"))
    
    df_preprocessing.show(5)

    df_pulita = df_preprocessing.dropna()
    df_pulita.show(5)

    feature_columns = [col for col in df_pulita.columns if col not in ["a.athlete_id", "a.data_allenamento"]]

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features",handleInvalid="skip")
    df_features = assembler.transform(df_pulita).select("features")
    df_features.show(5)

    matrix_row = Correlation.corr(df_features, "features",method="pearson").head()
    correlation_matrix = matrix_row[0]
    print("Correlation Matrix:")
    print(correlation_matrix)


    spark.stop()

if __name__ == "__main__":
    main()




