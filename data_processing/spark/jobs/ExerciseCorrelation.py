from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from itertools import combinations
from config import CLICKHOUSE_URL, CLICKHOUSE_PROPS


def main():


    #################################################################################
    ##                                                                             ##
    ##                       DATABASE CONNECTION AND RETRIEVE                      ##
    ##                                                                             ##
    #################################################################################

    spark = (
        SparkSession.builder.appName("exercise-correlation-analysis")
        .getOrCreate()
    )

    # Register the JDBC driver explicitly in the driver JVM.
    spark._jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    # set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    bounds_query = f"""
        (SELECT
            coalesce(min(athlete_id), 0) AS min_athlete_id,
            coalesce(max(athlete_id), 0) AS max_athlete_id
        FROM allenamenti) AS athlete_bounds
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

    if atleta_min == 0 and atleta_max == 0:
        print("Nessun allenamento disponibile per l'analisi.")
        spark.stop()
        return

    reader = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", "allenamenti")
        .option("user", CLICKHOUSE_PROPS["user"])
        .option("password", CLICKHOUSE_PROPS["password"])
        .option("driver", CLICKHOUSE_PROPS["driver"])
    )

    if atleta_min == atleta_max:
        df = reader.load()
    else:
        df = (
            reader
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

    
    df_ordinato = df.filter(col("peso_allenamento").isNotNull())

    window_spec = Window.partitionBy("athlete_id", "allenamento_id", "nome_esercizio") \
        .orderBy(F.col("peso_allenamento").desc(),F.col("ripetizioni_allenamento").desc())


    df_ordinato = df_ordinato.withColumn("row_number", row_number().over(window_spec)) \
        .filter(col("row_number") == 1) \
        .drop("row_number")


    
    

    df_organizzato = df_ordinato.withColumn("massimale_teorico", F.col("peso_allenamento") * (1 + F.col("ripetizioni_allenamento") / 30))

    df_organizzato = df_organizzato.drop("ripetizioni_allenamento","serie_allenamento","recupero_allenamento")


    SOGLIA_COPERTURA = 0.80
    MIN_ATLETI_PER_ESERCIZIO = 30
    MIN_SESSIONI_PER_ATLETA_ESERCIZIO = 3

    atleti_validi = (
        df_organizzato
        .groupBy("athlete_id", "nome_esercizio")
        .agg(F.countDistinct("allenamento_id").alias("numero_sessioni"))
        .filter(F.col("numero_sessioni") >= MIN_SESSIONI_PER_ATLETA_ESERCIZIO)
    )

    numero_atleti = atleti_validi.select("athlete_id").distinct().count()

    esercizi_ammessi = (
        atleti_validi
        .groupBy("nome_esercizio")
        .agg(F.countDistinct("athlete_id").alias("numero_atleti"))
        .filter(
            (F.col("numero_atleti") >= MIN_ATLETI_PER_ESERCIZIO)
            & (F.col("numero_atleti") >= numero_atleti * SOGLIA_COPERTURA)
        )
        .select("nome_esercizio")
    )

    df_organizzato = df_organizzato.join(
        esercizi_ammessi,
        on="nome_esercizio",
        how="inner",
    )


    #################################################################################################################
    ##                                                                                                             ##
    ##                              START OF TEMPORAL WINDOW PROCESSING                                            ##
    ##                                                                                                             ##
    #################################################################################################################

    df_a = df_organizzato.alias("a")
    df_b = df_organizzato.alias("b")

    df_joined_12 = df_a.join(df_b,
                           (col("a.athlete_id") == col("b.athlete_id"))
                           & (col("b.data_allenamento") <= col("a.data_allenamento")) 
                           & (col("a.nome_esercizio") == col("b.nome_esercizio"))
                           & (col("b.data_allenamento") >= F.add_months(col("a.data_allenamento"), -12)),
                           "inner")

    df_varianze_mobili = df_joined_12.groupBy("a.athlete_id", "a.allenamento_id", "a.nome_esercizio","a.data_allenamento","a.massimale_teorico") \
        .agg(F.variance("b.massimale_teorico").alias("varianza_12_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.add_months(col("a.data_allenamento"), -6),
                               col("b.massimale_teorico"))).alias("varianza_6_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.add_months(col("a.data_allenamento"), -3),
                               col("b.massimale_teorico"))).alias("varianza_3_mesi"),
             F.variance(F.when(col("b.data_allenamento") >= F.add_months(col("a.data_allenamento"), -1),
                               col("b.massimale_teorico"))).alias("varianza_1_mese"))
    


    df_preprocessing = df_varianze_mobili.select(
        "athlete_id",
        "allenamento_id",
        "nome_esercizio",
        "data_allenamento",
        "massimale_teorico",
        F.expr("stack(4, 'varianza_12_mesi', varianza_12_mesi, 'varianza_6_mesi', varianza_6_mesi, 'varianza_3_mesi', varianza_3_mesi, 'varianza_1_mese', varianza_1_mese) as (periodo, valore)")
    )
    df_preprocessing = df_preprocessing.withColumn("features_name", F.concat_ws("_", F.col("nome_esercizio"), F.col("periodo")))

    # Raggruppo solo per atleta: la correlazione confronta esercizi nella stessa finestra
    # temporale, non richiede che siano nella stessa sessione/data.
    periodi = [
        "varianza_12_mesi",
        "varianza_6_mesi",
        "varianza_3_mesi",
        "varianza_1_mese",
    ]
    esercizi = [
        row["nome_esercizio"]
        for row in esercizi_ammessi.select("nome_esercizio").distinct().collect()
    ]
    pivot_values = [
        f"{esercizio}_{periodo}"
        for esercizio in esercizi
        for periodo in periodi
    ]

    df_preprocessing = df_preprocessing.groupBy(
        "athlete_id",
    ).pivot("features_name", pivot_values) \
     .agg(F.avg("valore"))
    



    feature_columns = [col for col in df_preprocessing.columns if col not in ["athlete_id", "data_allenamento"]]

    MIN_OSSERVAZIONI_COPPIA = 30

    if len(feature_columns) < 2:
        print("Not enough features to compute correlation.")
        spark.stop()
        return

    pair_definitions = list(combinations(feature_columns, 2))
    aggregate_expressions = []

    for pair_index, (left_feature, right_feature) in enumerate(pair_definitions):
        valid_pair = (
            F.col(left_feature).isNotNull() & F.col(right_feature).isNotNull()
        )

        aggregate_expressions.extend([
            F.sum(F.when(valid_pair, 1).otherwise(0)).alias(f"pair_{pair_index}_count"),
            F.corr(F.when(valid_pair, F.col(left_feature)), F.when(valid_pair, F.col(right_feature))).alias(f"pair_{pair_index}_corr")
        ])

    pair_results = df_preprocessing.agg(*aggregate_expressions).first()

    print("Correlazioni Pearson per coppie di esercizi:")
    for pair_index, (left_feature, right_feature) in enumerate(pair_definitions):
        observations = pair_results[f"pair_{pair_index}_count"]
        correlation = pair_results[f"pair_{pair_index}_corr"]
        if observations is None or observations < MIN_OSSERVAZIONI_COPPIA:
            print(f"{left_feature} - {right_feature}: Not enough observations (only {observations or 0})")
            continue
        if correlation is None:
            print(f"{left_feature} - {right_feature}: Correlation could not be computed")
            continue
        print(f"{left_feature} - {right_feature}: Correlazione = {correlation}, Osservazioni = {observations}")

        
    spark.stop()

if __name__ == "__main__":
    main()




