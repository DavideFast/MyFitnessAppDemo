from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg,  lag, countDistinct, first, row_number, to_date, radians, sin, cos, sqrt, atan2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from config import CLICKHOUSE_URL, CLICKHOUSE_PROPS, CLICKHOUSE_TABLE, POSTGRES_URL, POSTGRES_PROPS, POSTGRES_TABLE


def main():

    spark = (
        SparkSession.builder.appName("running-population-analysis")
        .getOrCreate()
    )

    # Register both JDBC drivers explicitly in the driver JVM.
    spark._jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    spark._jvm.java.lang.Class.forName("org.postgresql.Driver")

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", CLICKHOUSE_PROPS["user"])
        .option("password", CLICKHOUSE_PROPS["password"])
        .option("driver", CLICKHOUSE_PROPS["driver"])
        .load()
    )

    df_postgres = (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver", POSTGRES_PROPS["driver"])
        .load()
    )

    df.show(5)
    df_postgres.show(5)

    
    finestra_temporale = Window.partitionBy("athlete_id", "session_id").orderBy("sample_id")
    finestra_temporale_5min = finestra_temporale.rowsBetween(-60, 0)

    

    df_ordinato = df.orderBy(col("athlete_id"), col("session_id"), col("sample_id"))
    df_ordinato.show(5)

    df_pulito = df_ordinato.dropDuplicates(["athlete_id", "session_id", "sample_id"])

    df_pulito.show(5)

    df_pulito_null = df_pulito.filter(
        col("athlete_id").isNotNull()
        & col("session_id").isNotNull()
        & col("sample_id").isNotNull()
        & col("heart_rate").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("timestamp").isNotNull()
    )

    df_pulito_null.show(5)


    df_postgres_aggiornato = df_postgres.withColumn("BMI", col("peso_kg") / (col("altezza_cm") * col("altezza_cm")/10000))
    df_postgres_aggiornato.show(5)

    # join posticipato: solo le colonne antropometriche necessarie, dopo i calcoli pesanti sulle window function
    df_postgres_ridotto = df_postgres_aggiornato.select("athlete_id", "data_rilevazione", "peso_kg", "altezza_cm", "BMI")
    df_postgres_ridotto.show(5)
    # Calcolo deriva cardiaca puntuale per valore antropometrico

    R=6371000  # Raggio della Terra in metri
    df_deriva_cardiaca = df_pulito_null \
        .withColumn("lat_prec", lag("latitude", 1).over(finestra_temporale)) \
        .withColumn("lon_prec", lag("longitude", 1).over(finestra_temporale)) \
        .withColumn("lat_rad", radians(col("latitude"))) \
        .withColumn("lon_rad", radians(col("longitude"))) \
        .withColumn("lat_prec_rad", radians(col("lat_prec"))) \
        .withColumn("lon_prec_rad", radians(col("lon_prec"))) \
        .withColumn("dlat", col("lat_rad") - col("lat_prec_rad")) \
        .withColumn("dlon", col("lon_rad") - col("lon_prec_rad")) \
        .withColumn("a", sin(col("dlat") / 2) ** 2 + cos(col("lat_rad")) * cos(col("lat_prec_rad")) * sin(col("dlon") / 2) ** 2) \
        .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))) \
        .withColumn("distanza", R * col("c")) \
        .withColumn("velocita_puntuale", col("distanza") / 5) \
        .withColumn("velocita_media", avg("velocita_puntuale").over(finestra_temporale_5min)) \
        .withColumn("frequenza_cardiaca_media", avg("heart_rate").over(finestra_temporale_5min)) \
        .withColumn("Efficienza_puntuale", col("velocita_puntuale") / col("frequenza_cardiaca_media")) \
        .withColumn("Efficienza_puntuale_iniziale", first("Efficienza_puntuale").over(finestra_temporale)) \
        .withColumn("Deriva_cardiaca_percentuale", (col("Efficienza_puntuale")- col("Efficienza_puntuale_iniziale")) / col("Efficienza_puntuale_iniziale") * 100)

    # Troviamo la velocità che causa la deriva cardiaca più alta per ogni atleta e sessione

    finestra_sessione_tempo = Window.partitionBy("athlete_id", "session_id").orderBy("timestamp")
    df_crisi_ordinate = df_deriva_cardiaca \
                .filter(col("Deriva_cardiaca_percentuale") > 5) \
                .withColumn("riga_crisi", row_number().over(finestra_sessione_tempo))

    primo_punto_di_crisi_per_sessione = df_crisi_ordinate \
        .filter(col("riga_crisi") == 1) \
        .select("timestamp", "velocita_media", "Deriva_cardiaca_percentuale", "athlete_id", "session_id")


    df_conteggio_corse = primo_punto_di_crisi_per_sessione.groupBy("athlete_id").agg(countDistinct("session_id").alias("numero_corse"))
    # join con Postgres eseguito qui: una sola riga per sessione, non tutte le righe di crisi
    crisi = primo_punto_di_crisi_per_sessione.alias("crisi")
    antropometria = df_postgres_ridotto.alias("antropometria")

    # Associa la misura piu recente disponibile prima della data della crisi.
    crisi_con_antropometria = crisi.join(
        antropometria,
        (col("crisi.athlete_id") == col("antropometria.athlete_id"))
        & (col("antropometria.data_rilevazione") <= to_date(col("crisi.timestamp"))),
        how="inner",
    )

    finestra_antropometria = Window.partitionBy(
        "crisi.athlete_id", "crisi.session_id"
    ).orderBy(col("antropometria.data_rilevazione").desc())

    primo_punto_di_crisi_per_sessione = (
        crisi_con_antropometria
        .withColumn("riga_antropometria", row_number().over(finestra_antropometria))
        .filter(col("riga_antropometria") == 1)
        .select(
            col("crisi.timestamp").alias("timestamp"),
            col("crisi.velocita_media").alias("velocita_media"),
            col("crisi.Deriva_cardiaca_percentuale").alias("Deriva_cardiaca_percentuale"),
            col("crisi.athlete_id").alias("athlete_id"),
            col("crisi.session_id").alias("session_id"),
            col("antropometria.peso_kg").alias("peso_kg"),
            col("antropometria.altezza_cm").alias("altezza_cm"),
            col("antropometria.BMI").alias("BMI"),
        )
    )

    
    df_preanalisi = primo_punto_di_crisi_per_sessione.join(df_conteggio_corse, ["athlete_id"], how="left")


    # Vediamo se c'è correlazione rispetto all'altezza, al peso o al BMI
    colonne_da_analizzare = ["peso_kg", "altezza_cm", "BMI", "velocita_media","numero_corse"]

    df_ml = df_preanalisi.select(colonne_da_analizzare).na.drop()

    righe_ml = df_ml.count()
    if righe_ml < 2:
        print(
            f"Correlazione non calcolabile: disponibili {righe_ml} righe complete, "
            "ne servono almeno 2."
        )
        spark.stop()
        return

    assembler = VectorAssembler(inputCols=colonne_da_analizzare, outputCol="features")
    df_ml = assembler.transform(df_ml)

    matrice_correlazione = Correlation.corr(df_ml, "features","pearson").head()[0]

    print("Matrice di correlazione:")
    print(matrice_correlazione)

    spark.stop()

if __name__ == "__main__":
    main()




