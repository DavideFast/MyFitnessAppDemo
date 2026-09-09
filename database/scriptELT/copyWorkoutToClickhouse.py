import time
import json
import os
import signal
from datetime import date, datetime, time as dtime

import psycopg
import clickhouse_connect


# ============================================================
# ARRESTO CONTROLLATO
# ============================================================

class GracefulStop(Exception):
    """Sollevata alla ricezione di SIGTERM (stop del Job da Kubernetes)."""


def handle_sigterm(signum, frame):
    raise GracefulStop()


# ============================================================
# CONFIGURAZIONE POSTGRESQL
# ============================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "bigintensive"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


# ============================================================
# CONFIGURAZIONE CLICKHOUSE
# ============================================================

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "database": os.getenv("CLICKHOUSE_DB", "bigintensive"),
}
CLICKHOUSE_DATABASE = CLICKHOUSE_CONFIG["database"]


# ============================================================
# CONFIGURAZIONE BATCH
# ============================================================

# Numero di allenamenti letti per volta da PostgreSQL
POSTGRES_BATCH_SIZE = 10_000

# Numero di righe inviate per volta a ClickHouse
CLICKHOUSE_BATCH_SIZE = 10_000

# Numero massimo di allenamenti elaborati consecutivamente da ogni pod ELT.
ELT_CYCLE_WORKOUTS = int(os.getenv("ELT_CYCLE_WORKOUTS", "1000000"))

# Pausa dopo un ciclo completo, per ridurre il carico sul cluster.
ELT_CYCLE_PAUSE_SECONDS = int(os.getenv("ELT_CYCLE_PAUSE_SECONDS", "30"))

# Batch minimo dopo eventuali suddivisioni per errori memoria su ClickHouse
CLICKHOUSE_MIN_BATCH_SIZE = int(
    os.getenv("CLICKHOUSE_MIN_BATCH_SIZE", "500")
)

# Limite di sicurezza per evitare ricorsione infinita nei retry
CLICKHOUSE_MAX_SPLIT_ATTEMPTS = int(
    os.getenv("CLICKHOUSE_MAX_SPLIT_ATTEMPTS", "8")
)

# Numero di allenamenti RAW trasformati per finestra durante finalize.
FINALIZE_BATCH_WORKOUTS = int(
    os.getenv("FINALIZE_BATCH_WORKOUTS", "20000")
)

# Finestra minima RAW per split automatico in caso di memory error.
FINALIZE_MIN_BATCH_WORKOUTS = int(
    os.getenv("FINALIZE_MIN_BATCH_WORKOUTS", "500")
)

# Limite di sicurezza per split ricorsivi nella trasformazione finalize.
FINALIZE_MAX_SPLIT_ATTEMPTS = int(
    os.getenv("FINALIZE_MAX_SPLIT_ATTEMPTS", "8")
)

# Modalita' operative:
# - ingest: solo caricamento PostgreSQL -> RAW (nessuna trasformazione)
# - finalize: solo consolidamento RAW -> finale
ELT_RUN_MODE = os.getenv("ELT_RUN_MODE", "ingest").strip().lower()
ELT_GLOBAL_START_ID = os.getenv("ELT_GLOBAL_START_ID")
ELT_GLOBAL_END_ID = os.getenv("ELT_GLOBAL_END_ID")
ELT_TOTAL_WORKERS = int(os.getenv("ELT_TOTAL_WORKERS", "1"))
ELT_WORKER_INDEX = int(os.getenv("ELT_WORKER_INDEX", os.getenv("JOB_COMPLETION_INDEX", "0")))


# ============================================================
# CONNESSIONE POSTGRESQL
# ============================================================

def connect_postgres():

    return psycopg.connect(
        **POSTGRES_CONFIG
    )


# ============================================================
# CONNESSIONE CLICKHOUSE
# ============================================================

def connect_clickhouse():

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_CONFIG["host"],
        port=CLICKHOUSE_CONFIG["port"],
        username=CLICKHOUSE_CONFIG["username"],
        password=CLICKHOUSE_CONFIG["password"],
        database=CLICKHOUSE_CONFIG["database"],
    )


# ============================================================
# CREAZIONE TABELLA RAW
# ============================================================

def create_raw_table(ch):

    print("Controllo tabelle ClickHouse...")

    result = ch.query(
                f"""
        SELECT name, engine
        FROM system.tables
                WHERE database = '{CLICKHOUSE_DATABASE}'
          AND name IN ('allenamenti', 'allenamenti_raw')
        """
    )
    tables = {name: engine for name, engine in result.result_rows}
    expected = {
        "allenamenti": "Distributed",
        "allenamenti_raw": "Distributed",
    }
    missing_or_incorrect = [
        f"{name}={tables.get(name)!r} (atteso {engine})"
        for name, engine in expected.items()
        if tables.get(name) != engine
    ]
    if missing_or_incorrect:
        raise RuntimeError(
            "Schema ClickHouse non coerente: " + ", ".join(missing_or_incorrect)
        )

    print("Tabelle Distributed allenamenti e allenamenti_raw pronte.")


# ============================================================
# RECUPERO ULTIMO ID PRESENTE IN CLICKHOUSE
# ============================================================

def get_last_allenamento_id(ch, min_id=None, max_id=None):

    where_clause = ""
    if min_id is not None and max_id is not None:
        min_value = int(min_id)
        max_value = int(max_id)
        where_clause = f"WHERE allenamento_id >= {min_value} AND allenamento_id <= {max_value}"

    final_exists = ch.query(
                f"""
        SELECT count()
        FROM system.tables
                WHERE database = '{CLICKHOUSE_DATABASE}'
          AND name = 'allenamenti'
        """
    )

    raw_exists = ch.query(
                f"""
        SELECT count()
        FROM system.tables
                WHERE database = '{CLICKHOUSE_DATABASE}'
          AND name = 'allenamenti_raw'
        """
    )

    max_final = 0
    max_raw = 0

    if final_exists.result_rows and final_exists.result_rows[0][0] > 0:
        result_final = ch.query(
            f"""
            SELECT max(allenamento_id)
            FROM {CLICKHOUSE_DATABASE}.allenamenti
            {where_clause}
            """
        )
        value_final = result_final.result_rows[0][0]
        if value_final is not None:
            max_final = int(value_final)

    if raw_exists.result_rows and raw_exists.result_rows[0][0] > 0:
        result_raw = ch.query(
            f"""
            SELECT max(allenamento_id)
            FROM {CLICKHOUSE_DATABASE}.allenamenti_raw
            {where_clause}
            """
        )
        value_raw = result_raw.result_rows[0][0]
        if value_raw is not None:
            max_raw = int(value_raw)

    checkpoint = max(max_final, max_raw)
    return checkpoint


def get_worker_range():

    if ELT_GLOBAL_START_ID is None or ELT_GLOBAL_END_ID is None:
        return None

    global_start = int(ELT_GLOBAL_START_ID)
    global_end = int(ELT_GLOBAL_END_ID)

    if global_end < global_start:
        return None

    workers = max(1, ELT_TOTAL_WORKERS)
    index = max(0, ELT_WORKER_INDEX)

    total = (global_end - global_start) + 1
    chunk = (total + workers - 1) // workers
    worker_start = global_start + (index * chunk)
    worker_end = min(global_end, worker_start + chunk - 1)

    if worker_start > global_end:
        return None

    return worker_start, worker_end


def get_raw_row_count(ch):

    result = ch.query(
        f"""
        SELECT count()
        FROM {CLICKHOUSE_DATABASE}.allenamenti_raw
        """
    )

    value = result.result_rows[0][0]
    if value is None:
        return 0

    return int(value)


def get_raw_bounds(ch):

    result = ch.query(
        f"""
        SELECT
            count() AS total_rows,
            min(allenamento_id) AS min_id,
            max(allenamento_id) AS max_id
        FROM {CLICKHOUSE_DATABASE}.allenamenti_raw
        """
    )

    total_rows, min_id, max_id = result.result_rows[0]

    if not total_rows:
        return 0, None, None

    return int(total_rows), int(min_id), int(max_id)


# ============================================================
# CONVERSIONE JSONB → STRINGA JSON
# ============================================================

def json_to_string(value):

    if value is None:
        return "{}"

    # Psycopg normalmente restituisce già un dict
    # per una colonna JSONB.
    if isinstance(value, dict):

        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":")
        )

    # Caso in cui venga restituita una stringa
    if isinstance(value, str):

        return value

    # Fallback
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":")
    )


# ============================================================
# NORMALIZZAZIONE DATE → DATETIME
# ============================================================

def to_datetime(value):

    # Le colonne ClickHouse sono DateTime: un datetime.date puro non ha .timestamp().
    if value is None:
        return datetime(1970, 1, 1)

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    if isinstance(value, date):
        return datetime.combine(value, dtime.min)

    return value


# ============================================================
# INSERT BATCH CLICKHOUSE
# ============================================================

def insert_clickhouse_batch(
    ch,
    rows,
    split_depth=0
):

    if not rows:
        return

    try:
        ch.insert(
            f"{CLICKHOUSE_DATABASE}.allenamenti_raw",
            rows,

            column_names=[
                "allenamento_id",
                "athlete_id",
                "data_allenamento",
                "struttura_allenamento",
                "created_at"
            ]
        )
    except Exception as exc:
        error_text = str(exc).upper()
        is_memory_error = (
            "MEMORY_LIMIT_EXCEEDED" in error_text or
            "CODE: 241" in error_text or
            "MEMORY LIMIT" in error_text
        )

        cannot_split_more = (
            len(rows) <= CLICKHOUSE_MIN_BATCH_SIZE or
            split_depth >= CLICKHOUSE_MAX_SPLIT_ATTEMPTS
        )

        if (not is_memory_error) or cannot_split_more:
            raise

        midpoint = len(rows) // 2

        if midpoint == 0:
            raise

        print(
            "Memoria ClickHouse insufficiente per "
            f"{len(rows):,} righe: nuovo tentativo con 2 sottobatch "
            f"da circa {midpoint:,} righe."
        )

        insert_clickhouse_batch(
            ch,
            rows[:midpoint],
            split_depth + 1
        )

        insert_clickhouse_batch(
            ch,
            rows[midpoint:],
            split_depth + 1
        )


# ============================================================
# TRASFORMAZIONE RAW → TABELLA FINALE
# ============================================================

def transform_raw_data(ch):

    print("Esecuzione trasformazione allenamenti_raw → allenamenti...")

    total_rows, min_raw_id, max_raw_id = get_raw_bounds(ch)

    if total_rows == 0:
        print("Nessuna riga RAW da trasformare.")
        return

    print(
        f"RAW da trasformare: {total_rows:,} righe "
        f"(id {min_raw_id:,}..{max_raw_id:,})."
    )

    transformed_ranges = 0

    def transform_range(start_id, end_id, split_depth=0):
        nonlocal transformed_ranges

        if end_id < start_id:
            return

        try:
            ch.command(
                f"""
                INSERT INTO {CLICKHOUSE_DATABASE}.allenamenti
                (
                    allenamento_id,
                    athlete_id,
                    data_allenamento,
                    nome_esercizio,
                    serie_allenamento,
                    ripetizioni_allenamento,
                    recupero_allenamento,
                    peso_allenamento,
                    created_at
                )
                SELECT
                    r.allenamento_id,
                    r.athlete_id,
                    r.data_allenamento,
                    serie.1 AS nome_esercizio,
                    serie.2 AS serie_allenamento,
                    toUInt8(JSONExtractUInt(serie.3, 'ripetizioni')) AS ripetizioni_allenamento,
                    toUInt8(JSONExtractUInt(serie.3, 'recupero_secondi')) AS recupero_allenamento,
                    toDecimal32(JSONExtractFloat(serie.3, 'carico_kg'), 2) AS peso_allenamento,
                    r.created_at
                FROM {CLICKHOUSE_DATABASE}.allenamenti_raw AS r
                ARRAY JOIN
                    arrayFlatten(
                        arrayMap(
                            exercise -> arrayMap(
                                serie_numero ->
                                (
                                    JSONExtractString(exercise, 'nome'),
                                    serie_numero,
                                    exercise
                                ),
                                range(
                                    1,
                                    toUInt64(JSONExtractUInt(exercise, 'serie')) + 1
                                )
                            ),
                            JSONExtractArrayRaw(
                                r.struttura_allenamento,
                                'esercizi'
                            )
                        )
                    ) AS serie
                WHERE r.allenamento_id >= {int(start_id)}
                  AND r.allenamento_id <= {int(end_id)}
                  AND r.allenamento_id GLOBAL NOT IN
                  (
                      SELECT allenamento_id
                      FROM {CLICKHOUSE_DATABASE}.allenamenti
                      WHERE allenamento_id >= {int(start_id)}
                        AND allenamento_id <= {int(end_id)}
                  )
                """
            )

            transformed_ranges += 1
            print(
                "Range consolidato: "
                f"{start_id:,}..{end_id:,} "
                f"(blocchi completati: {transformed_ranges:,})."
            )
        except Exception as exc:
            error_text = str(exc).upper()
            is_memory_error = (
                "MEMORY_LIMIT_EXCEEDED" in error_text or
                "CODE: 241" in error_text or
                "MEMORY LIMIT" in error_text
            )

            range_size = (end_id - start_id) + 1
            can_split = (
                range_size > FINALIZE_MIN_BATCH_WORKOUTS and
                split_depth < FINALIZE_MAX_SPLIT_ATTEMPTS
            )

            if (not is_memory_error) or (not can_split):
                raise

            midpoint = start_id + (range_size // 2)
            left_end = midpoint - 1

            print(
                "Memoria insufficiente nella trasformazione range "
                f"{start_id:,}..{end_id:,}; split in "
                f"{start_id:,}..{left_end:,} e {midpoint:,}..{end_id:,}."
            )

            transform_range(start_id, left_end, split_depth + 1)
            transform_range(midpoint, end_id, split_depth + 1)

    current_start = min_raw_id

    while current_start <= max_raw_id:
        current_end = min(
            max_raw_id,
            current_start + FINALIZE_BATCH_WORKOUTS - 1,
        )

        transform_range(current_start, current_end)
        current_start = current_end + 1

    ch.command(
        f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.allenamenti_raw_local ON CLUSTER bigintensive_cluster"
    )

    final_count = ch.query(
        f"""
        SELECT count()
        FROM {CLICKHOUSE_DATABASE}.allenamenti
        """
    )

    raw_remaining = ch.query(
        f"""
        SELECT count()
        FROM {CLICKHOUSE_DATABASE}.allenamenti_raw
        """
    )

    print(
        "Risultato controllo ClickHouse: "
        f"allenamenti={int(final_count.result_rows[0][0]):,}, "
        f"raw_rimanenti={int(raw_remaining.result_rows[0][0]):,}"
    )

    print("Trasformazione ClickHouse completata.")


# ============================================================
# SINCRONIZZAZIONE
# ============================================================

def sync_allenamenti(
    pg,
    ch,
):

    print()
    print("=" * 70)
    print("SINCRONIZZAZIONE POSTGRESQL → CLICKHOUSE RAW")
    print("=" * 70)

    # --------------------------------------------------------
    # Se troviamo RAW residua da una run precedente, la
    # trasformiamo subito prima di continuare il trasferimento.
    # --------------------------------------------------------

    raw_rows_pending = get_raw_row_count(ch)
    if raw_rows_pending > 0:
        print(
            f"Rilevate {raw_rows_pending:,} righe in allenamenti_raw: verranno consolidate in finalize mode."
        )

    # --------------------------------------------------------
    # Se impostato, la replica lavora solo sul suo range
    # disgiunto (job indexed).
    # --------------------------------------------------------

    worker_range = get_worker_range()

    if worker_range is None and ELT_GLOBAL_START_ID is not None and ELT_GLOBAL_END_ID is not None:
        print("Range worker vuoto: nessun dato da elaborare per questa replica.")
        return

    if worker_range is not None:
        worker_start, worker_end = worker_range
        last_id = max(get_last_allenamento_id(ch, worker_start, worker_end), worker_start - 1)
        print(
            f"Worker index {ELT_WORKER_INDEX}/{max(1, ELT_TOTAL_WORKERS) - 1} range assegnato: {worker_start:,}..{worker_end:,}."
        )
        print(
            f"Checkpoint nel range: {last_id:,}"
        )
    else:
        last_id = get_last_allenamento_id(ch)
        print(
            f"Ultimo allenamento presente in ClickHouse: "
            f"{last_id:,}"
        )

    total_workouts = 0
    total_rows = 0

    with pg.cursor() as cur:
        while True:
            cycle_workouts = 0
            source_exhausted = False
            print(
                f"Avvio ciclo ELT: massimo {ELT_CYCLE_WORKOUTS:,} allenamenti."
            )

            while cycle_workouts < ELT_CYCLE_WORKOUTS:
                batch_limit = min(
                    POSTGRES_BATCH_SIZE,
                    ELT_CYCLE_WORKOUTS - cycle_workouts,
                )

                if worker_range is not None:
                    cur.execute(
                        """
                        SELECT
                            id,
                            athlete_id,
                            data_allenamento,
                            struttura_allenamento,
                            created_at
                        FROM allenamenti
                        WHERE id > %s
                          AND id <= %s
                        ORDER BY id
                        LIMIT %s
                        """,
                        (last_id, worker_end, batch_limit)
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            id,
                            athlete_id,
                            data_allenamento,
                            struttura_allenamento,
                            created_at
                        FROM allenamenti
                        WHERE id > %s
                        ORDER BY id
                        LIMIT %s
                        """,
                        (last_id, batch_limit)
                    )

                workouts = cur.fetchall()

                if not workouts:
                    source_exhausted = True
                    break

                rows = []
                for (
                    workout_id,
                    athlete_id,
                    workout_date,
                    workout_structure,
                    created_at,
                ) in workouts:
                    rows.append(
                        (
                            int(workout_id),
                            int(athlete_id),
                            to_datetime(workout_date),
                            json_to_string(workout_structure),
                            to_datetime(created_at),
                        )
                    )

                    if len(rows) >= CLICKHOUSE_BATCH_SIZE:
                        insert_clickhouse_batch(ch, rows)
                        total_rows += len(rows)
                        rows.clear()

                if rows:
                    insert_clickhouse_batch(ch, rows)
                    total_rows += len(rows)

                # Advance only after all rows from this PostgreSQL batch are flushed.
                last_id = workouts[-1][0]
                total_workouts += len(workouts)
                cycle_workouts += len(workouts)
                print(f"Ultimo ID trasferito: {last_id:,}")
                print(f"Allenamenti trasferiti in questa esecuzione: {total_workouts:,}")

                if worker_range is not None and last_id >= worker_end:
                    source_exhausted = True
                    break

                if len(workouts) < batch_limit:
                    source_exhausted = True
                    break

            if source_exhausted:
                if cycle_workouts == 0:
                    print("Nessun nuovo allenamento da trasferire.")
                break

            print(
                f"Ciclo completato: {cycle_workouts:,} allenamenti flushati. "
                f"Pausa di {ELT_CYCLE_PAUSE_SECONDS} secondi prima del prossimo ciclo."
            )
            time.sleep(ELT_CYCLE_PAUSE_SECONDS)

    # ========================================================
    # RISULTATO
    # ========================================================

    print()
    print("=" * 70)
    print("SINCRONIZZAZIONE COMPLETATA")
    print("=" * 70)

    print(
        f"Allenamenti trasferiti: "
        f"{total_workouts:,}"
    )

    print(
        f"Ultimo ID trasferito: "
        f"{last_id:,}"
    )

    print("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main():

    signal.signal(signal.SIGTERM, handle_sigterm)

    start_time = time.time()

    print()
    print("=" * 70)
    print("POSTGRESQL → CLICKHOUSE")
    print("MODALITÀ ELT - RAW")
    print("=" * 70)

    if ELT_RUN_MODE not in {"ingest", "finalize"}:
        raise ValueError(
            f"ELT_RUN_MODE non valido: {ELT_RUN_MODE}. Valori supportati: ingest, finalize"
        )

    print(f"Run mode: {ELT_RUN_MODE}")

    print(
        "Inizio:",
        datetime.now()
    )

    pg = None
    ch = None

    try:

        # ----------------------------------------------------
        # PostgreSQL
        # ----------------------------------------------------

        print()
        print("Connessione PostgreSQL...")

        pg = connect_postgres()

        print(
            "PostgreSQL connesso."
        )

        # ----------------------------------------------------
        # ClickHouse
        # ----------------------------------------------------

        print(
            "Connessione ClickHouse..."
        )

        ch = connect_clickhouse()

        print(
            "ClickHouse connesso."
        )

        # ----------------------------------------------------
        # Tabella RAW
        # ----------------------------------------------------

        create_raw_table(
            ch
        )

        if ELT_RUN_MODE == "finalize":
            raw_rows_pending = get_raw_row_count(ch)
            if raw_rows_pending > 0:
                print(
                    f"Finalize mode: trovate {raw_rows_pending:,} righe RAW, avvio consolidamento unico."
                )
                transform_raw_data(ch)
            else:
                print("Finalize mode: nessuna riga RAW da consolidare.")
        else:
            # ----------------------------------------------------
            # Sincronizzazione
            # ----------------------------------------------------

            sync_allenamenti(
                pg,
                ch,
            )


    except KeyboardInterrupt:

        print()
        print("=" * 70)
        print("INTERRUZIONE MANUALE")
        print("=" * 70)

        print(
            "Lo script è stato interrotto."
        )

    except GracefulStop:

        print()
        print("=" * 70)
        print("ARRESTO RICHIESTO (SIGTERM)")
        print("=" * 70)

        print(
            "Job fermato dall'esterno: i batch già inseriti restano validi."
        )

    except Exception as e:

        print()
        print("=" * 70)
        print("ERRORE")
        print("=" * 70)

        print(
            f"{type(e).__name__}: {e}"
        )

        raise

    finally:

        # ----------------------------------------------------
        # Chiusura PostgreSQL
        # ----------------------------------------------------

        if pg is not None:

            pg.close()

            print(
                "Connessione PostgreSQL chiusa."
            )

        # ----------------------------------------------------
        # Chiusura ClickHouse
        # ----------------------------------------------------

        if ch is not None:

            ch.close()

            print(
                "Connessione ClickHouse chiusa."
            )

    elapsed = (
        time.time() -
        start_time
    )

    print()
    print("=" * 70)
    print("FINE")
    print("=" * 70)

    print(
        f"Durata: "
        f"{elapsed / 60:.2f} minuti"
    )

    print(
        "Fine:",
        datetime.now()
    )


# ============================================================
# AVVIO
# ============================================================

if __name__ == "__main__":

    main()