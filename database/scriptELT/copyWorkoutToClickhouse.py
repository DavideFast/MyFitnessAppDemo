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


# ============================================================
# CONFIGURAZIONE BATCH
# ============================================================

# Numero di allenamenti letti per volta da PostgreSQL
POSTGRES_BATCH_SIZE = 10_000

# Numero di righe inviate per volta a ClickHouse
CLICKHOUSE_BATCH_SIZE = 50_000

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
        """
        SELECT name, engine
        FROM system.tables
        WHERE database = 'bigintensive'
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
        """
        SELECT count()
        FROM system.tables
        WHERE database = 'bigintensive'
          AND name = 'allenamenti'
        """
    )

    raw_exists = ch.query(
        """
        SELECT count()
        FROM system.tables
        WHERE database = 'bigintensive'
          AND name = 'allenamenti_raw'
        """
    )

    max_final = 0
    max_raw = 0

    if final_exists.result_rows and final_exists.result_rows[0][0] > 0:
        result_final = ch.query(
            f"""
            SELECT max(allenamento_id)
            FROM bigintensive.allenamenti
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
            FROM bigintensive.allenamenti_raw
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
        """
        SELECT count()
        FROM bigintensive.allenamenti_raw
        """
    )

    value = result.result_rows[0][0]
    if value is None:
        return 0

    return int(value)


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
    rows
):

    if not rows:
        return

    ch.insert(
        "bigintensive.allenamenti_raw",
        rows,

        column_names=[
            "allenamento_id",
            "athlete_id",
            "data_allenamento",
            "struttura_allenamento",
            "created_at"
        ]
    )


# ============================================================
# TRASFORMAZIONE RAW → TABELLA FINALE
# ============================================================

def transform_raw_data(ch):

    print("Esecuzione trasformazione allenamenti_raw → allenamenti...")

    with open("script.sql", encoding="utf-8") as sql_file:
        sql = "\n".join(
            line
            for line in sql_file
            if not line.lstrip().startswith("--")
        )

    statements = [
        statement.strip()
        for statement in sql.split(";")
        if statement.strip()
    ]

    for statement in statements:
        if statement.lstrip().upper().startswith("SELECT"):
            result = ch.query(statement)
            if result.result_rows:
                print(f"Risultato controllo ClickHouse: {result.result_rows[0]}")
        else:
            ch.command(statement)

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

    # --------------------------------------------------------
    # Cursor PostgreSQL
    # --------------------------------------------------------

    with pg.cursor() as cur:

        while True:

            # ------------------------------------------------
            # Leggiamo solamente i nuovi record
            # ------------------------------------------------

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
                    (
                        last_id,
                        worker_end,
                        POSTGRES_BATCH_SIZE
                    )
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
                    (
                        last_id,
                        POSTGRES_BATCH_SIZE
                    )
                )

            workouts = cur.fetchall()

            # ------------------------------------------------
            # Non ci sono nuovi dati
            # ------------------------------------------------

            if not workouts:

                print()
                print(
                    "Nessun nuovo allenamento da trasferire."
                )

                break

            rows = []

            # ------------------------------------------------
            # Preparazione batch
            # ------------------------------------------------

            for (
                workout_id,
                athlete_id,
                workout_date,
                workout_structure,
                created_at
            ) in workouts:

                json_string = json_to_string(
                    workout_structure
                )

                rows.append(
                    (
                        int(workout_id),

                        int(athlete_id),

                        to_datetime(workout_date),

                        json_string,

                        to_datetime(created_at)
                    )
                )

                # ------------------------------------------------
                # Se raggiungiamo il limite del batch ClickHouse
                # ------------------------------------------------

                if len(rows) >= CLICKHOUSE_BATCH_SIZE:

                    print(
                        f"Inserimento di "
                        f"{len(rows):,} "
                        f"allenamenti in ClickHouse..."
                    )

                    insert_clickhouse_batch(
                        ch,
                        rows
                    )

                    total_rows += len(rows)

                    rows.clear()

            # ------------------------------------------------
            # Inseriamo il restante batch
            # ------------------------------------------------

            if rows:

                print(
                    f"Inserimento di "
                    f"{len(rows):,} "
                    f"allenamenti in ClickHouse..."
                )

                insert_clickhouse_batch(
                    ch,
                    rows
                )

                total_rows += len(rows)

                rows.clear()

            # ------------------------------------------------
            # Aggiorniamo il checkpoint logico
            #
            # L'ultimo ID viene aggiornato SOLO dopo che
            # l'intero batch è stato inserito con successo.
            # ------------------------------------------------

            last_id = workouts[-1][0]

            total_workouts += len(workouts)

            print(
                f"Ultimo ID trasferito: "
                f"{last_id:,}"
            )

            print(
                f"Allenamenti trasferiti in questa esecuzione: "
                f"{total_workouts:,}"
            )

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