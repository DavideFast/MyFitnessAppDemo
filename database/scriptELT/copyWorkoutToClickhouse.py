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

def get_last_allenamento_id(ch):

    # La tabella finale viene creata da script.sql, quindi al primo run non esiste ancora.
    exists = ch.query(
        """
        SELECT count()
        FROM system.tables
        WHERE database = 'bigintensive'
          AND name = 'allenamenti'
        """
    )

    if not exists.result_rows or exists.result_rows[0][0] == 0:
        print("Tabella bigintensive.allenamenti non ancora presente: checkpoint = 0.")
        return 0

    result = ch.query(
        """
        SELECT max(allenamento_id)
        FROM bigintensive.allenamenti
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
    ch
):

    print()
    print("=" * 70)
    print("SINCRONIZZAZIONE POSTGRESQL → CLICKHOUSE RAW")
    print("=" * 70)

    # --------------------------------------------------------
    # Recuperiamo l'ultimo ID già trasferito
    # --------------------------------------------------------

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

        # ----------------------------------------------------
        # Sincronizzazione
        # ----------------------------------------------------

        sync_allenamenti(
            pg,
            ch
        )

        # Trasformiamo la staging RAW nella tabella finale dopo il trasferimento.
        transform_raw_data(ch)


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