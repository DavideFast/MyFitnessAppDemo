import random
import json
import os
from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal

import psycopg
from faker import Faker


# ============================================================
# CONFIGURAZIONE
# ============================================================

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "bigintensive"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

# ------------------------------------------------------------
# NUMERO ATLETI
# ------------------------------------------------------------

NUM_ATHLETES = int(os.getenv("NUM_ATHLETES", "50000"))


# ------------------------------------------------------------
# RILEVAZIONI ANTROPOMETRICHE
# ------------------------------------------------------------

MIN_ANTHROPOMETRIC = 4
MAX_ANTHROPOMETRIC = 8


# ------------------------------------------------------------
# ALLENAMENTI TOTALI PER ATLETA
#
# IMPORTANTE:
# questi valori rappresentano il numero TOTALE
# di allenamenti nel periodo 2021-2026.
# ------------------------------------------------------------

MIN_WORKOUTS = int(os.getenv("MIN_WORKOUTS", "750"))
MAX_WORKOUTS = int(os.getenv("MAX_WORKOUTS", "1500"))


# ------------------------------------------------------------
# FREQUENZA SETTIMANALE
#
# Usata per distribuire gli allenamenti.
#
# Il valore effettivo viene comunque adattato al numero
# totale di allenamenti richiesto.
# ------------------------------------------------------------

MIN_WORKOUTS_PER_WEEK = 2
MAX_WORKOUTS_PER_WEEK = 6


# ------------------------------------------------------------
# DIMENSIONE BATCH
# ------------------------------------------------------------

BATCH_SIZE = 10_000


# ------------------------------------------------------------
# PERIODO
# ------------------------------------------------------------

START_DATE = date(2021, 1, 1)
END_DATE = date(2026, 8, 16)


# ------------------------------------------------------------
# SEED
# ------------------------------------------------------------

SEED = 42


# ------------------------------------------------------------
# SVUOTAMENTO DATABASE
#
# False = non cancella dati esistenti
# True  = cancella le tabelle prima di iniziare
#
# ATTENZIONE:
# se True vengono cancellati anche i dati presenti in riepilogo_corse.
# ------------------------------------------------------------

CLEAR_EXISTING_DATA = os.getenv("CLEAR_EXISTING_DATA", "false").lower() == "true"


# ============================================================
# RANDOM
# ============================================================

random.seed(SEED)

fake = Faker("it_IT")
Faker.seed(SEED)


# ============================================================
# ESERCIZI
#
# NOTA:
# NON ESISTE PIÙ "tipo_esercizio".
# ============================================================

ESERCIZI = [

    # ========================================================
    # GAMBE
    # ========================================================

    (
        "Squat con bilanciere",
        "Squat con bilanciere libero"
    ),

    (
        "Squat frontale",
        "Squat frontale con bilanciere"
    ),

    (
        "Leg press",
        "Pressa per le gambe"
    ),

    (
        "Affondi con manubri",
        "Affondi eseguiti con manubri"
    ),

    (
        "Affondi bulgari",
        "Squat bulgaro con manubri"
    ),

    (
        "Stacco da terra",
        "Stacco da terra con bilanciere"
    ),

    (
        "Stacco rumeno",
        "Stacco rumeno per la catena posteriore"
    ),

    (
        "Leg extension",
        "Estensione delle gambe alla macchina"
    ),

    (
        "Leg curl",
        "Flessione delle gambe alla macchina"
    ),

    (
        "Calf raise",
        "Sollevamento dei polpacci"
    ),


    # ========================================================
    # PETTO
    # ========================================================

    (
        "Panca piana",
        "Distensione su panca piana con bilanciere"
    ),

    (
        "Panca inclinata",
        "Distensione su panca inclinata con bilanciere"
    ),

    (
        "Panca piana con manubri",
        "Distensione su panca piana con manubri"
    ),

    (
        "Chest press",
        "Spinta alla macchina per il petto"
    ),

    (
        "Croci con manubri",
        "Aperture con manubri"
    ),

    (
        "Croci ai cavi",
        "Aperture ai cavi"
    ),


    # ========================================================
    # DORSO
    # ========================================================

    (
        "Trazioni alla sbarra",
        "Trazioni a corpo libero alla sbarra"
    ),

    (
        "Lat machine",
        "Trazioni alla lat machine"
    ),

    (
        "Rematore con bilanciere",
        "Rematore con bilanciere"
    ),

    (
        "Rematore con manubrio",
        "Rematore con manubrio"
    ),

    (
        "Pulley basso",
        "Trazione orizzontale alla macchina"
    ),

    (
        "Pullover ai cavi",
        "Pullover ai cavi"
    ),


    # ========================================================
    # SPALLE
    # ========================================================

    (
        "Military press",
        "Distensione sopra la testa con bilanciere"
    ),

    (
        "Shoulder press",
        "Spinta per le spalle alla macchina"
    ),

    (
        "Alzate laterali",
        "Alzate laterali con manubri"
    ),

    (
        "Alzate frontali",
        "Alzate frontali con manubri"
    ),

    (
        "Face pull",
        "Trazione ai cavi per deltoidi posteriori"
    ),


    # ========================================================
    # BRACCIA
    # ========================================================

    (
        "Curl con bilanciere",
        "Curl per bicipiti con bilanciere"
    ),

    (
        "Curl con manubri",
        "Curl alternato con manubri"
    ),

    (
        "Hammer curl",
        "Curl a martello con manubri"
    ),

    (
        "Push down",
        "Estensione dei tricipiti ai cavi"
    ),

    (
        "French press",
        "Estensione dei tricipiti"
    ),

    (
        "Dip alle parallele",
        "Dip a corpo libero per petto e tricipiti"
    ),


    # ========================================================
    # CORE
    # ========================================================

    (
        "Crunch",
        "Crunch per la muscolatura addominale"
    ),

    (
        "Plank",
        "Esercizio isometrico per il core"
    ),

    (
        "Russian twist",
        "Rotazioni del tronco"
    ),

    (
        "Leg raise",
        "Sollevamento delle gambe"
    ),
]


# ============================================================
# GRUPPI MUSCOLARI
# ============================================================

GRUPPI_ESERCIZI = {

    "gambe": [
        "Squat con bilanciere",
        "Squat frontale",
        "Leg press",
        "Affondi con manubri",
        "Affondi bulgari",
        "Stacco da terra",
        "Stacco rumeno",
        "Leg extension",
        "Leg curl",
        "Calf raise",
    ],

    "petto": [
        "Panca piana",
        "Panca inclinata",
        "Panca piana con manubri",
        "Chest press",
        "Croci con manubri",
        "Croci ai cavi",
    ],

    "dorso": [
        "Trazioni alla sbarra",
        "Lat machine",
        "Rematore con bilanciere",
        "Rematore con manubrio",
        "Pulley basso",
        "Pullover ai cavi",
    ],

    "spalle": [
        "Military press",
        "Shoulder press",
        "Alzate laterali",
        "Alzate frontali",
        "Face pull",
    ],

    "braccia": [
        "Curl con bilanciere",
        "Curl con manubri",
        "Hammer curl",
        "Push down",
        "French press",
        "Dip alle parallele",
    ],

    "core": [
        "Crunch",
        "Plank",
        "Russian twist",
        "Leg raise",
    ],
}


# ============================================================
# RANGE CARICHI
# ============================================================

CARICHI_BASE = {

    "Squat con bilanciere": (40, 140),
    "Squat frontale": (30, 110),
    "Leg press": (80, 250),
    "Affondi con manubri": (8, 35),
    "Affondi bulgari": (8, 30),
    "Stacco da terra": (50, 160),
    "Stacco rumeno": (40, 130),
    "Leg extension": (20, 80),
    "Leg curl": (20, 80),
    "Calf raise": (30, 120),

    "Panca piana": (30, 120),
    "Panca inclinata": (25, 100),
    "Panca piana con manubri": (12, 45),
    "Chest press": (30, 100),
    "Croci con manubri": (6, 25),
    "Croci ai cavi": (5, 25),

    "Trazioni alla sbarra": (0, 20),
    "Lat machine": (30, 100),
    "Rematore con bilanciere": (30, 100),
    "Rematore con manubrio": (12, 45),
    "Pulley basso": (30, 100),
    "Pullover ai cavi": (15, 60),

    "Military press": (20, 70),
    "Shoulder press": (20, 70),
    "Alzate laterali": (4, 18),
    "Alzate frontali": (4, 18),
    "Face pull": (10, 40),

    "Curl con bilanciere": (15, 60),
    "Curl con manubri": (6, 25),
    "Hammer curl": (6, 25),
    "Push down": (15, 60),
    "French press": (15, 50),
    "Dip alle parallele": (0, 20),

    "Crunch": (0, 10),
    "Russian twist": (0, 16),
    "Leg raise": (0, 10),
}


# ============================================================
# FUNZIONI GENERICHE
# ============================================================

def random_date(start, end):

    if start > end:
        return None

    days = (end - start).days

    return start + timedelta(
        days=random.randint(0, days)
    )


def safe_date(year, month, day):

    try:
        return date(year, month, day)
    except ValueError:
        last_day = monthrange(year, month)[1]
        return date(year, month, min(day, last_day))


# ============================================================
# DATA DI NASCITA
# ============================================================

def random_birth_date():

    min_birth = safe_date(
        END_DATE.year - 55,
        END_DATE.month,
        END_DATE.day
    )

    max_birth = safe_date(
        END_DATE.year - 18,
        END_DATE.month,
        END_DATE.day
    )

    return random_date(
        min_birth,
        max_birth
    )


# ============================================================
# ALTEZZA
# ============================================================

def generate_height(sex):

    if sex == "M":

        value = random.gauss(
            178,
            7
        )

        return int(
            max(
                155,
                min(205, value)
            )
        )

    value = random.gauss(
        165,
        7
    )

    return int(
        max(
            145,
            min(195, value)
        )
    )


# ============================================================
# PESO
# ============================================================

def generate_weight(
    sex,
    height
):

    if sex == "M":

        bmi = random.gauss(
            24.5,
            3
        )

    else:

        bmi = random.gauss(
            22.5,
            3
        )

    weight = bmi * (
        (height / 100) ** 2
    )

    return round(
        max(
            45,
            min(150, weight)
        ),
        2
    )


# ============================================================
# ANTROPOMETRIA
# ============================================================

def generate_anthropometric_values(
    sex,
    birth_date
):

    number = random.randint(
        MIN_ANTHROPOMETRIC,
        MAX_ANTHROPOMETRIC
    )

    height = generate_height(
        sex
    )

    first_date = max(
        START_DATE,
        safe_date(
            birth_date.year + 18,
            birth_date.month,
            birth_date.day
        )
    )

    if first_date >= END_DATE:
        return []

    total_days = (
        END_DATE - first_date
    ).days

    number = min(
        number,
        total_days + 1
    )

    offsets = sorted(
        random.sample(
            range(total_days + 1),
            number
        )
    )

    weight = generate_weight(
        sex,
        height
    )

    result = []

    for index, offset in enumerate(offsets):

        measurement_date = (
            first_date +
            timedelta(days=offset)
        )

        if index > 0:

            weight += random.gauss(
                0,
                1.2
            )

        measured_weight = round(
            max(
                40,
                min(180, weight)
            ),
            2
        )

        result.append(
            (
                height,
                Decimal(
                    str(measured_weight)
                ),
                measurement_date
            )
        )

    return result


# ============================================================
# GENERAZIONE ESERCIZIO
# ============================================================

def generate_exercise_data(
    exercise_name,
    sex
):

    # --------------------------------------------------------
    # PLANK
    # --------------------------------------------------------

    if exercise_name == "Plank":

        return {
            "nome": exercise_name,
            "serie": random.randint(
                3,
                5
            ),
            "recupero_secondi": random.choice([
                20,
                40,
                60,
    ]),
            "durata_secondi": random.choice(
                [
                    30,
                    45,
                    60,
                    75,
                    90
                ]
            )
        }

    # --------------------------------------------------------
    # CARICO
    # --------------------------------------------------------

    min_weight, max_weight = (
        CARICHI_BASE[
            exercise_name
        ]
    )

    if sex == "F":

        min_weight *= 0.70
        max_weight *= 0.70

    weight = random.uniform(
        min_weight,
        max_weight
    )

    return {
        "nome": exercise_name,
        "serie": random.randint(
            3,
            5
        ),
        "ripetizioni": random.randint(
            6,
            15
        ),
        "recupero_secondi": random.choice([
        60,
        75,
        90,
        120,
        150,
        180
    ]),
        "carico_kg": round(
            weight,
            1
        )
    }


# ============================================================
# GENERAZIONE ALLENAMENTO
# ============================================================

def generate_strength_workout(
    athlete_id,
    workout_date,
    sex
):

    categoria = random.choice(
        [
            "full_body",
            "parte_superiore",
            "parte_inferiore",
            "ipertrofia",
            "forza"
        ]
    )

    # --------------------------------------------------------
    # SELEZIONE GRUPPI
    # --------------------------------------------------------

    if categoria == "full_body":

        groups = random.sample(
            list(
                GRUPPI_ESERCIZI.keys()
            ),
            4
        )

    elif categoria == "parte_superiore":

        groups = random.sample(
            [
                "petto",
                "dorso",
                "spalle",
                "braccia",
                "core"
            ],
            3
        )

    elif categoria == "parte_inferiore":

        groups = random.sample(
            [
                "gambe",
                "core"
            ],
            2
        )

    else:

        groups = random.sample(
            list(
                GRUPPI_ESERCIZI.keys()
            ),
            random.randint(
                2,
                4
            )
        )

    # --------------------------------------------------------
    # ESERCIZI
    # --------------------------------------------------------

    candidates = []

    for group in groups:

        candidates.extend(
            GRUPPI_ESERCIZI[group]
        )

    number_exercises = random.randint(
        5,
        min(
            8,
            len(candidates)
        )
    )

    selected = random.sample(
        candidates,
        number_exercises
    )

    exercises = []

    for exercise_name in selected:

        exercises.append(
            generate_exercise_data(
                exercise_name,
                sex
            )
        )

    # --------------------------------------------------------
    # JSONB
    # --------------------------------------------------------

    struttura = {

        "tipo": "palestra",

        "categoria": categoria,

        "esercizi": exercises
    }

    durata = random.randint(
        45,
        120
    )

    return (
        athlete_id,
        workout_date,
        "forza",
        durata,
        json.dumps(
            struttura,
            ensure_ascii=False
        )
    )


# ============================================================
# GENERAZIONE DATE ALLENAMENTI
#
# Questa è la parte importante:
#
# - 750-1500 allenamenti TOTALI
# - distribuiti per settimana
# - massimo 6 allenamenti/settimana
# - massimo 1 allenamento/giorno
# ============================================================

def generate_workout_dates(
    training_start,
    training_end,
    target_workouts
):

    if training_start >= training_end:

        return []

    available_days = (
        training_end -
        training_start
    ).days + 1

    # --------------------------------------------------------
    # Non possiamo superare un allenamento al giorno.
    # --------------------------------------------------------

    target_workouts = min(
        target_workouts,
        available_days
    )

    # --------------------------------------------------------
    # Creiamo tutte le settimane.
    #
    # Una settimana viene rappresentata come:
    # lista di date disponibili.
    # --------------------------------------------------------

    weeks = []

    current = training_start

    while current <= training_end:

        week_end = min(
            current + timedelta(days=6),
            training_end
        )

        days = []

        d = current

        while d <= week_end:

            days.append(d)

            d += timedelta(days=1)

        weeks.append(days)

        current = week_end + timedelta(days=1)

    if not weeks:

        return []

    # --------------------------------------------------------
    # Distribuzione iniziale.
    #
    # Partiamo da un allenamento per settimana e poi
    # aggiungiamo progressivamente gli altri.
    # --------------------------------------------------------

    selected_dates = []

    # settimane sufficienti
    # per almeno 2 allenamenti?
    minimum_per_week = MIN_WORKOUTS_PER_WEEK

    # --------------------------------------------------------
    # Prima distribuiamo il target in modo proporzionale
    # alle settimane disponibili.
    # --------------------------------------------------------

    number_of_weeks = len(weeks)

    # Frequenza media necessaria
    average_per_week = (
        target_workouts /
        number_of_weeks
    )

    # --------------------------------------------------------
    # Costruiamo il numero di allenamenti per ogni settimana.
    # --------------------------------------------------------

    weekly_counts = [
        max(
            1,
            min(
                MAX_WORKOUTS_PER_WEEK,
                int(
                    average_per_week
                )
            )
        )
        for _ in weeks
    ]

    # --------------------------------------------------------
    # Correzione del totale.
    # --------------------------------------------------------

    current_total = sum(
        weekly_counts
    )

    # Aggiungiamo allenamenti finché raggiungiamo il target.
    while current_total < target_workouts:

        possible = [
            i
            for i, count
            in enumerate(
                weekly_counts
            )
            if count <
            min(
                MAX_WORKOUTS_PER_WEEK,
                len(weeks[i])
            )
        ]

        if not possible:
            break

        index = random.choice(
            possible
        )

        weekly_counts[index] += 1

        current_total += 1

    # --------------------------------------------------------
    # Togliamo allenamenti se abbiamo superato il target.
    # --------------------------------------------------------

    while current_total > target_workouts:

        possible = [
            i
            for i, count
            in enumerate(
                weekly_counts
            )
            if count >
            MIN_WORKOUTS_PER_WEEK
        ]

        if not possible:
            break

        index = random.choice(
            possible
        )

        weekly_counts[index] -= 1

        current_total -= 1

    # --------------------------------------------------------
    # Generiamo le date effettive.
    # --------------------------------------------------------

    for week_index, days in enumerate(weeks):

        count = weekly_counts[
            week_index
        ]

        count = min(
            count,
            len(days)
        )

        chosen = random.sample(
            days,
            count
        )

        selected_dates.extend(
            chosen
        )

    selected_dates.sort()

    return selected_dates


# ============================================================
# SVUOTA DATABASE
# ============================================================

def clear_database(conn):

    print(
        "ATTENZIONE: cancellazione dati..."
    )

    with conn.cursor() as cur:

        cur.execute(
            """
            TRUNCATE TABLE
                riepilogo_corse,
                allenamenti,
                anthropometric_values,
                esercizi,
                athletes
            RESTART IDENTITY CASCADE
            """
        )

    conn.commit()

    print(
        "Database svuotato."
    )


# ============================================================
# ESERCIZI
# ============================================================

def insert_exercises(conn):

    print(
        "Inserimento esercizi..."
    )

    with conn.cursor() as cur:

        with cur.copy(
            """
            COPY esercizi
            (
                nome_esercizio,
                descrizione
            )
            FROM STDIN
            """
        ) as copy:

            for nome, descrizione in ESERCIZI:

                copy.write_row(
                    (
                        nome,
                        descrizione
                    )
                )

    conn.commit()

    print(
        f"Inseriti "
        f"{len(ESERCIZI)} esercizi."
    )


# ============================================================
# ATLETI
# ============================================================

def insert_athletes(conn):

    print(
        f"Generazione di "
        f"{NUM_ATHLETES:,} atleti..."
    )

    batch = []

    with conn.cursor() as cur:

        for athlete_id in range(
            1,
            NUM_ATHLETES + 1
        ):

            sesso = random.choice(
                [
                    "M",
                    "F"
                ]
            )

            if sesso == "M":

                nome = (
                    fake.first_name_male()
                )

            else:

                nome = (
                    fake.first_name_female()
                )

            cognome = fake.last_name()

            birth_date = (
                random_birth_date()
            )

            batch.append(
                (
                    athlete_id,
                    nome,
                    cognome,
                    birth_date,
                    sesso
                )
            )

            if len(batch) >= BATCH_SIZE:

                copy_athletes(
                    cur,
                    batch
                )

                conn.commit()

                print(
                    f"Atleti: "
                    f"{athlete_id:,}/"
                    f"{NUM_ATHLETES:,}"
                )

                batch.clear()

        if batch:

            copy_athletes(
                cur,
                batch
            )

            conn.commit()

    # --------------------------------------------------------
    # Aggiorna sequence SERIAL
    # --------------------------------------------------------

    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT setval(
                pg_get_serial_sequence(
                    'athletes',
                    'id'
                ),
                (
                    SELECT MAX(id)
                    FROM athletes
                )
            )
            """
        )

    conn.commit()

    print(
        "Atleti completati."
    )


def copy_athletes(
    cur,
    batch
):

    with cur.copy(
        """
        COPY athletes
        (
            id,
            nome,
            cognome,
            data_di_nascita,
            sesso
        )
        FROM STDIN
        """
    ) as copy:

        for row in batch:

            copy.write_row(row)


# ============================================================
# ANTROPOMETRIA
# ============================================================

def insert_anthropometric_values(
    conn
):

    print(
        "Generazione dati "
        "antropometrici..."
    )

    batch = []

    with conn.cursor() as read_cur, conn.cursor() as write_cur:

        read_cur.execute(
            """
            SELECT
                id,
                data_di_nascita,
                sesso
            FROM athletes
            ORDER BY id
            """
        )

        processed = 0

        while True:

            rows = read_cur.fetchmany(
                BATCH_SIZE
            )

            if not rows:
                break

            for (
                athlete_id,
                birth_date,
                sex
            ) in rows:

                values = (
                    generate_anthropometric_values(
                        sex,
                        birth_date
                    )
                )

                for (
                    height,
                    weight,
                    measurement_date
                ) in values:

                    batch.append(
                        (
                            athlete_id,
                            height,
                            weight,
                            measurement_date
                        )
                    )

                    if len(batch) >= BATCH_SIZE:

                        copy_anthropometry(
                            write_cur,
                            batch
                        )

                        conn.commit()

                        batch.clear()

            processed += len(rows)

            print(
                f"Antropometria: "
                f"{processed:,}/"
                f"{NUM_ATHLETES:,}"
            )

        if batch:

            copy_anthropometry(
                write_cur,
                batch
            )

            conn.commit()

    print(
        "Antropometria completata."
    )


def copy_anthropometry(
    cur,
    batch
):

    with cur.copy(
        """
        COPY anthropometric_values
        (
            athlete_id,
            altezza_cm,
            peso_kg,
            data_rilevazione
        )
        FROM STDIN
        """
    ) as copy:

        for row in batch:

            copy.write_row(row)


# ============================================================
# ALLENAMENTI
# ============================================================

def insert_workouts(conn):

    print()
    print(
        "Generazione allenamenti..."
    )

    print(
        f"Range per atleta: "
        f"{MIN_WORKOUTS} - "
        f"{MAX_WORKOUTS}"
    )

    print(
        "Tipo: SOLO PALESTRA / FORZA"
    )

    print()

    batch = []

    total_workouts = 0
    processed = 0

    with conn.cursor() as read_cur, conn.cursor() as write_cur:

        read_cur.execute(
            """
            SELECT
                id,
                data_di_nascita,
                sesso
            FROM athletes
            ORDER BY id
            """
        )

        while True:

            rows = read_cur.fetchmany(
                BATCH_SIZE
            )

            if not rows:
                break

            for (
                athlete_id,
                birth_date,
                sex
            ) in rows:

                # ------------------------------------------------
                # Inizio attività.
                #
                # L'atleta non può allenarsi prima dei 18 anni.
                # ------------------------------------------------

                training_start = max(
                    START_DATE,
                    safe_date(
                        birth_date.year + 18,
                        birth_date.month,
                        birth_date.day
                    )
                )

                if training_start >= END_DATE:

                    continue

                # ------------------------------------------------
                # Numero totale di allenamenti.
                # ------------------------------------------------

                target_workouts = random.randint(
                    MIN_WORKOUTS,
                    MAX_WORKOUTS
                )

                # ------------------------------------------------
                # Generazione delle date.
                #
                # Distribuzione settimanale.
                # ------------------------------------------------

                workout_dates = (
                    generate_workout_dates(
                        training_start,
                        END_DATE,
                        target_workouts
                    )
                )

                for workout_date in workout_dates:

                    workout = (
                        generate_strength_workout(
                            athlete_id,
                            workout_date,
                            sex
                        )
                    )

                    batch.append(
                        workout
                    )

                    total_workouts += 1

                    # ------------------------------------------------
                    # SALVATAGGIO A BLOCCHI
                    # ------------------------------------------------

                    if len(batch) >= BATCH_SIZE:

                        copy_workouts(
                            write_cur,
                            batch
                        )

                        conn.commit()

                        batch.clear()

                        print(
                            f"Allenamenti inseriti: "
                            f"{total_workouts:,}"
                        )

            processed += len(rows)

            print(
                f"Atleti processati: "
                f"{processed:,}/"
                f"{NUM_ATHLETES:,}"
            )

        # --------------------------------------------------------
        # Ultimo batch
        # --------------------------------------------------------

        if batch:

            copy_workouts(
                write_cur,
                batch
            )

            conn.commit()

            batch.clear()

    # ------------------------------------------------------------
    # Aggiorna sequence
    # ------------------------------------------------------------

    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT setval(
                pg_get_serial_sequence(
                    'allenamenti',
                    'id'
                ),
                (
                    SELECT MAX(id)
                    FROM allenamenti
                )
            )
            """
        )

    conn.commit()

    print()
    print(
        "Allenamenti completati."
    )

    print(
        f"Totale allenamenti: "
        f"{total_workouts:,}"
    )


def copy_workouts(
    cur,
    batch
):

    with cur.copy(
        """
        COPY allenamenti
        (
            athlete_id,
            data_allenamento,
            tipo_allenamento,
            durata_minuti,
            struttura_allenamento
        )
        FROM STDIN
        """
    ) as copy:

        for row in batch:

            copy.write_row(row)


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("GENERATORE DATASET ATLETI")
    print("=" * 70)

    print(
        f"Atleti:                 "
        f"{NUM_ATHLETES:,}"
    )

    print(
        f"Allenamenti per atleta: "
        f"{MIN_WORKOUTS} - "
        f"{MAX_WORKOUTS}"
    )

    print(
        f"Allenamenti/settimana:  "
        f"{MIN_WORKOUTS_PER_WEEK} - "
        f"{MAX_WORKOUTS_PER_WEEK}"
    )

    print(
        "Tipo allenamento:       SOLO FORZA"
    )

    print(
        "Corsa:                   NO"
    )

    print(
    )

    print(
        f"Periodo:                "
        f"{START_DATE} → {END_DATE}"
    )

    print(
        f"Batch:                   "
        f"{BATCH_SIZE:,}"
    )

    print(
        f"Seed:                    "
        f"{SEED}"
    )

    print("=" * 70)
    print()

    with psycopg.connect(
        **DB_CONFIG
    ) as conn:

        # ----------------------------------------------------
        # SVUOTAMENTO OPZIONALE
        # ----------------------------------------------------

        if CLEAR_EXISTING_DATA:

            clear_database(
                conn
            )

        # ----------------------------------------------------
        # 1. ESERCIZI
        # ----------------------------------------------------

        insert_exercises(
            conn
        )

        # ----------------------------------------------------
        # 2. ATLETI
        # ----------------------------------------------------

        insert_athletes(
            conn
        )

        # ----------------------------------------------------
        # 3. ANTROPOMETRIA
        # ----------------------------------------------------

        insert_anthropometric_values(
            conn
        )

        # ----------------------------------------------------
        # 4. ALLENAMENTI
        # ----------------------------------------------------

        insert_workouts(
            conn
        )

    print()
    print("=" * 70)
    print("GENERAZIONE COMPLETATA")
    print("=" * 70)
    print()

    print("Tabelle popolate:")
    print("  ✓ athletes")
    print("  ✓ anthropometric_values")
    print("  ✓ allenamenti")
    print("  ✓ esercizi")

    print()

    print("Tabelle NON popolate:")
    print("  - riepilogo_corse")

    print()


# ============================================================
# AVVIO
# ============================================================

if __name__ == "__main__":
    main()