import asyncio
import math
import os
import random
import time
from datetime import datetime, timezone

import httpx


# ============================================================
# CONFIGURAZIONE
# ============================================================

ENDPOINT_URL = os.getenv(
    "ENDPOINT_URL",
    "http://backend:3001/api/v1/pushToKafka",
)
NUM_ATHLETES = int(os.getenv("NUM_ATHLETES", "20000"))
SAMPLE_INTERVAL = float(os.getenv("SAMPLE_INTERVAL", "5.0"))
DANGER_EVENT_PROBABILITY = float(os.getenv("DANGER_EVENT_PROBABILITY", "0.05"))
DANGER_MIN_SAMPLES = int(os.getenv("DANGER_MIN_SAMPLES", "6"))
DANGER_MAX_SAMPLES = int(os.getenv("DANGER_MAX_SAMPLES", "18"))

if NUM_ATHLETES < 1:
    raise ValueError("NUM_ATHLETES deve essere almeno 1")
if SAMPLE_INTERVAL <= 0:
    raise ValueError("SAMPLE_INTERVAL deve essere maggiore di 0")
if not 0 <= DANGER_EVENT_PROBABILITY <= 1:
    raise ValueError("DANGER_EVENT_PROBABILITY deve essere compresa tra 0 e 1")
if DANGER_MIN_SAMPLES < 1 or DANGER_MAX_SAMPLES < DANGER_MIN_SAMPLES:
    raise ValueError("Intervallo durata evento pericoloso non valido")

# ------------------------------------------------------------
# SESSIONI
# ------------------------------------------------------------

MIN_SESSION_MINUTES = int(os.getenv("MIN_SESSION_MINUTES", "15"))
MAX_SESSION_MINUTES = int(os.getenv("MAX_SESSION_MINUTES", "120"))
MIN_REST_MINUTES = int(os.getenv("MIN_REST_MINUTES", "5"))
MAX_REST_MINUTES = int(os.getenv("MAX_REST_MINUTES", "60"))
INITIAL_START_WINDOW_MINUTES = int(
    os.getenv("INITIAL_START_WINDOW_MINUTES", "60")
)
MIN_SESSION_SAMPLES = 5

if MIN_SESSION_MINUTES < 1 or MAX_SESSION_MINUTES < MIN_SESSION_MINUTES:
    raise ValueError("Intervallo durata sessione non valido")
if MIN_REST_MINUTES < 0 or MAX_REST_MINUTES < MIN_REST_MINUTES:
    raise ValueError("Intervallo riposo non valido")
if INITIAL_START_WINDOW_MINUTES < 0:
    raise ValueError("INITIAL_START_WINDOW_MINUTES non puo' essere negativo")


# ============================================================
# HTTP
# ============================================================

MAX_CONNECTIONS = 2_000
MAX_KEEPALIVE_CONNECTIONS = 2_000

REQUEST_TIMEOUT = 10.0


# ============================================================
# RANDOM
# ============================================================

SEED = 42

random.seed(SEED)


# ============================================================
# STATISTICHE
# ============================================================

stats = {
    "requests": 0,
    "success": 0,
    "errors": 0,

    "samples": 0,
    "session_starts": 0,
    "session_ends": 0,

    "active_athletes": 0,

    "total_latency": 0.0,
}

stats_lock = asyncio.Lock()


# ============================================================
# STATO ATLETI
# ============================================================

athletes = {}


# ============================================================
# GENERAZIONE SESSION ID
# ============================================================

def generate_session_id():

    return random.randint(
        1_000_000,
        9_999_999_999
    )


# ============================================================
# INIZIALIZZAZIONE ATLETA
# ============================================================

def initialize_athlete(
    athlete_id
):

    # --------------------------------------------------------
    # Posizione iniziale
    # --------------------------------------------------------

    latitude = random.uniform(
        45.0,
        46.0
    )

    longitude = random.uniform(
        10.5,
        12.0
    )

    altitude = random.uniform(
        30.0,
        500.0
    )

    # --------------------------------------------------------
    # Parametri movimento
    #
    # Utilizzati SOLO internamente dal simulatore.
    # Non vengono inviati al backend.
    # --------------------------------------------------------

    speed_kmh = random.uniform(
        7.0,
        15.0
    )

    direction = random.uniform(
        0,
        360
    )

    # --------------------------------------------------------
    # Dati fisiologici
    # --------------------------------------------------------

    heart_rate = random.randint(
        125,
        165
    )

    cadence = random.randint(
        160,
        185
    )

    temperature = random.uniform(
        36.5,
        37.2
    )

    # --------------------------------------------------------
    # Stato sessione
    # --------------------------------------------------------

    athletes[athlete_id] = {

        "session_id": None,

        "sample_id": 0,

        "session_end": None,

        "next_session": None,

        "active": False,

        "latitude": latitude,

        "longitude": longitude,

        "altitude": altitude,

        "speed_kmh": speed_kmh,

        "direction": direction,

        "heart_rate": heart_rate,

        "cadence": cadence,

        "temperature": temperature,

        "danger_remaining_samples": 0,
    }


# ============================================================
# NUOVA SESSIONE
# ============================================================

def start_session(
    athlete
):

    athlete["session_id"] = (
        generate_session_id()
    )

    athlete["sample_id"] = 0

    # Durata casuale 15–120 minuti

    duration_minutes = random.uniform(
        MIN_SESSION_MINUTES,
        MAX_SESSION_MINUTES
    )

    athlete["session_end"] = (
        time.monotonic()
        + duration_minutes * 60
    )

    athlete["active"] = True

    # Reset di alcuni parametri fisiologici

    athlete["heart_rate"] = random.randint(
        120,
        160
    )

    athlete["cadence"] = random.randint(
        160,
        185
    )

    athlete["temperature"] = random.uniform(
        36.5,
        37.1
    )

    athlete["danger_remaining_samples"] = (
        random.randint(DANGER_MIN_SAMPLES, DANGER_MAX_SAMPLES)
        if random.random() < DANGER_EVENT_PROBABILITY
        else 0
    )


# ============================================================
# FINE SESSIONE
# ============================================================

def end_session(
    athlete
):

    athlete["active"] = False

    athlete["session_end"] = None

    # Pausa casuale prima della prossima sessione

    rest_minutes = random.uniform(
        MIN_REST_MINUTES,
        MAX_REST_MINUTES
    )

    athlete["next_session"] = (
        time.monotonic()
        + rest_minutes * 60
    )


# ============================================================
# MOVIMENTO
# ============================================================

def update_position(
    athlete
):

    speed_kmh = athlete[
        "speed_kmh"
    ]
    if athlete["danger_remaining_samples"] > 0:
        speed_kmh = min(speed_kmh, 0.1)

    direction = athlete[
        "direction"
    ]

    latitude = athlete[
        "latitude"
    ]

    # --------------------------------------------------------
    # Distanza percorsa nel campionamento
    # --------------------------------------------------------

    distance_km = (
        speed_kmh
        / 3600
        * SAMPLE_INTERVAL
    )

    direction_rad = math.radians(
        direction
    )

    # --------------------------------------------------------
    # Latitude
    # --------------------------------------------------------

    delta_latitude = (
        distance_km
        * math.cos(direction_rad)
        / 111.0
    )

    # --------------------------------------------------------
    # Longitude
    # --------------------------------------------------------

    longitude_scale = (
        111.0
        * math.cos(
            math.radians(latitude)
        )
    )

    delta_longitude = (
        distance_km
        * math.sin(direction_rad)
        / longitude_scale
    )

    athlete["latitude"] += (
        delta_latitude
    )

    athlete["longitude"] += (
        delta_longitude
    )

    # --------------------------------------------------------
    # Altitudine
    # --------------------------------------------------------

    athlete["altitude"] += random.uniform(
        -1.0,
        1.0
    )

    athlete["altitude"] = max(
        0,
        athlete["altitude"]
    )

    # --------------------------------------------------------
    # Velocità interna
    # --------------------------------------------------------

    if athlete["danger_remaining_samples"] > 0:
        athlete["speed_kmh"] *= random.uniform(0.35, 0.55)
        minimum_speed = 1.0
    else:
        athlete["speed_kmh"] += random.uniform(-0.20, 0.20)
        minimum_speed = 6.0

    athlete["speed_kmh"] = max(
        minimum_speed,
        min(
            athlete["speed_kmh"],
            18.0
        )
    )

    # --------------------------------------------------------
    # Direzione
    # --------------------------------------------------------

    athlete["direction"] += random.uniform(
        -8,
        8
    )

    athlete["direction"] %= 360


# ============================================================
# DATI FISIOLOGICI
# ============================================================

def update_physiology(
    athlete
):

    # --------------------------------------------------------
    # Battito
    # --------------------------------------------------------

    if athlete["danger_remaining_samples"] > 0:
        athlete["heart_rate"] += random.randint(4, 8)
    else:
        athlete["heart_rate"] += random.randint(-3, 3)

    athlete["heart_rate"] = max(
        110,
        min(
            athlete["heart_rate"],
            190
        )
    )

    # --------------------------------------------------------
    # Cadenza
    # --------------------------------------------------------

    if athlete["danger_remaining_samples"] > 0:
        athlete["cadence"] += random.randint(-15, -8)
    else:
        athlete["cadence"] += random.randint(-2, 2)

    athlete["cadence"] = max(
        145,
        min(
            athlete["cadence"],
            195
        )
    )

    # --------------------------------------------------------
    # Temperatura
    # --------------------------------------------------------

    athlete["temperature"] += random.uniform(
        -0.03,
        0.03
    )

    athlete["temperature"] = max(
        36.2,
        min(
            athlete["temperature"],
            38.5
        )
    )


# ============================================================
# GENERAZIONE SAMPLE
# ============================================================

def generate_sample(
    athlete_id,
    athlete,
    event_type="sample"
):

    # --------------------------------------------------------
    # Aggiornamento movimento
    # --------------------------------------------------------

    update_position(
        athlete
    )

    # --------------------------------------------------------
    # Aggiornamento dati fisiologici
    # --------------------------------------------------------

    update_physiology(
        athlete
    )

    # --------------------------------------------------------
    # Sample ID
    # --------------------------------------------------------

    athlete["sample_id"] += 1

    # --------------------------------------------------------
    # Payload
    # --------------------------------------------------------

    return {

        "sample_id": athlete[
            "sample_id"
        ],

        "athlete_id": athlete_id,

        "session_id": athlete[
            "session_id"
        ],

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "latitude": round(
            athlete["latitude"],
            6
        ),

        "longitude": round(
            athlete["longitude"],
            6
        ),

        "altitude": round(
            athlete["altitude"],
            1
        ),

        "heart_rate": athlete[
            "heart_rate"
        ],

        "cadence_spm": athlete[
            "cadence"
        ],

        "temperature": round(
            athlete["temperature"],
            2
        ),

        "event_type": event_type,
    }

    if athlete["danger_remaining_samples"] > 0:
        athlete["danger_remaining_samples"] -= 1


# ============================================================
# INVIO SAMPLE
# ============================================================

async def send_sample(
    client,
    athlete_id,
    athlete,
    event_type="sample"
):

    payload = generate_sample(
        athlete_id,
        athlete,
        event_type
    )

    start = time.perf_counter()

    try:

        response = await client.post(
            ENDPOINT_URL,
            json=payload
        )

        latency = (
            time.perf_counter()
            - start
        )

        async with stats_lock:

            stats["requests"] += 1

            stats["total_latency"] += (
                latency
            )

            if 200 <= response.status_code < 300:

                stats["success"] += 1

                if event_type == "sample":

                    stats["samples"] += 1

                elif event_type == "end":

                    stats["session_ends"] += 1

            else:

                stats["errors"] += 1

                print(
                    f"[HTTP "
                    f"{response.status_code}] "
                    f"athlete="
                    f"{athlete_id}"
                )

    except Exception as exc:

        async with stats_lock:

            stats["requests"] += 1

            stats["errors"] += 1

        print(
            f"[ERRORE] "
            f"athlete={athlete_id} "
            f"{type(exc).__name__}: "
            f"{exc}"
        )


# ============================================================
# SIMULAZIONE ATLETA
# ============================================================

async def simulate_athlete(
    client,
    athlete_id,
    initial_delay
):

    athlete = athletes[
        athlete_id
    ]

    # --------------------------------------------------------
    # Ritardo iniziale casuale
    # --------------------------------------------------------

    await asyncio.sleep(
        initial_delay
    )

    while True:

        # ====================================================
        # ATLETA INATTIVO
        # ====================================================

        if not athlete["active"]:

            # ------------------------------------------------
            # Prima sessione
            # ------------------------------------------------

            if athlete["next_session"] is None:

                start_session(
                    athlete
                )

                async with stats_lock:

                    stats[
                        "session_starts"
                    ] += 1

                    stats[
                        "active_athletes"
                    ] += 1

            # ------------------------------------------------
            # Attesa della prossima sessione
            # ------------------------------------------------

            else:

                remaining = (
                    athlete[
                        "next_session"
                    ]
                    - time.monotonic()
                )

                if remaining > 0:

                    await asyncio.sleep(
                        min(
                            remaining,
                            10
                        )
                    )

                    continue

                start_session(
                    athlete
                )

                async with stats_lock:

                    stats[
                        "session_starts"
                    ] += 1

                    stats[
                        "active_athletes"
                    ] += 1

        # ====================================================
        # SESSIONE ATTIVA
        # ====================================================

        if athlete["active"]:

            # -----------------------------------------------
            # Controlliamo se la sessione deve terminare
            # -----------------------------------------------

            remaining = (
                athlete[
                    "session_end"
                ]
                - time.monotonic()
            )

            if remaining <= 0 and athlete["sample_id"] >= MIN_SESSION_SAMPLES - 1:

                # -------------------------------------------
                # Ultimo campione
                # -------------------------------------------

                await send_sample(
                    client,
                    athlete_id,
                    athlete,
                    event_type="end"
                )

                # -------------------------------------------
                # Fine sessione
                # -------------------------------------------

                end_session(
                    athlete
                )

                async with stats_lock:

                    stats[
                        "active_athletes"
                    ] -= 1

                continue

            # -----------------------------------------------
            # Campione normale
            # -----------------------------------------------

            event_type = (
                "danger_scenario"
                if athlete["danger_remaining_samples"] > 0
                else "sample"
            )

            await send_sample(
                client,
                athlete_id,
                athlete,
                event_type=event_type
            )

            # -----------------------------------------------
            # Attesa 5 secondi
            # -----------------------------------------------

            await asyncio.sleep(
                SAMPLE_INTERVAL
            )


# ============================================================
# MONITOR
# ============================================================

async def monitor():

    previous_requests = 0

    previous_time = (
        time.perf_counter()
    )

    while True:

        await asyncio.sleep(5)

        now = time.perf_counter()

        async with stats_lock:

            current_requests = (
                stats["requests"]
            )

            success = (
                stats["success"]
            )

            errors = (
                stats["errors"]
            )

            samples = (
                stats["samples"]
            )

            session_starts = (
                stats["session_starts"]
            )

            session_ends = (
                stats["session_ends"]
            )

            active = (
                stats["active_athletes"]
            )

            total_latency = (
                stats["total_latency"]
            )

        elapsed = (
            now
            - previous_time
        )

        interval_requests = (
            current_requests
            - previous_requests
        )

        rps = (
            interval_requests
            / elapsed
            if elapsed > 0
            else 0
        )

        avg_latency = (
            total_latency
            / current_requests
            if current_requests > 0
            else 0
        )

        print(
            f"[MONITOR] "
            f"RPS={rps:,.0f} | "
            f"attivi={active:,} | "
            f"sample={samples:,} | "
            f"start={session_starts:,} | "
            f"end={session_ends:,} | "
            f"errori={errors:,} | "
            f"latency="
            f"{avg_latency * 1000:.1f} ms"
        )

        previous_requests = (
            current_requests
        )

        previous_time = now


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 75)
    print("SIMULATORE DISPOSITIVI RUNNING")
    print("=" * 75)

    print(
        f"Atleti:              "
        f"{NUM_ATHLETES:,}"
    )

    print(
        f"Campione ogni:       "
        f"{SAMPLE_INTERVAL:.0f} sec"
    )

    print(
        f"Durata sessione:     "
        f"{MIN_SESSION_MINUTES}-"
        f"{MAX_SESSION_MINUTES} min"
    )

    print(
        f"Riposo:              "
        f"{MIN_REST_MINUTES}-"
        f"{MAX_REST_MINUTES} min"
    )

    print(
        f"RPS teorici massimi: "
        f"{NUM_ATHLETES / SAMPLE_INTERVAL:,.0f}"
    )

    print(
        f"Endpoint:            "
        f"{ENDPOINT_URL}"
    )

    print("=" * 75)

    # --------------------------------------------------------
    # Inizializzazione
    # --------------------------------------------------------

    print(
        "Inizializzazione atleti..."
    )

    for athlete_id in range(
        1,
        NUM_ATHLETES + 1
    ):

        initialize_athlete(
            athlete_id
        )

    print(
        f"Creati "
        f"{NUM_ATHLETES:,} atleti."
    )

    # --------------------------------------------------------
    # HTTP client
    # --------------------------------------------------------

    limits = httpx.Limits(
        max_connections=MAX_CONNECTIONS,
        max_keepalive_connections=(
            MAX_KEEPALIVE_CONNECTIONS
        )
    )

    timeout = httpx.Timeout(
        REQUEST_TIMEOUT
    )

    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        headers={
            "Content-Type":
                "application/json"
        }
    ) as client:

        tasks = []

        # ----------------------------------------------------
        # Partenze iniziali distribuite
        # ----------------------------------------------------

        for athlete_id in range(
            1,
            NUM_ATHLETES + 1
        ):

            initial_delay = random.uniform(
                0,
                INITIAL_START_WINDOW_MINUTES
                * 60
            )

            tasks.append(
                asyncio.create_task(
                    simulate_athlete(
                        client,
                        athlete_id,
                        initial_delay
                    )
                )
            )

        print(
            f"Creati "
            f"{len(tasks):,} dispositivi simulati."
        )

        print(
            "Simulazione avviata."
        )

        print(
            "CTRL+C per terminare."
        )

        print()

        monitor_task = (
            asyncio.create_task(
                monitor()
            )
        )

        try:

            await asyncio.gather(
                *tasks
            )

        finally:

            monitor_task.cancel()

            for task in tasks:

                task.cancel()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print("=" * 75)
        print("SIMULAZIONE TERMINATA")
        print("=" * 75)