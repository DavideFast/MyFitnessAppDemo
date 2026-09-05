-- ============================================================
-- CONFIGURAZIONE
-- ============================================================

CREATE DATABASE IF NOT EXISTS bigintensive;


-- ============================================================
-- CONTROLLO DATI RAW
-- ============================================================

SELECT
    count() AS allenamenti_da_processare,
    min(allenamento_id) AS primo_id,
    max(allenamento_id) AS ultimo_id
    FROM bigintensive.allenamenti_raw;


-- ============================================================
-- TRASFORMAZIONE
-- ============================================================

INSERT INTO bigintensive.allenamenti
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

    toUInt8(
        JSONExtractUInt(
            serie.3,
            'ripetizioni'
        )
    ) AS ripetizioni_allenamento,

    toUInt8(
        JSONExtractUInt(
            serie.3,
            'recupero_secondi'
        )
    ) AS recupero_allenamento,

    toDecimal32(
        JSONExtractFloat(
            serie.3,
            'carico_kg'
        ),
        2
    ) AS peso_allenamento,

    r.created_at

FROM bigintensive.allenamenti_raw AS r

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
    ) AS serie;


-- ============================================================
-- VERIFICA
-- ============================================================

SELECT
    count() AS righe_generate
FROM bigintensive.allenamenti;


-- ============================================================
-- SVUOTAMENTO STAGING
-- ============================================================

TRUNCATE TABLE bigintensive.allenamenti_raw;


-- ============================================================
-- CONTROLLO FINALE
-- ============================================================

SELECT
    count() AS raw_rimanenti
FROM bigintensive.allenamenti_raw;