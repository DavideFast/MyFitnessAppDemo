-- Tabella atleta
CREATE TABLE athletes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    cognome VARCHAR(100) NOT NULL,
    data_di_nascita DATE NOT NULL,
    sesso VARCHAR(10) CHECK (sesso IN ('M', 'F')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);

-- Tabella valori antropometrici
CREATE TABLE anthropometric_values  (
    athlete_id INT NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
    altezza_cm INT CHECK (altezza_cm > 50 AND altezza_cm < 300),
    peso_kg DECIMAL(5, 2) CHECK (peso_kg > 10 AND peso_kg < 500),
    data_rilevazione DATE NOT NULL,
    PRIMARY KEY (athlete_id, data_rilevazione)
);


-- Tabella allenamenti
CREATE TABLE allenamenti (
    id SERIAL PRIMARY KEY,
    athlete_id INT NOT NULL REFERENCES athletes(id) ON DELETE CASCADE ,
    data_allenamento DATE NOT NULL,
    tipo_allenamento VARCHAR(50) CHECK (tipo_allenamento IN ('forza', 'endurance', 'mobilità')),
    durata_minuti INT CHECK (durata_minuti > 0),
    struttura_allenamento JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Riepilogo corse
CREATE TABLE riepilogo_corse (
    id SERIAL PRIMARY KEY,
    athlete_id INT NOT NULL REFERENCES athletes(id) ON DELETE CASCADE ,
    velocita_media DECIMAL(6, 2) CHECK (velocita_media > 0),
    velocita_max DECIMAL(6, 2) CHECK (velocita_max > 0),
    frequenza_media DECIMAL(6, 2) CHECK (frequenza_media > 0),
    frequenza_max DECIMAL(6, 2) CHECK (frequenza_max > 0),
    cadenza DECIMAL (6,2) CHECK (cadenza > 0),
    distanza_km DECIMAL(8, 2) CHECK (distanza_km > 0),
    durata_minuti INT CHECK (durata_minuti > 0),
    data_corsa DATE NOT NULL,    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella esercizi

CREATE TABLE esercizi (
    id SERIAL PRIMARY KEY,
    nome_esercizio VARCHAR(150) NOT NULL,
    descrizione TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

