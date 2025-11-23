CREATE TABLE IF NOT EXISTS raw_crypto (
    id SERIAL PRIMARY KEY,
    asset VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC,
    close NUMERIC,
    low NUMERIC,
    high NUMERIC,
    volume NUMERIC,
    sma7 NUMERIC,
    sma25 NUMERIC,
    sma99 NUMERIC,
    bb_bbm NUMERIC,
    bb_bbh NUMERIC,
    bb_bbl NUMERIC,
    psar NUMERIC,
    rsi NUMERIC
);

CREATE TABLE IF NOT EXISTS curated_crypto (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    price_usd NUMERIC
);
