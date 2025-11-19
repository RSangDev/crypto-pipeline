CREATE TABLE IF NOT EXISTS raw_crypto (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC
);

CREATE TABLE IF NOT EXISTS curated_crypto (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    price_usd NUMERIC
);
