-- Active: 1753091497982@@127.0.0.1@5432
-- Run this in pgAdmin or psql

CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    steam_id TEXT,
    items_count INTEGER,
    user_url TEXT
);

CREATE TABLE user_items (
    user_id TEXT REFERENCES users(user_id),
    item_id TEXT,
    item_name TEXT,
    playtime_forever INTEGER,
    playtime_2weeks INTEGER,
    PRIMARY KEY (user_id, item_id)
);

CREATE TABLE games (
    id VARCHAR(50) PRIMARY KEY,
    app_name VARCHAR(255),
    title VARCHAR(255),
    url TEXT,
    release_date DATE,
    developer VARCHAR(255),
    publisher VARCHAR(255),
    genres TEXT[],
    tags TEXT[],
    price NUMERIC(10, 2),
    discount_price NUMERIC(10, 2),
    early_access BOOLEAN,
    metascore INTEGER,
    sentiment VARCHAR(50),
    specs TEXT[],
    reviews_url TEXT
);

SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM user_items;

SELECT COUNT(*) AS total_terms
FROM games;

CREATE TABLE IF NOT EXISTS user_play_ratio (
    user_id TEXT PRIMARY KEY,
    playtime_vector FLOAT8[]
);

CREATE EXTENSION IF NOT EXISTS playtime_vector;

CREATE EXTENSION vector;

ALTER TABLE user_play_ratio
ALTER COLUMN playtime_vector TYPE vector(100);

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'user_play_ratio' AND column_name = 'playtime_vector';

ALTER TABLE user_play_ratio
DROP COLUMN playtime_vector;

-- Then re-add it with the correct type
ALTER TABLE user_play_ratio
ADD COLUMN playtime_vector vector();  

ALTER TABLE user_play_ratio ADD COLUMN playtime_vector vector;


SELECT 
    user_id, 
    playtime_vector <=> (SELECT playtime_vector FROM user_play_ratio WHERE user_id = 'doctr') AS similarity
FROM user_play_ratio
WHERE user_id != 'doctr'
ORDER BY similarity ASC  
LIMIT 20;


SELECT * FROM items WHERE id != 1 ORDER BY embedding <-> (SELECT embedding FROM items WHERE id = 1) LIMIT 5;

