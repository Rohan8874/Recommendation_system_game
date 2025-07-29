-- Active: 1753091497982@@127.0.0.1@5432@postgres@public
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

SELECT * FROM games 
WHERE tfidf_vector @@ to_tsquery('english', 'strategy & indie');

SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM user_items;


SELECT COUNT(*) AS total_terms
FROM games;