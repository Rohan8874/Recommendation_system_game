import psycopg2, json, pathlib

conn = psycopg2.connect(
    host="127.0.0.1",
    dbname="games",
    user="postgres",
    password="Rohan$123",
)
cur = conn.cursor()


with pathlib.Path('data/steam_games_clean.json').open(encoding='utf-8') as f:
    games = json.load(f)

for g in games:
    cur.execute("""
        INSERT INTO games(id, app_name, title, url, release_date,
                          developer, publisher, genres, tags,
                          price, discount_price, early_access,
                          metascore, sentiment, specs, reviews_url)
        VALUES (%(id)s, %(app_name)s, %(title)s, %(url)s,
                %(release_date)s::date,
                %(developer)s, %(publisher)s, %(genres)s,
                %(tags)s, %(price)s, %(discount_price)s,
                %(early_access)s, %(metascore)s, %(sentiment)s,
                %(specs)s, %(reviews_url)s)
        ON CONFLICT (id) DO NOTHING;
    """, g)

with pathlib.Path('data/australian_users_items_clean.json').open(encoding='utf-8') as f:
    users = json.load(f)

for u in users:
    cur.execute("""
        INSERT INTO users(user_id, steam_id, items_count, user_url)
        VALUES (%(user_id)s, %(steam_id)s, %(items_count)s, %(user_url)s)
        ON CONFLICT (user_id) DO NOTHING;
    """, u)
    for it in u['items']:
        cur.execute("""
            INSERT INTO user_games(user_id, game_id,
                                   playtime_forever, playtime_2weeks)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (u['user_id'], it['item_id'],
              it['playtime_forever'], it.get('playtime_2weeks', 0)))

conn.commit()
cur.close()
conn.close()