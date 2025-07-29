import psycopg2
import csv
from datetime import datetime

def to_pg_array(lst):
    if not lst:
        return '{}'
    return '{' + ','.join('"' + v.replace('"', '\\"') + '"' for v in lst) + '}'

# DB credentials
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

with open('games.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # skip header

    for row in reader:
        id = row[0]
        app_name = row[1]
        title = row[2]
        url = row[3]

        try:
            release_date = datetime.strptime(row[4], '%Y-%m-%d').date() if row[4] else None
        except ValueError:
            release_date = None

        developer = row[5]
        publisher = row[6]
        genres = to_pg_array(row[7].split(',')) if row[7] else '{}'
        tags = to_pg_array(row[8].split(',')) if row[8] else '{}'

        try:
            price = float(row[9]) if row[9] else None
        except ValueError:
            price = None

        try:
            discount_price = float(row[10]) if row[10] else None
        except ValueError:
            discount_price = None

        early_access = row[11].lower() == 'true' if row[11] else None

        try:
            metascore = int(row[12]) if row[12] else None
        except ValueError:
            metascore = None

        sentiment = row[13]
        specs = to_pg_array(row[14].split(',')) if row[14] else '{}'
        reviews_url = row[15]

        cur.execute("""
            INSERT INTO games (
                id, app_name, title, url, release_date, developer, publisher,
                genres, tags, price, discount_price, early_access,
                metascore, sentiment, specs, reviews_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING;
        """, (
            id, app_name, title, url, release_date, developer, publisher,
            genres, tags, price, discount_price, early_access,
            metascore, sentiment, specs, reviews_url
        ))

conn.commit()
cur.close()
conn.close()
print("✅ games.csv successfully loaded into PostgreSQL.")
