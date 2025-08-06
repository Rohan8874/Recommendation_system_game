import psycopg2
import numpy as np
from scipy.stats import spearmanr

# ---------- PostgreSQL Connection ----------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",  # change this to your password
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# ---------- Config ----------
target_user_id = 'doctr'  # replace with desired user_id

# ---------- Step 1: Get top 10 games played by user ----------
query_user_top_games = """
SELECT ui.item_id, ui.item_name, ui.playtime_forever, ui.playtime_2weeks, g.tfidf_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_user_top_games, (target_user_id,))
user_games = cursor.fetchall()

# ---------- Step 2: Get user count per game ----------
user_game_ids = [row[0] for row in user_games]
format_ids = ','.join(['%s'] * len(user_game_ids))

query_user_count = f"""
SELECT item_id, COUNT(DISTINCT user_id) as user_count
FROM user_items
WHERE item_id IN ({format_ids})
GROUP BY item_id;
"""
cursor.execute(query_user_count, user_game_ids)
user_counts = dict(cursor.fetchall())

# ---------- Step 3: Get top 10 globally popular games ----------
query_global_top_games = """
SELECT ui.item_id, g.app_name, COUNT(DISTINCT ui.user_id) as user_count, g.tfidf_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
GROUP BY ui.item_id, g.app_name, g.tfidf_vector
ORDER BY user_count DESC
LIMIT 10;
"""
cursor.execute(query_global_top_games)
global_top_games = cursor.fetchall()

# ---------- Step 4: TF-IDF Vector Parsing ----------
def parse_vector(vec_str):
    return np.array([float(x.strip()) for x in vec_str.strip('{}').split(',')]) if isinstance(vec_str, str) else np.array(vec_str)

user_vectors = [parse_vector(row[4]) for row in user_games]
global_vectors = [parse_vector(row[3]) for row in global_top_games]

# Ensure vectors are of same length
user_vectors = np.array(user_vectors)
global_vectors = np.array(global_vectors)
min_len = min(len(user_vectors), len(global_vectors))
user_vectors = user_vectors[:min_len]
global_vectors = global_vectors[:min_len]

# ---------- Step 5: Calculate Spearman Correlation ----------
# Average similarity per pair (same rank assumed)
spearman_results = []
for i in range(min_len):
    sim, _ = spearmanr(user_vectors[i], global_vectors[i])
    spearman_results.append(sim if not np.isnan(sim) else 0.0)

# Final average Spearman correlation score
average_spearman = round(np.mean(spearman_results), 4)

# ---------- Print Results ----------
print("\nTop 10 Played Games by User:")
for row in user_games:
    item_id = row[0]
    print(f"- {row[1]} | Played: {row[2]} min | Users Played: {user_counts.get(item_id, 0)}")

print("\nTop 10 Popular Games Globally:")
for row in global_top_games:
    print(f"- {row[1]} | Users Played: {row[2]}")

print("\nAverage Spearman Correlation Between User’s Games and Top Popular Games (TF-IDF):", average_spearman)
