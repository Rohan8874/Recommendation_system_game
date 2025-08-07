import psycopg2
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# ---------- Connect to PostgreSQL ----------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# ---------- Target User ----------
target_user_id = 'doctr'

# ---------- Helper: Parse TF-IDF Vector ----------
def parse_vector(vec):
    if isinstance(vec, str):
        return np.array([float(x.strip()) for x in vec.strip('{}').split(',')])
    return np.array(vec)

# ---------- Step 1: Get Top 10 Played Games ----------
cursor.execute("""
SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
""", (target_user_id,))
user_rows = cursor.fetchall()

user_games = []
user_vectors = []

for row in user_rows:
    item_id, item_name, tfidf = row
    user_games.append({'item_id': item_id, 'item_name': item_name})
    user_vectors.append(parse_vector(tfidf))

user_vectors = np.array(user_vectors)
avg_user_vector = np.mean(user_vectors, axis=0).tolist()
avg_vector_pg = f"[{','.join(f'{x:.6f}' for x in avg_user_vector)}]"

# ---------- Step 2: Top 10 Similar Games using pgvector <=> ----------
played_ids = tuple(g['item_id'] for g in user_games)

cursor.execute(f"""
SELECT id, app_name, tfidf_vec_vector
FROM games
WHERE id NOT IN %s
ORDER BY tfidf_vec_vector <=> %s::vector
LIMIT 10;
""", (played_ids, avg_vector_pg))

recommended_rows = cursor.fetchall()
recommended_names = []
recommended_vectors = []

for row in recommended_rows:
    rec_id, name, tfidf = row
    recommended_names.append(name)
    recommended_vectors.append(parse_vector(tfidf))

recommended_vectors = np.array(recommended_vectors)

# ---------- Step 3: Spearman's Rank Correlation ----------
spearman_scores = []
for i in range(min(len(user_vectors), len(recommended_vectors))):
    coef, _ = spearmanr(user_vectors[i], recommended_vectors[i])
    spearman_scores.append(coef)

avg_spearman = np.mean([s for s in spearman_scores if not np.isnan(s)])

# ---------- Step 4: Output ----------
print("Top 10 Played Games:")
for i, game in enumerate(user_games, 1):
    print(f"{i}. {game['item_name']}")

print("\nTop 10 Recommended Games:")
for i, name in enumerate(recommended_names, 1):
    print(f"{i}. {name}")

print("\nSpearman's Rank Correlation per Game:")
for i, score in enumerate(spearman_scores, 1):
    print(f"Game {i}: {score:.4f}")

print(f"\nAverage Spearman’s Correlation: {avg_spearman:.4f}")
