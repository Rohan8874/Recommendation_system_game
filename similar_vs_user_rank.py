import psycopg2
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics.pairwise import cosine_similarity

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

target_user_id = 'doctr'

# ---------- Step 1: Get Top 10 Played Games ----------
query_user_games = """
SELECT ui.item_id, ui.item_name, ui.playtime_forever, ui.playtime_2weeks, g.tfidf_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_user_games, (target_user_id,))
user_rows = cursor.fetchall()

def parse_vector(vec):
    if isinstance(vec, str):
        return np.array([float(x.strip()) for x in vec.strip('{}').split(',')])
    return np.array(vec)

user_games = []
user_vectors = []

for row in user_rows:
    item_id, item_name, _, _, tfidf = row
    vector = parse_vector(tfidf)
    user_games.append({'item_id': item_id, 'item_name': item_name})
    user_vectors.append(vector)

user_vectors = np.array(user_vectors)

# ---------- Step 2: Recommend Top 10 Similar Games ----------
cursor.execute("SELECT id, app_name, tfidf_vector FROM games;")
all_games = cursor.fetchall()

similarities = {}
for row in all_games:
    game_id, name, vec = row
    if game_id in [g['item_id'] for g in user_games]:
        continue 
    vec_parsed = parse_vector(vec)
    sim = cosine_similarity([vec_parsed], user_vectors).mean()
    if game_id not in similarities or sim > similarities[game_id]:
        similarities[game_id] = sim

sorted_recommendations = sorted(similarities.items(), key=lambda x: x[1], reverse=True)[:10]
recommended_ids = [game_id for game_id, _ in sorted_recommendations]

placeholders = ','.join(['%s'] * len(recommended_ids))
cursor.execute(f"""
SELECT id, app_name, tfidf_vector FROM games
WHERE id IN ({placeholders});
""", recommended_ids)
recommended_rows = cursor.fetchall()

recommended_vectors = []
recommended_names = []
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
