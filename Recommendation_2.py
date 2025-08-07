import psycopg2
import pandas as pd
import numpy as np
from scipy.stats import spearmanr

# -------------------- PostgreSQL Connection --------------------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# -------------------- Parse PGVector --------------------
def parse_pgvector(vec):
    if isinstance(vec, str):
        vec = vec.strip()
        if vec.startswith('{') and vec.endswith('}'):
            vec = vec.strip('{}')
        elif vec.startswith('[') and vec.endswith(']'):
            vec = vec.strip('[]')
        try:
            return np.array([float(x.strip()) for x in vec.split(',')])
        except ValueError:
            print(f"Could not parse vector: {vec}")
            return np.array([])
    elif isinstance(vec, list):
        return np.array(vec, dtype=float)
    else:
        return np.array([])

# -------------------- Step 1: User Top 10 Games by Playtime --------------------
target_user_id = 'doctr' 
query_user_top = """
SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_user_top, (target_user_id,))
user_rows = cursor.fetchall()
user_df = pd.DataFrame(user_rows, columns=['item_id', 'item_name', 'tfidf_vec_vector'])
user_df['tfidf_vec_vector'] = user_df['tfidf_vec_vector'].apply(parse_pgvector)

# Extract user game names to exclude from global top games
user_game_names = tuple(user_df['item_name'].tolist())
if len(user_game_names) == 1:
    user_game_names += ("",)  # Ensure tuple has a trailing comma for single-item tuples

# -------------------- Step 2: Global Top 10 Games by Popularity (excluding user's games) --------------------
query_global_top = """
SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector, COUNT(ui.user_id) AS user_count
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.item_name NOT IN %s
GROUP BY ui.item_id, ui.item_name, g.tfidf_vec_vector
ORDER BY user_count DESC
LIMIT 10;
"""
cursor.execute(query_global_top, (user_game_names,))
global_rows = cursor.fetchall()
global_df = pd.DataFrame(global_rows, columns=['item_id', 'item_name', 'tfidf_vec_vector', 'user_count'])
global_df['tfidf_vec_vector'] = global_df['tfidf_vec_vector'].apply(parse_pgvector)

# -------------------- Step 3: Spearman Correlation --------------------
user_vecs = user_df['tfidf_vec_vector'].tolist()
global_vecs = global_df['tfidf_vec_vector'].tolist()

spearman_scores = []
for i, (u_vec, g_vec) in enumerate(zip(user_vecs, global_vecs)):
    if len(u_vec) == len(g_vec) and len(u_vec) > 0:
        rho, _ = spearmanr(u_vec, g_vec)
        if np.isnan(rho):
            rho = 0
    else:
        rho = 0
    spearman_scores.append(rho)

# -------------------- Step 4: Output Results --------------------
print("\nTop 10 User Games:")
print(user_df[['item_name']])

print("\nTop 10 Most Popular Games (excluding user games):")
print(global_df[['item_name']])

print("\nSpearman Rank Correlation Between Matching Ranks:")
for i, rho in enumerate(spearman_scores):
    print(f"Rank {i+1}: Spearman ρ = {rho:.4f}")

avg_rho = sum(spearman_scores) / len(spearman_scores) if spearman_scores else 0
print(f"\nAverage Spearman Correlation (Top 10 Games): {avg_rho:.4f}")

# -------------------- Cleanup --------------------
cursor.close()
conn.close()
