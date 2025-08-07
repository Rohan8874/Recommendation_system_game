import psycopg2
import pandas as pd
import numpy as np
from scipy.stats import spearmanr

# ---------------------- Connect to PostgreSQL ----------------------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# ---------------------- Helper: Parse pgvector ----------------------
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

# ---------------------- Step 1: Get Top 10 Games for Target User ----------------------
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

# ---------------------- Step 2: Get Top 10 Global Games (Excluding Matching Names) ----------------------
user_game_names = tuple(user_df['item_name'].tolist())

if len(user_game_names) == 1:
    user_game_names += ("",)

query_global_top = f"""
SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector, 
       SUM(ui.playtime_forever) AS total_playtime, 
       SUM(ui.playtime_2weeks) AS total_2weeks
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.item_name NOT IN %s
GROUP BY ui.item_id, ui.item_name, g.tfidf_vec_vector
ORDER BY total_playtime DESC, total_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_global_top, (user_game_names,))
global_rows = cursor.fetchall()
global_df = pd.DataFrame(global_rows, columns=['item_id', 'item_name', 'tfidf_vec_vector', 'playtime_forever', 'playtime_2weeks'])
global_df['tfidf_vec_vector'] = global_df['tfidf_vec_vector'].apply(parse_pgvector)

# ---------------------- Step 3: Compute Spearman Rank Correlation ----------------------
user_vecs = user_df['tfidf_vec_vector'].tolist()
global_vecs = global_df['tfidf_vec_vector'].tolist()

spearman_scores = []
for i, (u_vec, g_vec) in enumerate(zip(user_vecs, global_vecs)):
    if len(u_vec) == len(g_vec) and len(u_vec) > 0:
        rho, _ = spearmanr(u_vec, g_vec)
        rho = 0 if np.isnan(rho) else rho
    else:
        rho = 0
    spearman_scores.append(rho)

# ---------------------- Step 4: Print Results ----------------------
print("\nTop 10 User Games:")
print(user_df[['item_name']])

print("\nTop 10 Global Games (excluding same names):")
print(global_df[['item_name']])

print("\nSpearman Rank Correlation Between Matching Ranks:")
for i, rho in enumerate(spearman_scores):
    print(f"Rank {i+1}: Spearman ρ = {rho:.4f}")

avg_rho = sum(spearman_scores) / len(spearman_scores) if spearman_scores else 0
print(f"\nAverage Spearman Correlation (Top 10 Games): {avg_rho:.4f}")

# ---------------------- Cleanup ----------------------
cursor.close()
conn.close()



