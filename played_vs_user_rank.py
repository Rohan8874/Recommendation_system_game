import psycopg2
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
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

target_user_id = 'doctr' 

# ---------------------- Helper: Parse TF-IDF vector ----------------------
def parse_vector(vec):
    if isinstance(vec, str):
        return np.array([float(x.strip()) for x in vec.strip('{}').split(',')])
    elif isinstance(vec, list):
        return np.array(vec)
    else:
        return np.array([])

# ---------------------- Step 1: Top 10 User Games by Playtime ----------------------
query_user_top = """
SELECT ui.item_id, ui.item_name, g.tfidf_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_user_top, (target_user_id,))
user_rows = cursor.fetchall()
user_df = pd.DataFrame(user_rows, columns=['item_id', 'item_name', 'tfidf_vector'])
user_df['tfidf_vector'] = user_df['tfidf_vector'].apply(parse_vector)

# ---------------------- Step 2: Top 10 Global Games by Playtime ----------------------
query_global_top = """
SELECT ui.item_id, ui.item_name, g.tfidf_vector, SUM(ui.playtime_forever) AS total_playtime, SUM(ui.playtime_2weeks) AS total_2weeks
FROM user_items ui
JOIN games g ON ui.item_id = g.id
GROUP BY ui.item_id, ui.item_name, g.tfidf_vector
ORDER BY total_playtime DESC, total_2weeks DESC
LIMIT 10;
"""
cursor.execute(query_global_top)
global_rows = cursor.fetchall()
global_df = pd.DataFrame(global_rows, columns=['item_id', 'item_name', 'tfidf_vector', 'playtime_forever', 'playtime_2weeks'])
global_df['tfidf_vector'] = global_df['tfidf_vector'].apply(parse_vector)

# ---------------------- Step 3: Calculate Cosine Similarity for Rank-wise Matching ----------------------
similarities = []
for i in range(min(len(user_df), len(global_df))):
    u_vec = user_df.iloc[i]['tfidf_vector'].reshape(1, -1)
    g_vec = global_df.iloc[i]['tfidf_vector'].reshape(1, -1)
    if u_vec.shape[1] == g_vec.shape[1] and u_vec.shape[1] > 0:
        sim = cosine_similarity(u_vec, g_vec)[0][0]
    else:
        sim = 0
    similarities.append(sim)

# ---------------------- Step 4: Spearman Rank Correlation ----------------------
ranks = list(range(1, len(similarities)+1))
rho, p_value = spearmanr(similarities, ranks)

# ---------------------- Output Results ----------------------
print("\n Top 10 User Games:")
print(user_df[['item_name']])
print("\n Top 10 Global Games:")
print(global_df[['item_name']])

print("\n Cosine Similarity (Rank-wise TF-IDF Comparison):")
for i, sim in enumerate(similarities):
    print(f"Rank {i+1}: Similarity = {sim:.4f}")

average_similarity = sum(similarities) / len(similarities) if similarities else 0
print(f"\n Average Cosine Similarity of Rank-wise Matched Games: {average_similarity:.4f}")