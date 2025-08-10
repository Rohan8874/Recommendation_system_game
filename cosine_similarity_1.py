import psycopg2
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.optimize import linear_sum_assignment

# ----------- PostgreSQL Connection -----------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# ----------- Helper to Parse PGVector to np.array -----------
def parse_pgvector(vec):
    if isinstance(vec, str):
        vec = vec.strip('{}[]')
        return np.array([float(x.strip()) for x in vec.split(',') if x.strip()])
    return np.array([])

# ----------- Step 1: Get Top 10 Games of a User -----------
user_id = 'doctr'

query_user_top = """
SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever + ui.playtime_2weeks DESC
LIMIT 10;
"""

cursor.execute(query_user_top, (user_id,))
user_games = cursor.fetchall()
user_game_ids = [row[0] for row in user_games]
user_game_names = [row[1] for row in user_games]
user_vectors = [parse_pgvector(row[2]) for row in user_games]

# ----------- Step 2: Get Global Top 10 Games (Excluding User's Games) -----------
query_global_top = """
SELECT ui.item_id, MAX(ui.item_name), g.tfidf_vec_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.item_id NOT IN %s
GROUP BY ui.item_id, g.tfidf_vec_vector
ORDER BY SUM(ui.playtime_forever + ui.playtime_2weeks) DESC
LIMIT 10;
"""

cursor.execute(query_global_top, (tuple(user_game_ids),))
global_games = cursor.fetchall()
global_game_ids = [row[0] for row in global_games]
global_game_names = [row[1] for row in global_games]
global_vectors = [parse_pgvector(row[2]) for row in global_games]

# ----------- Step 3: Compute Cosine Similarity Matrix -----------
cos_sim_matrix = cosine_similarity(np.stack(user_vectors), np.stack(global_vectors))

# ----------- Step 4: Apply Hungarian Algorithm -----------
cost_matrix = 1 - cos_sim_matrix  
row_ind, col_ind = linear_sum_assignment(cost_matrix)
best_similarities = cos_sim_matrix[row_ind, col_ind]
average_similarity = np.mean(best_similarities)

# ----------- Step 5: Output Results -----------
print("\n🎮 User Top 10 Games:")
for i, name in enumerate(user_game_names):
    print(f"{i+1}. {name}")

print("\n Global Top 10 Games (Excluding User's Games):")
for i, name in enumerate(global_game_names):
    print(f"{i+1}. {name}")

print("\n Optimal Matching (User Top i ↔ Global j):")
for i, j, sim in zip(row_ind, col_ind, best_similarities):
    print(f"User #{i+1} ({user_game_names[i]}) ↔ Global #{j+1} ({global_game_names[j]}) | Cosine Similarity: {sim:.4f}")

print(f"\n Average Cosine Similarity (Optimal Matching): {average_similarity:.4f}")
