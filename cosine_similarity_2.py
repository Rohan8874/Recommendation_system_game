import psycopg2
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.optimize import linear_sum_assignment

# ------------------ PostgreSQL Connection ------------------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# ------------------ Helper: Parse pgvector to numpy ------------------
def parse_pgvector(vec):
    if isinstance(vec, str):
        vec = vec.strip('{}[]')
        try:
            return np.array([float(x.strip()) for x in vec.split(',') if x.strip()])
        except ValueError:
            return np.array([])
    return np.array([])

# ------------------ Step 1: User's Top 10 Games ------------------
user_id = 'doctr'

cursor.execute("""
    SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
    FROM user_items ui
    JOIN games g ON ui.item_id = g.id
    WHERE ui.user_id = %s
    ORDER BY (ui.playtime_forever + ui.playtime_2weeks) DESC
    LIMIT 10;
""", (user_id,))
user_rows = cursor.fetchall()

user_game_names = [row[1] for row in user_rows]
user_vectors = [parse_pgvector(row[2]) for row in user_rows]

# ------------------ Step 2: Global Top 10 Games (excluding user's) ------------------
cursor.execute("""
    SELECT ui.item_id, g.app_name, g.tfidf_vec_vector, COUNT(*) as player_count
    FROM user_items ui
    JOIN games g ON ui.item_id = g.id
    WHERE ui.item_id NOT IN %s
    GROUP BY ui.item_id, g.app_name, g.tfidf_vec_vector
    ORDER BY COUNT(*) DESC
    LIMIT 10;
""", (tuple([row[0] for row in user_rows]),))
global_rows = cursor.fetchall()

global_game_names = [row[1] for row in global_rows]
global_vectors = [parse_pgvector(row[2]) for row in global_rows]

# ------------------ Step 3: Cosine Similarity Matrix ------------------
sim_matrix = np.zeros((10, 10))
for i, u_vec in enumerate(user_vectors):
    for j, g_vec in enumerate(global_vectors):
        sim = cosine_similarity(u_vec.reshape(1, -1), g_vec.reshape(1, -1))[0][0]
        sim_matrix[i][j] = sim

# Convert similarity to cost (higher sim => lower cost)
cost_matrix = 1 - sim_matrix

# ------------------ Step 4: Optimal Matching ------------------
row_ind, col_ind = linear_sum_assignment(cost_matrix)
best_avg_similarity = sim_matrix[row_ind, col_ind].mean()

# ------------------ Step 5: Output ------------------
print("User Top 10 Games:")
for i, name in enumerate(user_game_names, 1):
    print(f"{i}. {name}")

print("\nBest Matching Global Top 10 Games:")
for i, j in enumerate(col_ind):
    print(f"{i+1}. {global_game_names[j]} (Similarity: {sim_matrix[i][j]:.4f})")

print(f"\nAverage Cosine Similarity: {best_avg_similarity:.4f}")

cursor.close()
conn.close()
