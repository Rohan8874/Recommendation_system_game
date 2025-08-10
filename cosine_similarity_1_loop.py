import psycopg2
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.optimize import linear_sum_assignment

# -------------------- Configuration --------------------
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Rohan$123",
    "host": "localhost",
    "port": "5432"
}
TARGET_USER_ID = 'doctr'

# -------------------- PGVector Parser --------------------
def parse_pgvector(vec):
    if isinstance(vec, str):
        vec = vec.strip('{}[]')
        try:
            return np.array([float(x.strip()) for x in vec.split(',')])
        except Exception as e:
            print(f"Error parsing vector: {e}")
            return np.array([])
    return np.array([])

# -------------------- Database Queries --------------------
def fetch_user_top_games(cursor, user_id):
    query = """
        SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
        FROM user_items ui
        JOIN games g ON ui.item_id = g.id
        WHERE ui.user_id = %s
        ORDER BY ui.playtime_forever + ui.playtime_2weeks DESC
        LIMIT 10;
    """
    cursor.execute(query, (user_id,))
    return cursor.fetchall()

def fetch_global_top_games(cursor, exclude_ids):
    query = """
        SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
        FROM user_items ui
        JOIN games g ON ui.item_id = g.id
        WHERE ui.item_id NOT IN %s
        GROUP BY ui.item_id, ui.item_name, g.tfidf_vec_vector
        ORDER BY SUM(ui.playtime_forever + ui.playtime_2weeks) DESC
        LIMIT 10;
    """
    cursor.execute(query, (tuple(exclude_ids),))
    return cursor.fetchall()

# -------------------- Compute Best Cosine Matching --------------------
def compute_best_cosine_matching(user_vecs, global_vecs):
    user_stack = np.stack(user_vecs)
    global_stack = np.stack(global_vecs)

    cos_sim_matrix = cosine_similarity(user_stack, global_stack)
    cost_matrix = 1 - cos_sim_matrix  

    row_ind, col_ind = linear_sum_assignment(cost_matrix)
    best_similarities = cos_sim_matrix[row_ind, col_ind]
    average_similarity = np.mean(best_similarities)

    return row_ind, col_ind, best_similarities, average_similarity, cos_sim_matrix

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Step 1: Get user top games
    user_results = fetch_user_top_games(cursor, TARGET_USER_ID)
    if len(user_results) < 10:
        print(" Not enough user games for comparison.")
        return
    user_ids = [r[0] for r in user_results]
    user_names = [r[1] for r in user_results]
    user_vectors = [parse_pgvector(r[2]) for r in user_results]

    # Step 2: Get global top games (excluding user's)
    global_results = fetch_global_top_games(cursor, user_ids)
    if len(global_results) < 10:
        print(" Not enough global games for comparison.")
        return
    global_ids = [r[0] for r in global_results]
    global_names = [r[1] for r in global_results]
    global_vectors = [parse_pgvector(r[2]) for r in global_results]

    # Step 3: Print 
    print(" User's Top 10 Games:")
    for i, (gid, name) in enumerate(zip(user_ids, user_names), 1):
        print(f"{i}. {name} (ID: {gid})")

    print("\n Global Top 10 Games (excluding user's):")
    for i, (gid, name) in enumerate(zip(global_ids, global_names), 1):
        print(f"{i}. {name} (ID: {gid})")

    row_ind, col_ind, best_similarities, avg_sim, cos_sim_matrix = compute_best_cosine_matching(user_vectors, global_vectors)

    print("\n Optimal Matching (User Top i ↔ Global Top j):")
    for i, j, sim in zip(row_ind, col_ind, best_similarities):
        print(f"{i+1}. {user_names[i]} ↔ {global_names[j]} | Cosine Similarity: {sim:.4f}")

    print(f"\n Average Cosine Similarity (Optimal Assignment): {avg_sim:.4f}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
