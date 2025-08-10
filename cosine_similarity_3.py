import psycopg2
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.optimize import linear_sum_assignment
from collections import Counter

# ------------------ Configuration ------------------
DB_CFG = dict(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432",
)

USER_ID = "doctr"   # <-- change as needed
N_USER = 10         # how many of the user's games to use
K_REC = 10          # how many candidate recommendations to pull

SHOW_SIDE_BY_SIDE = True  # also print a compact side-by-side panel

# ------------------ Helpers ------------------
def parse_pgvector(vec):
    """
    Convert a pgvector-like value into a numpy array.
    Accepts strings like '[0.1, 0.2, ...]' or '{...}', or Python lists.
    """
    if vec is None:
        return np.array([])
    if isinstance(vec, (list, tuple, np.ndarray)):
        try:
            return np.array(vec, dtype=float)
        except Exception:
            return np.array([])
    if isinstance(vec, str):
        s = vec.strip().strip('{}[]')
        if not s:
            return np.array([])
        try:
            return np.array([float(x.strip()) for x in s.split(',') if x.strip()], dtype=float)
        except ValueError:
            return np.array([])
    return np.array([])

def to_pgvector_literal(vec: np.ndarray) -> str:
    """Build a pgvector literal string: '[v1, v2, ...]'."""
    return "[" + ",".join(f"{float(x):.8f}" for x in vec.tolist()) + "]"

def mode_dimension(vectors):
    """Return the most common (mode) non-zero dimension among vectors."""
    dims = [v.shape[0] for v in vectors if v.size > 0]
    if not dims:
        return None
    cnt = Counter(dims)
    return cnt.most_common(1)[0][0]

def fmt_row(left, right, width=36):
    return f"{left:<{width}}  {right}"

# ------------------ Main ------------------
def main():
    # Connect
    conn = psycopg2.connect(**DB_CFG)
    cursor = conn.cursor()

    # Step 1: User's Top N Games (join on text to avoid type issues)
    cursor.execute(
        """
        SELECT ui.item_id, ui.item_name, g.tfidf_vec_vector
        FROM user_items ui
        JOIN games g ON ui.item_id::text = g.id::text
        WHERE ui.user_id = %s
        ORDER BY (COALESCE(ui.playtime_forever,0) + COALESCE(ui.playtime_2weeks,0)) DESC
        LIMIT %s;
        """,
        (USER_ID, N_USER),
    )
    user_rows = cursor.fetchall()
    if not user_rows:
        raise RuntimeError("No games found for this user.")

    # Parse and keep rows
    user_parsed = []
    for item_id, item_name, vec in user_rows:
        v = parse_pgvector(vec)
        user_parsed.append((str(item_id), item_name, v))

    dim = mode_dimension([v for _, _, v in user_parsed])
    if dim is None:
        raise RuntimeError("All user game vectors are empty or invalid.")

    # Keep consistent dimension only
    user_valid = [(i, n, v) for (i, n, v) in user_parsed if v.size == dim]
    if len(user_valid) == 0:
        raise RuntimeError("No valid user vectors with consistent dimension.")

    user_ids = [i for (i, _, _) in user_valid]
    user_names = [n for (_, n, _) in user_valid]
    user_vecs = [v for (_, _, v) in user_valid]
    user_mat = np.stack(user_vecs, axis=0)

    # Centroid
    centroid = user_mat.mean(axis=0)
    centroid_literal = to_pgvector_literal(centroid)

    # Step 2: Recommend K nearest (exclude played)
    # Build WHERE clause
    if len(user_ids) == 1:
        played_clause = "WHERE id::text <> %s"
        played_params = (user_ids[0],)
    else:
        played_clause = "WHERE id::text NOT IN %s"
        played_params = (tuple(user_ids),)

    sql = f"""
        SELECT id, app_name, tfidf_vec_vector
        FROM games
        {played_clause}
        ORDER BY tfidf_vec_vector <=> %s::vector
        LIMIT %s;
    """
    params = played_params + (centroid_literal, K_REC)
    cursor.execute(sql, params)
    rec_rows = cursor.fetchall()

    if not rec_rows:
        raise RuntimeError("No recommendations found (candidate set empty).")

    rec_parsed = []
    for rid, rname, rvec in rec_rows:
        v = parse_pgvector(rvec)
        rec_parsed.append((str(rid), rname, v))

    # Filter recs to the same dimension
    rec_valid = [(i, n, v) for (i, n, v) in rec_parsed if v.size == dim]
    if len(rec_valid) == 0:
        raise RuntimeError("Recommended vectors are empty or wrong dimension.")

    rec_ids = [i for (i, _, _) in rec_valid]
    rec_names = [n for (_, n, _) in rec_valid]
    rec_vecs = [v for (_, _, v) in rec_valid]
    rec_mat = np.stack(rec_vecs, axis=0)

    # Step 3: Cosine Similarity Matrix (user_top x rec_candidates)
    sim_matrix = cosine_similarity(user_mat, rec_mat)
    cost_matrix = 1.0 - sim_matrix

    # Step 4: Optimal Matching (Hungarian)
    row_ind, col_ind = linear_sum_assignment(cost_matrix)
    best_avg_similarity = sim_matrix[row_ind, col_ind].mean()

    # Step 5: Output
    print("User Top Games (used in centroid & matching):")
    for idx, (uid, name) in enumerate(zip(user_ids, user_names), 1):
        print(f"{idx}. {name} (id={uid})")

    print("\nRecommended Candidates (nearest to centroid, filtered by dimension):")
    for idx, (rid, rname) in enumerate(zip(rec_ids, rec_names), 1):
        print(f"{idx}. {rname} (id={rid})")

    print("\nOptimal Matches (User -> Recommended):")
    for ui, ri in zip(row_ind, col_ind):
        print(f"- {user_names[ui]}  ->  {rec_names[ri]}  (similarity: {sim_matrix[ui, ri]:.4f})")

    print(f"\nAverage Cosine Similarity over matched pairs: {best_avg_similarity:.4f}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
