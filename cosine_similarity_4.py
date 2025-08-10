import psycopg2
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.optimize import linear_sum_assignment
from collections import Counter, defaultdict

# ------------------ Configuration ------------------
DB_CFG = dict(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432",
)

USER_ID = "doctr"   # <-- change as needed
N_USER = 10         # how many of the user's games to use (top by playtime)
K_REC = 10          # how many recommendations to output
K_NEIGHBOR = 20     # how many similar users to fetch
K_NEIGHBOR_TOP = 5  # how many games to show for each neighbor

# ------------------ Helpers ------------------
def parse_pgvector(vec):
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
    return "[" + ",".join(f"{float(x):.8f}" for x in vec.tolist()) + "]"

def mode_dimension(vectors):
    dims = [v.shape[0] for v in vectors if v.size > 0]
    if not dims:
        return None
    cnt = Counter(dims)
    return cnt.most_common(1)[0][0]

def fetchall(cursor):
    try:
        return cursor.fetchall()
    except psycopg2.ProgrammingError:
        return []

# ------------------ Main ------------------
def main():
    conn = psycopg2.connect(**DB_CFG)
    cursor = conn.cursor()

    try:
        # ---------- Step 1: target user's top N games ----------
        cursor.execute(
            """
            SELECT ui.item_id::text, ui.item_name, g.tfidf_vec_vector,
                   (COALESCE(ui.playtime_forever,0) + COALESCE(ui.playtime_2weeks,0)) AS total_play
            FROM user_items ui
            JOIN games g ON ui.item_id::text = g.id::text
            WHERE ui.user_id = %s
            ORDER BY total_play DESC
            LIMIT %s;
            """,
            (USER_ID, N_USER),
        )
        user_rows = fetchall(cursor)
        if not user_rows:
            raise RuntimeError("No games found for this user in user_items.")

        user_parsed = []
        for item_id, item_name, vec, total_play in user_rows:
            v = parse_pgvector(vec)
            user_parsed.append((str(item_id), item_name, v, int(total_play)))

        dim = mode_dimension([v for (_, _, v, _) in user_parsed])
        if dim is None:
            raise RuntimeError("All TF-IDF vectors for user's top games are empty or invalid.")

        user_valid = [(i, n, v, p) for (i, n, v, p) in user_parsed if v.size == dim]
        user_ids = [i for (i, _, _, _) in user_valid]
        user_names = [n for (_, n, _, _) in user_valid]
        user_vecs = [v for (_, _, v, _) in user_valid]
        user_mat = np.stack(user_vecs, axis=0)

        print(f"=== User '{USER_ID}' Top {N_USER} Games ===")
        for idx, (gid, gname, _, play) in enumerate(user_valid, start=1):
            print(f"{idx}. {gname} (id={gid}) — Playtime: {play}")
        print()

        # centroid of user's top games
        centroid = user_mat.mean(axis=0)
        centroid_lit = to_pgvector_literal(centroid)

        # All games already played by user (exclude later)
        cursor.execute(
            "SELECT ui.item_id::text FROM user_items ui WHERE ui.user_id = %s;",
            (USER_ID,),
        )
        already_played = {r[0] for r in fetchall(cursor)}

        # ---------- Step 2: top 20 similar users by playtime_vector ----------
        cursor.execute(
            """
            SELECT upr.user_id
            FROM user_play_ratio upr
            WHERE upr.user_id <> %s
            ORDER BY upr.playtime_vector <=> (
                SELECT playtime_vector FROM user_play_ratio WHERE user_id = %s
            ) ASC
            LIMIT %s;
            """,
            (USER_ID, USER_ID, K_NEIGHBOR),
        )
        neighbor_ids = [row[0] for row in fetchall(cursor)]

        print(f"=== Top {K_NEIGHBOR} Similar Users ===")
        print(", ".join(neighbor_ids))
        print()

        # ---------- Step 3: each neighbor's top 5 games ----------
        print(f"=== Each Neighbor's Top {K_NEIGHBOR_TOP} Games ===")
        for nid in neighbor_ids:
            cursor.execute(
                """
                SELECT ui.item_id::text, ui.item_name,
                       (COALESCE(ui.playtime_forever,0) + COALESCE(ui.playtime_2weeks,0)) AS total_play
                FROM user_items ui
                WHERE ui.user_id = %s
                ORDER BY total_play DESC
                LIMIT %s;
                """,
                (nid, K_NEIGHBOR_TOP),
            )
            rows = fetchall(cursor)
            top_line = " | ".join(f"{name} ({tp})" for _, name, tp in rows)
            print(f"- {nid}: {top_line}")
        print()

        # ---------- Step 4: candidate pool from all neighbor games ----------
        cursor.execute(
            """
            SELECT DISTINCT ui.item_id::text, ui.item_name, g.tfidf_vec_vector
            FROM user_items ui
            JOIN games g ON ui.item_id::text = g.id::text
            WHERE ui.user_id = ANY(%s)
              AND ui.item_id::text <> ALL(%s)
            """,
            (neighbor_ids, list(already_played)),
        )
        cand_rows = fetchall(cursor)

        candidates = []
        for cid, cname, cvec in cand_rows:
            v = parse_pgvector(cvec)
            if v.size == dim:
                candidates.append((cid, cname, v))

        cand_ids = [c[0] for c in candidates]
        cand_names = [c[1] for c in candidates]
        cand_vecs = [c[2] for c in candidates]
        cand_mat = np.stack(cand_vecs, axis=0)

        # ---------- Step 5: rank by similarity to centroid ----------
        centroid_sim = cosine_similarity(centroid.reshape(1, -1), cand_mat).ravel()
        top_idx = np.argsort(-centroid_sim)[:K_REC]
        rec_ids = [cand_ids[i] for i in top_idx]
        rec_names = [cand_names[i] for i in top_idx]
        rec_vecs = [cand_vecs[i] for i in top_idx]
        rec_mat = np.stack(rec_vecs, axis=0)

        print(f"=== Top {K_REC} Recommendations ===")
        for rnk, (rid, rname, rsim) in enumerate(zip(rec_ids, rec_names, centroid_sim[top_idx]), start=1):
            print(f"{rnk}. {rname} (id={rid}) — Centroid cosine: {rsim:.4f}")
        print()

        # ---------- Step 6: best match & average cosine ----------
        sim_matrix = cosine_similarity(user_mat, rec_mat)
        row_ind, col_ind = linear_sum_assignment(1.0 - sim_matrix)
        best_avg_similarity = float(sim_matrix[row_ind, col_ind].mean())

        print("=== Hungarian Best Matches (User Top -> Recommendation) ===")
        for ui, ri in zip(row_ind, col_ind):
            print(f"- {user_names[ui]}  ->  {rec_names[ri]}  (cosine: {sim_matrix[ui, ri]:.4f})")
        print(f"\nAverage cosine over matched pairs: {best_avg_similarity:.4f}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
