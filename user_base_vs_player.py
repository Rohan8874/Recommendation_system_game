import psycopg2
from scipy.stats import spearmanr
from numpy import array, dot
from numpy.linalg import norm

# -------------------- PostgreSQL Connection --------------------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# -------------------- Step 1: Get Top 20 Similar Users --------------------
target_user = 'doctr'
cursor.execute("""
    SELECT user_id
    FROM user_play_ratio
    WHERE user_id != %s
    ORDER BY playtime_vector <=> (
        SELECT playtime_vector
        FROM user_play_ratio
        WHERE user_id = %s
    ) ASC
    LIMIT 20;
""", (target_user, target_user))

top_user_ids = [row[0] for row in cursor.fetchall()]

# -------------------- Step 2: Fetch Top 5 Games of These Users --------------------
user_games_dict = {}

for user_id in top_user_ids:
    cursor.execute("""
        SELECT item_name, playtime_forever
        FROM user_items
        WHERE user_id = %s
        ORDER BY playtime_forever DESC
        LIMIT 5;
    """, (user_id,))
    
    games = cursor.fetchall()
    user_games_dict[user_id] = games

# -------------------- Step 3: Display Similar Users' Top Games --------------------
for user_id, games in user_games_dict.items():
    print(f"\nUser: {user_id}")
    for item_name, playtime in games:
        print(f"   - {item_name}: {playtime} mins")

# -------------------- Step 4: Show Target User's Top 10 Games --------------------
print(f"\nTop 10 Games for Target User '{target_user}' Based on Combined Playtime:")

cursor.execute("""
    SELECT item_name, playtime_forever, playtime_2weeks, 
           (playtime_forever + playtime_2weeks) AS total_playtime
    FROM user_items
    WHERE user_id = %s
    ORDER BY total_playtime DESC
    LIMIT 10;
""", (target_user,))

top_games = cursor.fetchall()
for item_name, playtime_forever, playtime_2weeks, total in top_games:
    print(f"   - {item_name}: {playtime_forever} mins (forever), {playtime_2weeks} mins (2 weeks), Total: {total} mins")

# -------------------- Step 5: Prepare Game Lists --------------------
part1_game_names = set()
for games in user_games_dict.values():
    for item_name, _ in games:
        part1_game_names.add(item_name)

part2_game_names = set(item_name for item_name, _, _, _ in top_games)
all_game_names = list(part1_game_names.union(part2_game_names))

# -------------------- Step 6: Get TF-IDF Vectors --------------------
game_vectors = {}
for game_name in all_game_names:
    cur = conn.cursor()
    cur.execute("""
        SELECT tfidf_vector
        FROM games
        WHERE app_name = %s
        LIMIT 1;
    """, (game_name,))
    result = cur.fetchone()
    cur.close()

    if result:
        tfidf = result[0]
        if isinstance(tfidf, list):
            game_vectors[game_name] = [float(x) for x in tfidf]
        else:
            print(f" Unexpected format for tfidf_vector in game: {game_name}")

# -------------------- Step 7: Average TF-IDF Vectors --------------------
part1_vectors = [game_vectors[game] for game in part1_game_names if game in game_vectors]
part2_vectors = [game_vectors[game] for game in part2_game_names if game in game_vectors]

if not part1_vectors or not part2_vectors:
    print("\nNot enough valid TF-IDF vectors for comparison.")
else:
    part1_avg_vector = array(part1_vectors).mean(axis=0)
    part2_avg_vector = array(part2_vectors).mean(axis=0)

    # -------------------- Step 8: Similarity Measures --------------------
    spearman_corr, _ = spearmanr(part1_avg_vector, part2_avg_vector)
    print(f"\nSpearman Rank Correlation (TF-IDF): {spearman_corr:.4f}")

    cosine_sim = dot(part1_avg_vector, part2_avg_vector) / (norm(part1_avg_vector) * norm(part2_avg_vector))
    print(f"Cosine Similarity (TF-IDF): {cosine_sim:.4f}")

# -------------------- Cleanup --------------------
cursor.close()
conn.close()
