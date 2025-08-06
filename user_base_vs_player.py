import psycopg2
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
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tfidf_vector
        FROM games
        WHERE app_name = %s
        LIMIT 1;
    """, (game_name,))
    result = cursor.fetchone()
    cursor.close()
    if result:
        tfidf = result[0]
        if isinstance(tfidf, list):
            game_vectors[game_name] = [float(x) for x in tfidf]
        else:
            print(f" Unexpected format for tfidf_vector in game: {game_name}")


# -------------------- Step 7: Create Rankings --------------------
part1_rank = {}
rank = 1
for games in user_games_dict.values():
    for item_name, _ in games:
        if item_name in game_vectors and item_name not in part1_rank:
            part1_rank[item_name] = rank
            rank += 1

part2_rank = {}
rank = 1
for item_name, _, _, _ in top_games:
    if item_name in game_vectors and item_name not in part2_rank:
        part2_rank[item_name] = rank
        rank += 1

# -------------------- Step 8: Compute Spearman Rank Correlation --------------------
common_games = list(set(part1_rank.keys()).intersection(part2_rank.keys()))

if len(common_games) < 2:
    print("\n Not enough common games to compute Spearman correlation.")
else:
    part1_ranks = [part1_rank[game] for game in common_games]
    part2_ranks = [part2_rank[game] for game in common_games]

    correlation, _ = spearmanr(part1_ranks, part2_ranks)
    print(f"\n Spearman Rank Correlation between Part 1 and Part 2: {correlation:.4f}")

# -------------------- Cleanup --------------------
cursor.close()
conn.close()
