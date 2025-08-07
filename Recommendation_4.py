import psycopg2
from scipy.stats import spearmanr
from numpy import array, dot
from numpy.linalg import norm
from collections import defaultdict

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
target_user = 'Br0wni3'
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

# -------------------- Step 2: Get Target User's Top 10 Games --------------------
cursor.execute("""
    SELECT item_name, playtime_forever, playtime_2weeks,
           (playtime_forever + playtime_2weeks) AS total_playtime
    FROM user_items
    WHERE user_id = %s
    ORDER BY total_playtime DESC
    LIMIT 10;
""", (target_user,))
top_games = cursor.fetchall()

# Store names of target user's top games to exclude from recommendations
target_user_top_game_names = set(item_name for item_name, _, _, _ in top_games)

print(f"\nTop 10 Games for Target User '{target_user}' Based on Combined Playtime:")
for item_name, pf, p2w, total in top_games:
    print(f"   - {item_name}: {pf} mins (forever), {p2w} mins (2w), Total: {total} mins")

# -------------------- Step 3: Fetch Similar Users' Top Games (excluding target user's games) --------------------
user_games_dict = {}
recommendation_pool = defaultdict(lambda: {'count': 0, 'total_playtime': 0})

for user_id in top_user_ids:
    cursor.execute("""
        SELECT item_name, playtime_forever
        FROM user_items
        WHERE user_id = %s
        ORDER BY playtime_forever DESC
        LIMIT 5;
    """, (user_id,))
    
    filtered_games = []
    for item_name, playtime in cursor.fetchall():
        if item_name not in target_user_top_game_names:
            filtered_games.append((item_name, playtime))
            # Collect for recommendation
            recommendation_pool[item_name]['count'] += 1
            recommendation_pool[item_name]['total_playtime'] += playtime
    user_games_dict[user_id] = filtered_games

# -------------------- Step 4: Display Filtered Top Games of Similar Users --------------------
print("\nSimilar Users' Top 5 Games (excluding games already played by target user):")
for user_id, games in user_games_dict.items():
    print(f"\nUser: {user_id}")
    for item_name, playtime in games:
        print(f"   - {item_name}: {playtime} mins")

# -------------------- Step 5: Recommend Most Common/Popular Games --------------------
print("\nTop Recommended Games for Target User (based on similar users):")
# Sort by frequency and playtime
sorted_recommendations = sorted(
    recommendation_pool.items(),
    key=lambda x: (x[1]['count'], x[1]['total_playtime']),
    reverse=True
)

for item_name, stats in sorted_recommendations[:10]:  # Top 10 recommended
    print(f"   - {item_name}: recommended by {stats['count']} users, total playtime {stats['total_playtime']} mins")

# -------------------- Step 6: TF-IDF Vector Comparison --------------------
from itertools import product

print("\nTF-IDF Spearman Rank Correlations (Each Recommendation ↔ Each Top Game):")

# Step A: Fetch TF-IDF vectors from the `games` table
def fetch_tfidf_vector(game_name):
    cursor.execute("""
        SELECT tfidf_vec_vector
        FROM games
        WHERE app_name = %s
        LIMIT 10;
    """, (game_name,))
    result = cursor.fetchone()
    return list(result[0]) if result and isinstance(result[0], list) else None

# Step B: Get TF-IDF vectors for target user's top games
target_game_vectors = {}
for game in target_user_top_game_names:
    vec = fetch_tfidf_vector(game)
    if vec:
        target_game_vectors[game] = vec

# Step C: Get TF-IDF vectors for recommended games
recommendation_game_vectors = {}
for game in recommendation_pool:
    if game not in target_user_top_game_names:  
        vec = fetch_tfidf_vector(game)
        if vec:
            recommendation_game_vectors[game] = vec

# Step D: Compute pairwise Spearman correlations
pairwise_corrs = []
for rec_game, rec_vec in recommendation_game_vectors.items():
    for tgt_game, tgt_vec in target_game_vectors.items():
        if len(rec_vec) == len(tgt_vec):
            corr, _ = spearmanr(rec_vec, tgt_vec)
            pairwise_corrs.append(corr)
            print(f"   Spearman({rec_game} ↔ {tgt_game}): {corr:.4f}")
        else:
            print(f"   ⚠️ Mismatched vector length: {rec_game} ↔ {tgt_game}")

# Step E: Print average
if pairwise_corrs:
    avg_corr = sum(pairwise_corrs) / len(pairwise_corrs)
    print(f"\n✅ Average Spearman Rank Correlation: {avg_corr:.4f}")
else:
    print("\n⚠️ No valid TF-IDF vector pairs for correlation.")
