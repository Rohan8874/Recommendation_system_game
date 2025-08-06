# i have postgres dataset and this dataset have in user table user_id,steam_id,items_count,user_url. 
# user_items table have user_id,item_id,playtime_forever,playtime_2weeks,item_name.
# games table have id,app_name,tfidf_vector.
# now i want to find a single users played games show base on playtime_forever and playtime_2weeks. 
# find top 10 games based on playtime_forever and playtime_2weeks.
# Now also i want to find compare same user games to all games similarty based on tfidf_vector 
# filter out duplicate games and keep only the best one. now top 10 games will be recommended to user.
# provide me python code to do this.

import psycopg2
import math

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

target_user_id = 'doctr'

# -------------------- Step 1: Get Top 10 Played Games --------------------
query = """
SELECT ui.item_id, ui.item_name, ui.playtime_forever, ui.playtime_2weeks, g.tfidf_vector
FROM user_items ui
JOIN games g ON ui.item_id = g.id
WHERE ui.user_id = %s
ORDER BY ui.playtime_forever DESC, ui.playtime_2weeks DESC
LIMIT 10;
"""
cursor.execute(query, (target_user_id,))
user_rows = cursor.fetchall()

def parse_vector(vec):
    if isinstance(vec, str):
        return [float(x.strip()) for x in vec.strip('{}').split(',')]
    elif isinstance(vec, list):
        return [float(x) for x in vec]
    else:
        raise ValueError("Unsupported TF-IDF vector format")


user_game_ids = []
user_vectors = []

print("Top 10 Played Games by Playtime:")
for row in user_rows:
    item_id, item_name, pf, p2w, tfidf_str = row
    print(f"{item_name} (Forever: {pf} mins, 2 Weeks: {p2w} mins)")
    user_game_ids.append(item_id)
    user_vectors.append(parse_vector(tfidf_str))

# -------------------- Step 2: Fetch All Games --------------------
cursor.execute("SELECT id, tfidf_vector FROM games;")
all_rows = cursor.fetchall()

game_vector_dict = {}
for gid, vec_str in all_rows:
    game_vector_dict[gid] = parse_vector(vec_str)

# -------------------- Step 3: Cosine Similarity Function --------------------
def cosine_similarity(vec1, vec2):
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    mag1 = math.sqrt(sum(a * a for a in vec1))
    mag2 = math.sqrt(sum(b * b for b in vec2))
    if mag1 == 0 or mag2 == 0:
        return 0.0
    return dot_product / (mag1 * mag2)

# -------------------- Step 4: Compare User Games to All Games --------------------
similarities = {}  

for user_vec in user_vectors:
    for game_id, vec in game_vector_dict.items():
        if game_id in user_game_ids:
            continue  
        sim = cosine_similarity(user_vec, vec)
        if game_id not in similarities or sim > similarities[game_id]:
            similarities[game_id] = sim

sorted_sims = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
top_10 = sorted_sims[:10]
top_10_ids = tuple([gid for gid, sim in top_10])

cursor.execute("SELECT id, app_name FROM games WHERE id IN %s", (top_10_ids,))
recommended_rows = cursor.fetchall()

print("\nTop 10 Recommended Games:")
for game_id, app_name in recommended_rows:
    print(f"{app_name} (Game ID: {game_id})")

cursor.close()
conn.close()
