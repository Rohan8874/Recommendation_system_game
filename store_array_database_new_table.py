import psycopg2

# -------------------- PostgreSQL Connection --------------------
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Rohan$123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Get ordered item IDs (game_order)
cursor.execute("""
    SELECT DISTINCT item_id
    FROM user_items
    ORDER BY item_id;
""")
game_order = [row[0] for row in cursor.fetchall()]

# Get all users
cursor.execute("SELECT DISTINCT user_id FROM user_items;")
all_user_ids = [row[0] for row in cursor.fetchall()]

# Loop through each user
for user_id in all_user_ids:
    cursor.execute("""
        SELECT item_id, playtime_forever
        FROM user_items
        WHERE user_id = %s;
    """, (user_id,))
    user_games = cursor.fetchall()

    user_play_dict = {item_id: playtime for item_id, playtime in user_games}
    total_playtime = sum(user_play_dict.values())

    if total_playtime == 0:
        total_playtime = 1

    # Build ratio vector
    ratio_vector = [round(user_play_dict.get(item_id, 0) / total_playtime, 6) for item_id in game_order]

    # Convert to space-separated string for pgvector
    vector_str = f"[{', '.join(map(str, ratio_vector))}]"

    # Insert into user_play_ratio
    cursor.execute("""
        INSERT INTO user_play_ratio (user_id, playtime_vector)
        VALUES (%s, %s)
        ON CONFLICT (user_id) DO UPDATE
        SET playtime_vector = EXCLUDED.playtime_vector;
    """, (user_id, vector_str))

# Finalize
conn.commit()
cursor.close()
conn.close()
print("All user playtime vectors stored in the PostgreSQL vector format.")
