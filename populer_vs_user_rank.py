import psycopg2
import pandas as pd
from scipy.stats import spearmanr

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Rohan$123',
    'port': '5432'
}

def fetch_game_popularity_and_playtime():
    """
    Fetch data about game popularity and user playtime
    Returns DataFrame with user_id, items_count, game_popularity, and playtime
    """
    query = """
    WITH game_popularity AS (
        SELECT 
            item_id,
            COUNT(user_id) AS popularity
        FROM 
            user_items
        GROUP BY 
            item_id
    ),
    user_popular_games AS (
        SELECT 
            ui.user_id,
            gp.popularity,
            ui.playtime_forever
        FROM 
            user_items ui
        JOIN 
            game_popularity gp ON ui.item_id = gp.item_id
    ),
    user_most_popular_game AS (
        SELECT 
            user_id,
            MAX(popularity) AS max_popularity,
            MAX(playtime_forever) AS playtime
        FROM 
            user_popular_games
        GROUP BY 
            user_id
    )
    SELECT 
        u.user_id,
        u.items_count,
        ump.max_popularity AS game_popularity,
        ump.playtime
    FROM 
        users u
    JOIN 
        user_most_popular_game ump ON u.user_id = ump.user_id
    WHERE 
        ump.playtime > 0;
    """
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Database error: {e}")
        return None

def calculate_rank_correlation(df):
    """Calculate Spearman's rank correlation between game popularity and playtime"""
    if df is None or df.empty:
        print("No data available for analysis")
        return None
    
    # Calculate Spearman's rank correlation
    correlation = spearmanr(df['game_popularity'], df['playtime']).correlation
    return correlation

def main():
    print("Fetching data about game popularity and user playtime...")
    data = fetch_game_popularity_and_playtime()
    
    if data is not None:
        print(f"Analyzing {len(data)} users...")
        correlation = calculate_rank_correlation(data)
        print(f"Rank correlation coefficient between game popularity and user playtime: {correlation:.3f}")

if __name__ == "__main__":
    main()