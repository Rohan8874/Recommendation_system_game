import psycopg2
from psycopg2.extras import DictCursor
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.stats import spearmanr

# Database configuration
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Rohan$123',
    'host': 'localhost',
    'port': '5432'
}

def get_random_user_ids(limit=10):
    """Get random user IDs from the database for analysis."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cur:
            # Fixed query - using subquery to avoid DISTINCT + ORDER BY conflict
            cur.execute("""
                SELECT user_id FROM (
                    SELECT DISTINCT ui.user_id, RANDOM() as rnd
                    FROM user_items ui
                    JOIN games g ON ui.item_id = g.id
                    WHERE g.tfidf_vector IS NOT NULL
                ) t
                ORDER BY rnd
                LIMIT %s
            """, (limit,))
            results = cur.fetchall()
            return [row[0] for row in results] if results else []
    except Exception as e:
        print(f"Error fetching random user IDs: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_user_games(user_id):
    """Retrieve games played by a specific user from the database."""
    conn = None
    games = []
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        query = """
            SELECT item_id, item_name, playtime_forever, playtime_2weeks 
            FROM user_items 
            WHERE user_id = %s
            ORDER BY playtime_forever DESC
        """
        
        cursor.execute(query, (user_id,))
        
        for row in cursor.fetchall():
            games.append(dict(row))
            
    except psycopg2.Error as e:
        print(f"Database error for user {user_id}: {e}")
    finally:
        if conn:
            conn.close()
    return games

def get_user_game_vectors(user_id):
    """Get all TF-IDF vectors for games owned by a user."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT item_id 
                FROM user_items 
                WHERE user_id = %s
            """, (user_id,))
            game_ids = [row['item_id'] for row in cur.fetchall()]
            
            if not game_ids:
                return []
            
            cur.execute("""
                SELECT id, tfidf_vector 
                FROM games 
                WHERE id = ANY(%s) AND tfidf_vector IS NOT NULL
            """, (game_ids,))
            return [(row['id'], np.array(row['tfidf_vector'])) for row in cur.fetchall()]
            
    except Exception as e:
        print(f"Error fetching vectors for user {user_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def analyze_correlation(user_id):
    """Calculate Spearman's Rank Correlation for a single user."""
    try:
        user_games = get_user_games(user_id)
        if not user_games:
            print(f"User {user_id} has no games")
            return None
        
        game_vectors = get_user_game_vectors(user_id)
        if not game_vectors:
            print(f"User {user_id} has no games with TF-IDF vectors")
            return None
        
        playtime_ranks = {game['item_id']: i+1 for i, game in enumerate(user_games)}
        avg_vector = np.mean([vec for (_, vec) in game_vectors], axis=0)
        
        similarity_scores = []
        for game_id, vector in game_vectors:
            similarity = cosine_similarity([avg_vector], [vector])[0][0]
            similarity_scores.append((game_id, similarity))
        
        similarity_scores.sort(key=lambda x: x[1], reverse=True)
        similarity_ranks = {game_id: i+1 for i, (game_id, _) in enumerate(similarity_scores)}
        
        common_games = set(playtime_ranks.keys()) & set(similarity_ranks.keys())
        if len(common_games) < 3:
            print(f"User {user_id} has only {len(common_games)} games with both playtime and TF-IDF data (need at least 3)")
            return None
        
        playtime_rank_values = []
        similarity_rank_values = []
        
        for game_id in common_games:
            playtime_rank_values.append(playtime_ranks[game_id])
            similarity_rank_values.append(similarity_ranks[game_id])
        
        return spearmanr(playtime_rank_values, similarity_rank_values)
        
    except Exception as e:
        print(f"Error analyzing user {user_id}: {e}")
        return None

def analyze_multiple_users():
    """Analyze correlation for multiple users and calculate average."""
    user_ids = get_random_user_ids(10)
    if not user_ids:
        print("No users found with games that have TF-IDF vectors")
        print("Possible reasons:")
        print("1. Database connection failed")
        print("2. No users in user_items table")
        print("3. No games with tfidf_vector in games table")
        print("4. No users own games with tfidf_vector")
        return
    
    correlations = []
    valid_users = 0
    
    print("Analyzing 10 users...\n")
    print(f"{'User ID':<20} {'Correlation':<12} {'P-value':<10}")
    print("-" * 45)
    
    for user_id in user_ids:
        result = analyze_correlation(user_id)
        if result:
            corr, p_value = result
            correlations.append(corr)
            valid_users += 1
            print(f"{user_id:<20} {corr:.4f}{'*' if p_value < 0.05 else '':<11} {p_value:.4f}")
        else:
            print(f"{user_id:<20} Insufficient data")
    
    if valid_users > 0:
        avg_correlation = sum(correlations) / valid_users
        print("\nAverage Spearman's Rank Correlation:", round(avg_correlation, 4))
        print(f"Based on {valid_users} users with sufficient data")
        
        # Interpretation
        if avg_correlation > 0.5:
            print("Overall strong positive correlation: Users tend to play games similar to their library")
        elif avg_correlation > 0.3:
            print("Overall moderate positive correlation: Some relationship between similarity and playtime")
        elif avg_correlation > 0:
            print("Overall weak positive correlation: Slight tendency to play similar games")
        elif avg_correlation < -0.5:
            print("Overall strong negative correlation: Users tend to play games dissimilar to their library")
        elif avg_correlation < -0.3:
            print("Overall moderate negative correlation: Some inverse relationship")
        else:
            print("Overall negligible correlation: No clear relationship between similarity and playtime")
    else:
        print("\nNo valid correlation data could be calculated for any users")

if __name__ == "__main__":
    analyze_multiple_users()