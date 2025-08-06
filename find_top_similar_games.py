import psycopg2
from psycopg2.extras import DictCursor
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def get_user_game_vectors(user_id):
    """
    Get all TF-IDF vectors for games owned by a user
    
    Args:
        user_id (str): The user ID to fetch games for
        
    Returns:
        list: List of TF-IDF vectors for the user's games
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Rohan$123",
            host="localhost",
            port="5432"
        )
        
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Get all game IDs owned by the user
            cur.execute("""
                SELECT item_id 
                FROM user_items 
                WHERE user_id = %s
            """, (user_id,))
            game_ids = [row['item_id'] for row in cur.fetchall()]
            
            if not game_ids:
                raise ValueError(f"No games found for user {user_id}")
            
            # Get TF-IDF vectors for all these games
            cur.execute("""
                SELECT id, tfidf_vector 
                FROM games 
                WHERE id = ANY(%s) AND tfidf_vector IS NOT NULL
            """, (game_ids,))
            game_vectors = [(row['id'], np.array(row['tfidf_vector'])) for row in cur.fetchall()]
            
            return game_vectors
            
    except Exception as e:
        print(f"Error fetching user games: {e}")
        return []
    finally:
        if conn:
            conn.close()

def find_similar_games_for_user(user_id, top_n=10):
    """
    Find top N most similar games to a user's game library
    
    Args:
        user_id (str): The user ID to find recommendations for
        top_n (int): Number of similar games to return
        
    Returns:
        list: List of similar games with similarity scores
    """
    conn = None
    try:
        # Get the user's game vectors
        user_game_vectors = get_user_game_vectors(user_id)
        if not user_game_vectors:
            return []
        
        # Calculate average vector of user's games
        avg_vector = np.mean([vec for (_, vec) in user_game_vectors], axis=0)
        
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Rohan$123",
            host="localhost",
            port="5432"
        )
        
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Get all games not owned by the user
            user_game_ids = [game_id for (game_id, _) in user_game_vectors]
            cur.execute("""
                SELECT id, app_name, tfidf_vector 
                FROM games 
                WHERE id != ALL(%s) AND tfidf_vector IS NOT NULL
            """, (user_game_ids,))
            games = cur.fetchall()
            
            # Calculate cosine similarities
            similarities = []
            for game in games:
                if game['tfidf_vector'] is not None:
                    game_array = np.array(game['tfidf_vector'])
                    similarity = cosine_similarity([avg_vector], [game_array])[0][0]
                    similarities.append((game['id'], game['app_name'], similarity))
            
            # Sort by similarity in descending order
            similarities.sort(key=lambda x: x[2], reverse=True)
            
            # Return top N similar games
            return similarities[:top_n]
            
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        if conn:
            conn.close()

# Example usage
if __name__ == "__main__":
    user_id = "76561198323066619" 
    similar_games = find_similar_games_for_user(user_id)
    
    print(f"Top 10 recommended games for user {user_id}:")
    for i, (game_id, game_name, similarity) in enumerate(similar_games, 1):
        print(f"{i}. {game_name} (ID: {game_id}) - Similarity: {similarity:.4f}")