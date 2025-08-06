import psycopg2
from psycopg2 import sql
import json
from datetime import datetime

def get_user_game_stats(db_connection_params):
    """
    Retrieves game play statistics for all users from the PostgreSQL database.
    
    Args:
        db_connection_params (dict): Dictionary containing database connection parameters
            (e.g., {'host': 'localhost', 'database': 'postgres', 'user': 'postgres', 'password': 'Rohan$123', 'port': '5432'})
    
    Returns:
        dict: A dictionary with user_id as key and game statistics as value
    """
    user_stats = {}
    
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_connection_params)
        cursor = conn.cursor()
        
        # Query to get all users
        cursor.execute("SELECT user_id, steam_id, user_url FROM users;")
        users = cursor.fetchall()
        
        for user in users:
            user_id, steam_id, user_url = user
            
            # Query to get all games for this user with playtime
            cursor.execute(
                """
                SELECT item_id, item_name, playtime_forever, playtime_2weeks 
                FROM user_items 
                WHERE user_id = %s
                ORDER BY playtime_forever DESC;
                """,
                (user_id,)
            )
            games = cursor.fetchall()
            
            # Calculate total games and total playtime
            total_games = len(games)
            total_playtime = sum(game[2] for game in games) if games else 0
            
            # Prepare game list with human-readable playtime (in hours)
            game_list = []
            for game in games:
                item_id, item_name, playtime_forever, playtime_2weeks = game
                game_list.append({
                    'item_id': item_id,
                    'name': item_name,
                    'total_playtime_hours': round(playtime_forever / 60, 1),
                    'recent_playtime_hours': round(playtime_2weeks / 60, 1) if playtime_2weeks else 0
                })
            
            # Add user stats to the dictionary
            user_stats[user_id] = {
                'steam_id': steam_id,
                'profile_url': user_url,
                'total_games': total_games,
                'total_playtime_hours': round(total_playtime / 60, 1),
                'games': game_list
            }
            
        cursor.close()
        conn.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
        return None
    
    return user_stats

def save_stats_to_json(stats, filename=None):
    """
    Save the game statistics to a JSON file.
    
    Args:
        stats (dict): The game statistics dictionary returned by get_user_game_stats
        filename (str): Optional filename. If not provided, generates one with timestamp.
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"user_game_stats_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(stats, jsonfile, indent=4, ensure_ascii=False)
        
        print(f"Successfully saved statistics to {filename}")
        return filename
    
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
        return None

if __name__ == "__main__":
    # Replace with your actual database connection parameters
    db_params = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'Rohan$123',
        'port': '5432'  
    }
    
    # Get all user game statistics
    stats = get_user_game_stats(db_params)
    
    if stats:
        # Save to JSON
        json_file = save_stats_to_json(stats)
        
        # Print confirmation
        if json_file:
            print(f"\nData successfully saved to {json_file}")
            print("\nSample of the saved data:")
            print(json.dumps({k: stats[k] for k in list(stats.keys())[:1]}, indent=4))
        else:
            print("\nFailed to save data to JSON file.")
    else:
        print("Failed to retrieve user game statistics.")