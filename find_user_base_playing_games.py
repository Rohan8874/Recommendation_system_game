import psycopg2
from psycopg2 import sql

def get_user_games(db_params, user_id, limit=None):
    """
    Retrieve games played by a specific user from the database.
    
    Args:
        db_params (dict): Database connection parameters
        user_id (str): The user_id to search for
        limit (int, optional): Limit the number of results (for top games)
    
    Returns:
        list: List of dictionaries containing game information
    """
    games = []
    
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Base query
        base_query = """
            SELECT item_id, item_name, playtime_forever, playtime_2weeks 
            FROM user_items 
            WHERE user_id = %s
            ORDER BY playtime_forever DESC
        """
        
        # Add LIMIT if specified
        if limit:
            query = base_query + f" LIMIT {limit}"
        else:
            query = base_query
        
        cursor.execute(query, (user_id,))
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        for row in rows:
            games.append({
                'item_id': row[0],
                'item_name': row[1],
                'playtime_forever': row[2],
                'playtime_2weeks': row[3]
            })
            
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
    
    return games

def display_games(games, title):
    """Display games in a formatted way."""
    print(f"\n{title}:")
    print("-" * 60)
    print(f"{'No.':<4} {'Game Name':<40} {'Total Hours':<12} {'Recent Hours (2wks)':<16}")
    print("-" * 60)
    
    for i, game in enumerate(games, 1):
        total_hours = round(game['playtime_forever'] / 60, 1)
        recent_hours = round(game['playtime_2weeks'] / 60, 1) if game['playtime_2weeks'] else 0
        print(f"{i:<4} {game['item_name'][:38]:<40} {total_hours:<12} {recent_hours:<16}")

# Example usage
if __name__ == "__main__":
    # Database connection parameters
    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Rohan$123',
        'host': 'localhost',
        'port': '5432'
    }
    
    # User ID to search for
    user_id_to_search = input("Enter the user ID to search: ")
    
    # Get all user games
    all_games = get_user_games(db_params, user_id_to_search)
    
    # Get top 10 games by playtime
    top_10_games = get_user_games(db_params, user_id_to_search, limit=10)
    
    # Display results
    if all_games:
        print(f"\nUser {user_id_to_search} has {len(all_games)} games in total.")
        display_games(top_10_games, "Top 10 Most Played Games")
        
        # Show additional stats
        total_hours_all = sum(game['playtime_forever'] for game in all_games) / 60
        top10_hours = sum(game['playtime_forever'] for game in top_10_games) / 60
        percentage = (top10_hours / total_hours_all) * 100 if total_hours_all > 0 else 0
        
        print(f"\nStatistics:")
        print(f"- Total playtime across all games: {round(total_hours_all, 1)} hours")
        print(f"- Top 10 games account for {round(percentage, 1)}% of total playtime")
    else:
        print(f"No games found for user {user_id_to_search}")