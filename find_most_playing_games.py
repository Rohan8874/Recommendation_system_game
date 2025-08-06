import psycopg2

def get_top_10_most_played_games():
    # Database connection parameters - update these with your credentials
    db_params = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'Rohan$123',
        'port': '5432'
    }
    
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Query to get top 10 games by total playtime
        query = """
        SELECT 
            item_name,
            SUM(playtime_forever) AS total_playtime_minutes,
            ROUND(SUM(playtime_forever)/60, 1) AS total_playtime_hours
        FROM 
            user_items
        GROUP BY 
            item_name
        ORDER BY 
            total_playtime_minutes DESC
        LIMIT 10;
        """
        
        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Close the connection
        cursor.close()
        conn.close()
        
        # Display results in serial order
        print("Top 10 Most Played Games:")
        print("-------------------------")
        for i, (game_name, minutes, hours) in enumerate(results, start=1):
            print(f"{i}. {game_name}")
            print(f"   Total Playtime: {minutes:,} minutes ({hours:,} hours)")
            print()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

if __name__ == "__main__":
    get_top_10_most_played_games()