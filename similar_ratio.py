import psycopg2
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List, Dict, Tuple
from psycopg2.extras import execute_values

class UserSimilaritySystem:
    def __init__(self, db_config: Dict):
        """
        Initialize the system with database configuration
        
        Args:
            db_config: Dictionary containing database connection parameters
                      {'dbname': 'postgres', 'user': 'postgres', 
                       'password': 'Rohan$123', 'host': 'localhost'}
        """
        self.db_config = db_config
        self.model = SentenceTransformer('all-MiniLM-L6-v2')  
        self.embedding_dim = 384
        
        # Initialize database connection
        self.conn = psycopg2.connect(**self.db_config)
        self._setup_database()
    
    def _setup_database(self):
        """Setup the database with required extensions and columns"""
        with self.conn.cursor() as cur:
            # Enable pgvector extension if not exists
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            
            # Check if embedding columns exist, if not add them
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='users' AND column_name='user_embedding'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE users ADD COLUMN user_embedding vector({self.embedding_dim})")
            
            self.conn.commit()
    
    def generate_user_embeddings(self, batch_size: int = 100):
        """
        Generate embeddings for all users based on their game library and playtime
        
        Args:
            batch_size: Number of users to process at once
        """
        with self.conn.cursor() as cur:
            # Get total user count
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = cur.fetchone()[0]
            
            print(f"Generating embeddings for {total_users} users...")
            
            # Process users in batches
            for offset in range(0, total_users, batch_size):
                cur.execute("""
                    SELECT u.user_id, 
                           ARRAY_AGG(ui.item_name) as games, 
                           ARRAY_AGG(ui.playtime_forever) as playtimes
                    FROM users u
                    JOIN user_items ui ON u.user_id = ui.user_id
                    GROUP BY u.user_id
                    ORDER BY u.user_id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                
                batch = cur.fetchall()
                
                # Prepare data for embedding generation
                user_data = []
                for user_id, games, playtimes in batch:
                    # Create a text description of user's gaming preferences
                    game_descriptions = [f"{game} played for {time} minutes" 
                                        for game, time in zip(games, playtimes)]
                    user_text = " ".join(game_descriptions)
                    user_data.append((user_id, user_text))
                
                # Generate embeddings in batch
                user_ids, user_texts = zip(*user_data)
                embeddings = self.model.encode(user_texts, show_progress_bar=False)
                
                # Update database with embeddings
                update_data = [(embedding.tolist(), user_id) 
                             for embedding, user_id in zip(embeddings, user_ids)]
                
                execute_values(
                    cur,
                    "UPDATE users SET user_embedding = %s WHERE user_id = %s",
                    update_data
                )
                
                self.conn.commit()
                print(f"Processed {min(offset + batch_size, total_users)}/{total_users} users")
    
    def get_similar_users(self, target_user_id: str, top_n: int = 5) -> List[Tuple]:
        """
        Find similar users to the target user based on their embeddings
        
        Args:
            target_user_id: ID of the user to find similarities for
            top_n: Number of similar users to return
            
        Returns:
            List of tuples (user_id, steam_id, similarity_score)
        """
        with self.conn.cursor() as cur:
            # First get the target user's embedding
            cur.execute("""
                SELECT user_embedding FROM users WHERE user_id = %s
            """, (target_user_id,))
            
            result = cur.fetchone()
            if not result or not result[0]:
                raise ValueError(f"No embedding found for user {target_user_id}")
            
            target_embedding = result[0]
            
            # Find most similar users using cosine similarity
            cur.execute("""
                SELECT 
                    user_id, 
                    steam_id,
                    1 - (user_embedding <=> %s) AS similarity
                FROM users
                WHERE user_id != %s AND user_embedding IS NOT NULL
                ORDER BY similarity DESC
                LIMIT %s
            """, (target_embedding, target_user_id, top_n))
            
            return cur.fetchall()
    
    def create_embedding_index(self):
        """Create an index on the embedding column for faster similarity search"""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_embeddings 
                ON users USING ivfflat (user_embedding vector_cosine_ops)
                WITH (lists = 100)
            """)
            self.conn.commit()
    
    def close(self):
        """Close the database connection"""
        self.conn.close()

# Example Usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Rohan$123',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Initialize the system
    similarity_system = UserSimilaritySystem(db_config)
    
    try:
        # Step 1: Generate embeddings for all users
        similarity_system.generate_user_embeddings()
        
        # Step 2: Create index for faster search (optional but recommended for large datasets)
        similarity_system.create_embedding_index()
        
        # Step 3: Find similar users to a target user
        target_user = "doctr"  
        similar_users = similarity_system.get_similar_users(target_user, top_n=5)
        
        print(f"\nUsers most similar to {target_user}:")
        for user_id, steam_id, similarity in similar_users:
            print(f"User ID: {user_id}, Steam ID: {steam_id}, Similarity: {similarity:.3f}")
            
    finally:
        similarity_system.close()