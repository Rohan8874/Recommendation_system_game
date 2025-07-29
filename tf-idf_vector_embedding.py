import psycopg2
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import create_engine

class TFIDFProcessor:
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:Rohan$123@localhost/postgres')
        
    def get_db_connection(self):
        """Establish a database connection"""
        return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Rohan$123",
            host="localhost",
            port=5432
        )

    def add_tfidf_column(self):
        """Add TF-IDF vector column if it doesn't exist"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name='games' AND column_name='tfidf_vector'
                        ) THEN
                            ALTER TABLE games ADD COLUMN tfidf_vector FLOAT8[];
                        END IF;
                    END $$;
                """)
                conn.commit()
                print("✅ Verified/added tfidf_vector column")

    def fetch_text_data(self):
        """Fetch and combine all relevant text data from games table"""
        query = """
            SELECT 
                id,
                COALESCE(title, '') || ' ' ||
                COALESCE(app_name, '') || ' ' ||
                COALESCE(developer, '') || ' ' ||
                COALESCE(publisher, '') || ' ' ||
                COALESCE(array_to_string(genres, ' '), '') || ' ' ||
                COALESCE(array_to_string(tags, ' '), '') || ' ' ||
                COALESCE(array_to_string(specs, ' '), '') || ' ' ||
                COALESCE(sentiment, '') || ' ' ||
                COALESCE(price::text, '') || ' ' ||
                COALESCE(discount_price::text, '') || ' ' ||
                COALESCE(release_date::text, '') AS combined_text
            FROM games;
        """
        return pd.read_sql(query, self.engine)

    def calculate_and_store_tfidf(self):
        """Calculate TF-IDF vectors and store in database"""
        df = self.fetch_text_data()
        if df.empty:
            print("⚠️ No data to process")
            return

        print("🔍 Calculating TF-IDF vectors...")
        vectorizer = TfidfVectorizer(max_features=5000)  # Limit features for performance
        tfidf_matrix = vectorizer.fit_transform(df['combined_text'])
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Store TF-IDF vectors
                for i, game_id in enumerate(df['id']):
                    tfidf_vector = tfidf_matrix[i].toarray()[0].tolist()
                    
                    cur.execute("""
                        UPDATE games 
                        SET tfidf_vector = %s
                        WHERE id = %s
                    """, (tfidf_vector, game_id))
                    
                    if i % 100 == 0:
                        print(f"⏳ Processed {i+1}/{len(df)} games")
                        conn.commit()
                
                conn.commit()
                print(f"✅ Stored TF-IDF vectors for {len(df)} games")

    def create_index(self):
        """Create index for the TF-IDF vector column"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_games_tfidf_vector 
                    ON games USING GIN(tfidf_vector array_ops);
                """)
                conn.commit()
                print("✅ Created GIN index on tfidf_vector")

    def run(self):
        """Execute the full TF-IDF processing pipeline"""
        try:
            print("🚀 Starting TF-IDF processing pipeline")
            
            # Database preparation
            self.add_tfidf_column()
            
            # TF-IDF calculation and storage
            self.calculate_and_store_tfidf()
            
            # Create index for performance
            self.create_index()
            
            print("🎉 TF-IDF processing completed successfully!")
        except Exception as e:
            print(f"❌ Error during processing: {str(e)}")
            raise

if __name__ == "__main__":
    processor = TFIDFProcessor()
    processor.run()