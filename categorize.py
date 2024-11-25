import psycopg2
from transformers import pipeline 
import os
from dotenv import load_dotenv

load_dotenv()

candidate_labels = ['Technology', 'Finance', 'Politics', 'AI', 'Aerospace']

connection = psycopg2.connect(
    dbname = os.getenv('PG_DBNAME'), 
    host = os.getenv('PG_HOST'), 
    user = os.getenv('PG_USER'), 
    password = os.getenv('PG_PASSWORD'), 
    port = os.getenv('PG_PORT')
)
cursor = connection.cursor()
cursor.execute('SELECT * FROM articles;')
data = cursor.fetchall()


def categorize_data(data):
    classifier = pipeline("zero-shot-classification",
                      model="facebook/bart-large-mnli")
   
    for article in data: 
        classified_dict = classifier(article[5], candidate_labels)
        predicted_category = classified_dict['labels'][0]
        cursor.execute("UPDATE articles SET category = %s WHERE id = %s;", (predicted_category, article[0]))

def createCategoryTables(labels):
    for label in labels:
            cursor.execute(f"""
                CREATE OR REPLACE TABLE "{label}"(
                    id INTEGER, 
                    title TEXT,
                    content TEXT, 
                    publication_date TIMESTAMP WITH TIME ZONE, 
                    source TEXT, 
                    description TEXT, 
                    urlToImage TEXT, 
                    url TEXT
                )
            """)
if __name__ == "__main__":

    # categorize_data(data)
    
    createCategoryTables(candidate_labels)
    connection.commit()

  
    