import mysql.connector
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Update DB_CONFIG for PostgreSQL
DB_CONFIG = {
     'user': 'root',
     'password': 'yournewpassword',
     'host': 'localhost',
     'database': 'pipeline_db'
}

def fetch_api_data():
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    return response.json()

def fetch_web_data():
    url = 'https://quotes.toscrape.com'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    quotes = []
    for quote in soup.select('.quote'):
        text = quote.select_one('.text').text.strip()
        author = quote.select_one('.author').text.strip()
        quotes.append({'quote': text, 'author': author})
    return quotes

def clean_data(api_data, web_data):
    df_api = pd.DataFrame(api_data)[['id', 'name', 'email']]
    df_web = pd.DataFrame(web_data)
    df_web['quote'] = df_web['quote'].str.replace('“|”', '', regex=True)
    return df_api, df_web

def store_data_to_db(df_users, df_quotes):
    conn = mysql.connector.connect(**DB_CONFIG)# Use psycopg2 for PostgreSQL
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author VARCHAR(255)
        )
    """)

    # Insert data into users table
    for _, row in df_users.iterrows():
        cursor.execute(
            "REPLACE INTO users (id, name, email) VALUES (%s, %s, %s)",
            (row['id'], row['name'], row['email'])
        )

    # Insert data into quotes table
    for _, row in df_quotes.iterrows():
        cursor.execute(
            "INSERT INTO quotes (quote, author) VALUES (%s, %s)",
            (row['quote'], row['author'])
        )

    conn.commit()
    cursor.close()
    conn.close()

def run_pipeline():
    api_data = fetch_api_data()
    web_data = fetch_web_data()
    df_users, df_quotes = clean_data(api_data, web_data)
    store_data_to_db(df_users, df_quotes)

# Run the pipeline
run_pipeline()
