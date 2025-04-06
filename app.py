from flask import Flask, render_template, redirect, url_for
import mysql.connector
from pipeline import run_pipeline, DB_CONFIG

app = Flask(__name__)



@app.route('/run')
def run():
    run_pipeline()
    return redirect(url_for('view_data'))

@app.route('/')
def index():
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    # Fetch all users
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()

    # Fetch all quotes
    cursor.execute("SELECT * FROM quotes")
    quotes = cursor.fetchall()

    cursor.close()
    connection.close()

    return render_template('index.html', users=users, quotes=quotes)

@app.route('/view')
def view_data():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()

    cursor.execute("SELECT * FROM quotes LIMIT 10")
    quotes = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template('index.html', users=users, quotes=quotes)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
