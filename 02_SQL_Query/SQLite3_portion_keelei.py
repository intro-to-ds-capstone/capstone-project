import sqlite3
import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Database and CSV file names
db_name = 'publications.db'
csv_file = 'pubmed_data.csv' 

def create_database(db_name, csv_file):
    """
    Create a SQLite database and import data from a CSV file.
    """
    conn = None
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if the table already exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='publications'")
        if cursor.fetchone() is None:
            # Create table if it doesn't exist
            cursor.execute('''CREATE TABLE publications
                              (title TEXT, authors TEXT, publication_date TEXT, abstract TEXT)''')

            # Read CSV file and import data
            with open(csv_file, 'r', encoding='utf-8') as file:
                dr = csv.DictReader(file)
                to_db = [(i['Title'], i['Authors'], i['Publication Date'], i['Abstract']) for i in dr]

            cursor.executemany("INSERT INTO publications (title, authors, publication_date, abstract) VALUES (?, ?, ?, ?);", to_db)
            conn.commit()
            logging.info("Database created and data imported successfully.")
        else:
            logging.info("Database already exists. Skipping creation.")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if conn:
            conn.close()

def query_by_author(db_name, author_name):
    """
    Query the database for publications by a specific author.
    """
    conn = None
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # SQL query
        query = "SELECT title, publication_date FROM publications WHERE authors LIKE ?"
        cursor.execute(query, ('%'+author_name+'%',))

        # Fetch and display results
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(f"Title: {row[0]}, Publication Date: {row[1]}")
        else:
            print("No publications found for this author.")
            
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Create database and import data
    create_database(db_name, csv_file)

    # Example query
    author_name = input("Enter author's name to search for publications: ")
    query_by_author(db_name, author_name)