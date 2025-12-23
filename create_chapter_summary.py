import sqlite3
import os, apsw, re
from agentmake import agentmake
from agentmake.plugins.uba.lib.BibleParser import BibleVerseParser
from biblemate import AGENTMAKE_CONFIG
from agentmake import agentmake

DATABASE_NAME = 'ai_chapter_summary.db'

def initialize_db(db_name=DATABASE_NAME):
    """
    Connects to the SQLite database and creates the 'Summary' table 
    if it does not already exist.
    """
    try:
        # Connect to the SQLite database (creates the file if it doesn't exist)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # SQL command to create the table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS Summary (
            Book INTEGER,
            Chapter INTEGER,
            Content TEXT
        );
        """
        
        # Execute the table creation command
        cursor.execute(create_table_sql)
        conn.commit()
        
        print(f"Database '{db_name}' initialized successfully.")
        return conn
    except sqlite3.Error as e:
        print(f"An error occurred during database initialization: {e}")
        return None

def entry_exists(conn, book, chapter):
    """
    Check if an entity exists in the Summary table.
    """
    if conn is None:
        print("Cannot check: Database connection is not established.")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Summary WHERE Book=? AND Chapter=?", (book, chapter))
        fetch = cursor.fetchone()
        if fetch:
            return True
    except sqlite3.Error as e:
        print(f"An error occurred during insertion: {e}")
    return False

def check_is_summary(conn, book, chapter):
    """
    Check if an entity exists in the Summary table.
    """
    if conn is None:
        print("Cannot check: Database connection is not established.")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Summary WHERE Book=? AND Chapter=?", (book, chapter))
        fetch = cursor.fetchone()
        if fetch and not "Conclusion" in fetch[-1] and not "Summary" in fetch[-1] and not "If you’d like" in fetch[-1]:
            return False
        elif fetch and not fetch[-1].strip().endswith("[NO_CONTENT]"):
            return True
    except sqlite3.Error as e:
        print(f"An error occurred during insertion: {e}")
    return False


def insert_chapter(conn, book, chapter, content, update=False):
    """
    Inserts a new entry into the Summary table.
    
    Args:
        conn (sqlite3.Connection): The database connection object.
        book (int): The book number.
        chapter (int): The chapter number.
        content (str): The text of the Summary.
    """
    if conn is None:
        print("Cannot insert data: Database connection is not established.")
        return

    try:
        cursor = conn.cursor()
        if update:
            update_query = """
                UPDATE Summary 
                SET Content = ? 
                WHERE Book = ? AND Chapter = ?
            """
            cursor.execute(update_query, (content, book, chapter))
        else:
            insert_sql = """
            INSERT INTO Summary (Book, Chapter, Content)
            VALUES (?, ?, ?);
            """
            cursor.execute(insert_sql, (book, chapter, content))
        conn.commit()
        print(f"{'Updated' if update else 'Inserted'}: Book={book}, Chapter={chapter}")
        
    except sqlite3.Error as e:
        print(f"An error occurred during insertion: {e}")

def fetch_net_chapter(b, c):
    db = os.path.expanduser("~/UniqueBible/marvelData/bibles/NET.bible")
    with apsw.Connection(db) as connn:
        cursor = connn.cursor()
        cursor.execute("SELECT * FROM Verses WHERE Book=? AND Chapter=? ORDER BY Verse", (b,c))
        fetches = cursor.fetchall()
    return fetches

if __name__ == '__main__':
    # 1. Initialize the database and get the connection object
    db_connection = initialize_db()

    if db_connection:
        parser = BibleVerseParser(False)
        for i in range(1, 67):
            fetches = []
            c = 1
            while fetches or c == 1:
                print(f"Processing Book {i}, Chapter {c}")
                if entry_exists(db_connection, i, c):
                    fetches = 1
                    c += 1
                    continue
                fetches = fetch_net_chapter(i, c)
                if not fetches:
                    break
                try:
                    content = f"# Bible Chapter - {parser.bcvToVerseReference(i, c, 1, isChapter=True)}\n"
                    for *_, v, verse_text in fetches:
                        content += f"\n[{v}] {re.sub("<.*?>", "", verse_text.strip())}"
                    response = agentmake(content, system=os.path.join(os.getcwd(), "system.md"))[-1]["content"]
                    if response:
                        response = parser.parseText(response)
                        #print(response)
                        insert_chapter(db_connection, i, c, response, update=False)
                    else:
                        print(f"Error: {i} {c} got no response")
                        break
                except Exception as e:
                    print(f"Error processing Book {i}, Chapter {c}: {e}")
                c += 1
            