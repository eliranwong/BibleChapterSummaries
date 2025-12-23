from agentmake.plugins.chinese.convert_tc import convert_traditional_chinese
import os, apsw, sqlite3


DATABASE_NAME = 'ai_chapter_summary_sc.db'

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

def insert_summary(conn, book, chapter, content, update=False):
    """
    Inserts a new entry into the Commentary table.
    
    Args:
        conn (sqlite3.Connection): The database connection object.
        book (int): The book number.
        chapter (int): The chapter number.
        content (str): The text of the commentary.
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

def fetch_tc_commentaries():
    db = os.path.join(os.getcwd(), "ai_chapter_summary_tc.db")
    with apsw.Connection(db) as connn:
        cursor = connn.cursor()
        cursor.execute("SELECT * FROM Summary")
        fetches = cursor.fetchall()
    return fetches

if __name__ == '__main__':
    # 1. Initialize the database and get the connection object
    db_connection = initialize_db()

    if db_connection:
        for b, c, content in fetch_tc_commentaries():
            print("Working on verse:", b, c)
            content_sc = convert_traditional_chinese(content, print_on_terminal=False)
            insert_summary(db_connection, b, c, content_sc, update=False)

