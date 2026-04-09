# Database.py
import mysql.connector
from mysql.connector import Error

class Database:
    def __init__(self, host, user, password, database):
        self.conn = None
        self.cursor = None

        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor(dictionary=True)
            # print(f"Connected to MySQL database: {database}")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    # ---------------- PUBLISHER TABLE ----------------
    def create_publisher_table(self):
        if not self.conn or not self.cursor:
            print("No database connection. Publisher table not created.")
            return

        query = """
        CREATE TABLE IF NOT EXISTS published_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(255),
            content TEXT,
            publish_time DATETIME
        );
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_published_message(self, message_id, content, publish_time):
        if not self.conn or not self.cursor:
            print("No database connection. Cannot insert publisher message.")
            return False

        query = """
        INSERT INTO published_messages (message_id, content, publish_time)
        VALUES (%s, %s, %s)
        """
        values = (message_id, content, publish_time)

        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Error as e:
            print(f"MySQL insert error (publisher): {e}")
            return False

    def fetch_published_messages(self):
        if not self.conn or not self.cursor:
            print("No database connection. Cannot fetch published messages.")
            return []

        query = """
        SELECT id, message_id, content, publish_time
        FROM published_messages
        ORDER BY id DESC
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # ---------------- SUBSCRIBER TABLE ----------------
    def create_subscriber_table(self):
        if not self.conn or not self.cursor:
            print("No database connection. Subscriber table not created.")
            return

        query = """
        CREATE TABLE IF NOT EXISTS received_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(255),
            content TEXT,
            publish_time DATETIME,
            receive_time DATETIME,
            latency_ms INT,
            is_duplicate BOOLEAN,
            INDEX idx_message_id (message_id)
            INDEX idx_receive_time (receive_time)
        );
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_received_message(self, message_id, content, publish_time, receive_time, is_duplicate):
        if not self.conn or not self.cursor:
            print("No database connection. Cannot insert subscriber message.")
            return False

        latency = int((receive_time - publish_time).total_seconds() * 1000)

        query = """
        INSERT INTO received_messages
        (message_id, content, publish_time, receive_time, latency_ms, is_duplicate)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (message_id, content, publish_time, receive_time, latency, is_duplicate)

        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Error as e:
            print(f"MySQL insert error (subscriber): {e}")
            return False

    def message_exists(self, message_id):
        if not self.conn or not self.cursor:
            print("No database connection. Cannot check duplicates.")
            return False

        query = "SELECT 1 FROM received_messages WHERE message_id = %s LIMIT 1"
        self.cursor.execute(query, (message_id,))
        result = self.cursor.fetchone()
        return result is not None 

    def fetch_received_messages(self):
        if not self.conn or not self.cursor:
            print("No database connection. Cannot fetch received messages.")
            return []

        query = """
        SELECT id, message_id, content, publish_time, receive_time, latency_ms, is_duplicate
        FROM received_messages
        ORDER BY id DESC
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # ---------------- CLOSE CONNECTION ----------------
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()