# Database.py
import mysql.connector
from mysql.connector import Error
from datetime import datetime

class Database:
    def __init__(self, host, user, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor()
            print("Connected to MySQL database")

        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.conn = None

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(255) UNIQUE,
            content TEXT,
            publish_time DATETIME,
            receive_time DATETIME,
            latency_ms INT
        );
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_message(self, message_id, content, publish_time, receive_time):
        latency = int((receive_time - publish_time).total_seconds() * 1000)

        query = """
        INSERT INTO messages (message_id, content, publish_time, receive_time, latency_ms)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (message_id, content, publish_time, receive_time, latency)

        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Error as e:
            print(f"MySQL insert error: {e}")
            return False

    def message_exists(self, message_id):
        query = "SELECT COUNT(*) FROM messages WHERE message_id = %s"
        self.cursor.execute(query, (message_id,))
        return self.cursor.fetchone()[0] > 0

    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()