import sqlite3
from pathlib import Path
import pandas as pd
from typing import Optional
from config import CONFIG, get_logger
import json  # اضافه شده برای تبدیل داده‌های headers به JSON

logger = get_logger(__name__)

class DatabaseManager:
    def __init__(self):
        try:
            # خواندن مسیر دیتابیس از config
            self.db_path = Path(CONFIG['DB_PATH'])
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # اتصال به دیتابیس
            self.conn = sqlite3.connect(str(self.db_path))
            self.cursor = self.conn.cursor()
            
            # فعال‌سازی قوانین foreign key
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self._create_tables()
            
            logger.info(f"Database initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            self.conn = None
            self.cursor = None
            raise

    def _create_tables(self):
        """ساخت جدول‌ها در صورت عدم وجود"""
        # جدول keywords
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE NOT NULL
            )
        ''')
        
        # جدول scraped_data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                description TEXT,
                headers TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            )
        ''')
        self.conn.commit()

    def get_keyword_id(self, keyword: str) -> int:
        """دریافت یا ایجاد شناسه برای کلمه کلیدی"""
        self.cursor.execute('SELECT id FROM keywords WHERE keyword = ?', (keyword,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            self.cursor.execute('INSERT INTO keywords (keyword) VALUES (?)', (keyword,))
            self.conn.commit()
            return self.cursor.lastrowid

    def insert_keyword(self, keyword: str) -> int:
        """درج کلمه کلیدی جدید و برگرداندن شناسه آن"""
        try:
            self.cursor.execute('INSERT INTO keywords (keyword) VALUES (?)', (keyword,))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:  # اگر کلمه کلیدی تکراری باشد
            self.cursor.execute('SELECT id FROM keywords WHERE keyword = ?', (keyword,))
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error inserting keyword: {str(e)}")
            return None

    def insert_link_data(
        self, 
        keyword_id: int, 
        url: str, 
        title: Optional[str] = None, 
        description: Optional[str] = None, 
        headers: Optional[str] = None
    ):
        """درج داده‌های استخراج‌شده"""
        try:
            self.cursor.execute('''
                INSERT INTO scraped_data 
                (keyword_id, url, title, description, headers)
                VALUES (?, ?, ?, ?, ?)
            ''', (keyword_id, url, title, description, headers))
            self.conn.commit()
            logger.info(f"Data inserted for URL: {url}")
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            self.conn.rollback()

    def insert_url_data(self, keyword_id: int, content: dict):
        """درج داده‌های لینک استخراج‌شده در دیتابیس"""
        try:
            url = content.get('url', '')
            title = content.get('title', '')
            description = content.get('meta_description', '')
            headers = json.dumps({
                'h1': content.get('h1', []),
                'h2': content.get('h2', []),
                'h3': content.get('h3', []),
                'h4': content.get('h4', []),
                'h5': content.get('h5', []),
                'h6': content.get('h6', [])
            }, ensure_ascii=False)
            self.insert_link_data(keyword_id, url, title, description, headers)
        except Exception as e:
            logger.error(f"Error inserting URL data: {str(e)}")

    def export_to_excel(self):
        """خروجی اکسل از دیتابیس"""
        try:
            keywords_df = pd.read_sql_query("SELECT * FROM keywords", self.conn)
            scraped_df = pd.read_sql_query("SELECT * FROM scraped_data", self.conn)
            
            with pd.ExcelWriter(CONFIG['DB_EXPORT_PATH']) as writer:
                keywords_df.to_excel(writer, sheet_name='Keywords', index=False)
                scraped_df.to_excel(writer, sheet_name='Scraped Data', index=False)
            
            logger.info(f"Exported to {CONFIG['DB_EXPORT_PATH']}")
            return True
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            return False

    def close(self):
        """بستن امن اتصال"""
        try:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}")

    def __del__(self):
        self.close()
