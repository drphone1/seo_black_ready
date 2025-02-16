import sqlite3
import pandas as pd
from pathlib import Path
from config import CONFIG, get_logger

logger = get_logger(__name__)

class DatabaseViewer:
    def __init__(self):
        self.db_path = Path(CONFIG['OUTPUT_DIR']) / 'seo_data.db'
        
    def view_keywords(self):
        """نمایش تمام کلمات کلیدی"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                query = '''
                SELECT id, keyword, created_at 
                FROM keywords 
                ORDER BY created_at DESC
                '''
                df = pd.read_sql_query(query, conn)
                return df
        except Exception as e:
            logger.error(f"Error viewing keywords: {str(e)}")
            return None

    def view_urls_for_keyword(self, keyword_id):
        """نمایش تمام URL‌های مربوط به یک کلمه کلیدی"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                query = '''
                SELECT 
                    u.url,
                    u.title,
                    u.meta_description,
                    u.google_rank,
                    u.content_score,
                    u.h1,
                    u.h2,
                    u.h3,
                    u.main_content,
                    k.keyword,
                    u.created_at
                FROM urls u
                JOIN keywords k ON u.keyword_id = k.id
                WHERE k.id = ?
                ORDER BY u.google_rank
                '''
                df = pd.read_sql_query(query, conn, params=(keyword_id,))
                
                # تبدیل JSON strings به لیست‌های قابل خواندن
                for col in ['h1', 'h2', 'h3']:
                    df[col] = df[col].apply(lambda x: json.loads(x) if x else [])
                    df[col] = df[col].apply(lambda x: ' | '.join(x) if x else '')
                
                # کوتاه کردن متن‌های طولانی برای نمایش بهتر
                df['meta_description'] = df['meta_description'].str[:100] + '...'
                df['main_content'] = df['main_content'].str[:100] + '...'
                
                return df
        except Exception as e:
            logger.error(f"Error viewing URLs for keyword {keyword_id}: {str(e)}")
            return None

    def export_to_excel(self, keyword_id=None):
        """صدور اطلاعات به اکسل"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                # Get all data with keyword information
                query = '''
                SELECT 
                    k.keyword,
                    u.url,
                    u.title,
                    u.description,
                    u.meta_description,
                    u.google_rank,
                    u.content_score,
                    u.h1,
                    u.h2,
                    u.created_at
                FROM urls u
                JOIN keywords k ON u.keyword_id = k.id
                '''
                if keyword_id:
                    query += ' WHERE k.id = ?'
                    df = pd.read_sql_query(query, conn, params=(keyword_id,))
                else:
                    df = pd.read_sql_query(query, conn)

                # Save to Excel
                output_file = Path(CONFIG['OUTPUT_DIR']) / 'database_export.xlsx'
                df.to_excel(output_file, index=False)
                logger.info(f"Data exported to {output_file}")
                return output_file
        except Exception as e:
            logger.error(f"Error exporting data: {str(e)}")
            return None

    def get_keyword_id(self, keyword):
        """دریافت شناسه کلمه کلیدی با استفاده از متن کلمه"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM keywords WHERE keyword = ?', (keyword,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting keyword ID: {str(e)}")
            return None

    def format_dataframe(self, df):
        """فرمت‌بندی DataFrame برای نمایش بهتر"""
        if df is None or df.empty:
            return "هیچ داده‌ای یافت نشد!"
        
        # تنظیم عرض نمایش pandas
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 40)
        
        # تنظیم راست‌چین برای متون فارسی
        return df.style.set_properties(**{
            'text-align': 'right',
            'font-family': 'Tahoma',
            'white-space': 'pre-wrap'
        })

def main():
    viewer = DatabaseViewer()
    
    while True:
        print("\n" + "=" * 80)
        print("=== مدیریت دیتابیس SEO ===".center(80))
        print("=" * 80)
        print("1. نمایش تمام کلمات کلیدی")
        print("2. نمایش URL‌های یک کلمه کلیدی")
        print("3. صدور به اکسل")
        print("4. خروج")
        print("-" * 80)
        
        choice = input("\nلطفاً یک گزینه را انتخاب کنید: ")
        
        if choice == "1":
            keywords = viewer.view_keywords()
            if keywords is not None:
                print("\nکلمات کلیدی موجود:")
                print("-" * 80)
                print(viewer.format_dataframe(keywords))
                print("-" * 80)
                print("\nراهنما: از شناسه (id) برای جستجوی URL‌ها استفاده کنید")
        
        elif choice == "2":
            print("\nکلمات کلیدی موجود:")
            keywords = viewer.view_keywords()
            if keywords is not None:
                print(keywords.to_string(index=False))
                
            keyword_input = input("\nشناسه (id) یا متن کلمه کلیدی را وارد کنید: ")
            
            try:
                # اول سعی می‌کنیم به عنوان شناسه عددی در نظر بگیریم
                keyword_id = int(keyword_input)
            except ValueError:
                # اگر عدد نبود، سعی می‌کنیم متن کلمه را پیدا کنیم
                keyword_id = viewer.get_keyword_id(keyword_input)
                if keyword_id is None:
                    print(f"کلمه کلیدی '{keyword_input}' پیدا نشد!")
                    continue
            
            urls = viewer.view_urls_for_keyword(keyword_id)
            if urls is not None and not urls.empty:
                print("\nURL‌های مرتبط:")
                print("-" * 80)
                display_cols = [
                    'url', 'title', 'meta_description', 'google_rank', 
                    'content_score', 'h1', 'h2', 'h3'
                ]
                print(viewer.format_dataframe(urls[display_cols]))
                print("\nنکته: برای دیدن محتوای کامل، از گزینه صدور به اکسل استفاده کنید")
            else:
                print("هیچ URL‌ای برای این کلمه کلیدی پیدا نشد!")
        
        elif choice == "3":
            print("\nکلمات کلیدی موجود:")
            keywords = viewer.view_keywords()
            if keywords is not None:
                print(keywords.to_string(index=False))
                
            keyword_input = input("\nشناسه (id) یا متن کلمه کلیدی را وارد کنید (Enter برای همه): ")
            
            if keyword_input.strip():
                try:
                    keyword_id = int(keyword_input)
                except ValueError:
                    keyword_id = viewer.get_keyword_id(keyword_input)
                    if keyword_id is None:
                        print(f"کلمه کلیدی '{keyword_input}' پیدا نشد!")
                        continue
            else:
                keyword_id = None
                
            output_file = viewer.export_to_excel(keyword_id)
            if output_file:
                print(f"\nداده‌ها در فایل زیر ذخیره شدند:\n{output_file}")
        
        elif choice == "4":
            break

if __name__ == "__main__":
    main()
