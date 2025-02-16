import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import os
from pathlib import Path
import json
import time
import random

from config import CONFIG, get_logger

logger = get_logger(__name__)

class ContentScraper:
    def __init__(self):
        self.output_dir = Path(CONFIG['OUTPUT_DIR'])
        self.content_dir = self.output_dir / "content"
        self.content_dir.mkdir(parents=True, exist_ok=True)

    def fetch_page_content(self, url):
        """دریافت محتوای صفحه از طریق URL"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            # استفاده از پراکسی خالی برای جلوگیری از استفاده از پراکسی نامعتبر
            response = requests.get(url, headers=headers, timeout=CONFIG['TIMEOUT'], proxies={})
            response.raise_for_status()
            time.sleep(random.uniform(1, 3))
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def calculate_content_score(self, content, google_rank):
        """محاسبه امتیاز محتوا بر اساس فاکتورهای مختلف"""
        try:
            score = 0
            
            # امتیاز بر اساس رتبه گوگل (رتبه 1 بیشترین امتیاز)
            rank_score = (21 - google_rank) * 5  # رتبه 1 = 100, رتبه 20 = 5
            score += rank_score

            # امتیاز برای متا دیسکریپشن
            if content.get('meta_description'):
                score += 10

            # امتیاز برای هدینگ‌ها
            heading_weights = {
                'h1': 20, 'h2': 15, 'h3': 10,
                'h4': 5, 'h5': 3, 'h6': 2
            }
            
            for h_type, weight in heading_weights.items():
                score += len(content.get(h_type, [])) * weight

            # امتیاز برای محتوای اصلی (بر اساس طول)
            main_content_length = len(content.get('main_content', ''))
            if main_content_length > 1000:
                score += 50
            elif main_content_length > 500:
                score += 30
            elif main_content_length > 200:
                score += 15

            # امتیاز برای جداول
            score += len(content.get('tables', [])) * 15

            return round(score, 2)
            
        except Exception as e:
            logger.error(f"Error calculating content score: {str(e)}")
            return 0

    def extract_tables(self, table):
        """استخراج جدول با استفاده از StringIO"""
        try:
            from io import StringIO
            table_html = str(table)
            return pd.read_html(StringIO(table_html))[0]
        except Exception as e:
            logger.error(f"Error extracting table: {str(e)}")
            return None

    def extract_content(self, html_content, url, google_rank=0):
        """استخراج محتوای صفحه از HTML با امتیازدهی"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            content = {
                'url': url,
                'title': soup.title.string.strip() if soup.title else "No Title",
                'meta_description': '',
                'h1': [], 'h2': [], 'h3': [], 'h4': [], 'h5': [], 'h6': [],
                'tables': [],
                'main_content': '',
                'google_rank': google_rank
            }
            
            # Extract meta description
            meta_desc = soup.find('meta', {'name': ['description', 'Description']})
            if meta_desc:
                content['meta_description'] = meta_desc.get('content', '').strip()

            # Extract headings
            for i in range(1, 7):
                content[f'h{i}'] = [h.get_text().strip() for h in soup.find_all(f'h{i}') if h.get_text().strip()]

            # Extract tables with new method
            tables = []
            for table in soup.find_all('table'):
                try:
                    df = self.extract_tables(table)
                    if df is not None:
                        tables.append(df.to_dict('records'))
                except:
                    continue
            content['tables'] = tables

            # Extract main content
            main_content = []
            for p in soup.find_all(['p', 'article', 'section', 'div']):
                text = p.get_text().strip()
                if text and len(text) > 50:
                    main_content.append(text)
            content['main_content'] = '\n\n'.join(main_content)

            # Calculate content score
            content['content_score'] = self.calculate_content_score(content, google_rank)

            return content

        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return None

    def save_content_to_excel(self, url, content, excel_file):
        """ذخیره محتوا در اکسل با اضافه کردن رتبه و امتیاز"""
        try:
            if not content:
                logger.warning(f"No content to save for {url}")
                return

            data = {
                'URL': [url],
                'Google Rank': [content.get('google_rank', 0)],
                'Content Score': [content.get('content_score', 0)],
                'Title': [content['title']],
                'Meta Description': [content['meta_description']],
                'H1': [' | '.join(content['h1'])],
                'H2': [' | '.join(content['h2'])],
                'H3': [' | '.join(content['h3'])],
                'H4': [' | '.join(content['h4'])],
                'H5': [' | '.join(content['h5'])],
                'H6': [' | '.join(content['h6'])],
                'Main Content': [content['main_content']],
                'Tables': [json.dumps(content['tables'], ensure_ascii=False)],
                'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            }
            
            df = pd.DataFrame(data)
            
            excel_path = Path(excel_file)
            if (excel_path.exists()):
                try:
                    existing_df = pd.read_excel(excel_path)
                    existing_df = existing_df[existing_df['URL'] != url]
                    df = pd.concat([existing_df, df], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error reading existing Excel file: {str(e)}")
            
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Content')
                
                # Format worksheet
                workbook = writer.book
                worksheet = writer.sheets['Content']
                
                # Add formats
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'bg_color': '#D9EAD3',
                    'border': 1
                })
                
                # Format headers and column widths
                for idx, col in enumerate(df.columns):
                    worksheet.write(0, idx, col, header_format)
                    if col in ['URL', 'Title', 'Meta Description']:
                        worksheet.set_column(idx, idx, 40)
                    elif col in ['Main Content']:
                        worksheet.set_column(idx, idx, 60)
                    else:
                        worksheet.set_column(idx, idx, 30)

            logger.info(f"Content saved to {excel_file}")

        except Exception as e:
            logger.error(f"Error saving content to Excel: {str(e)}")

    def scrape_content_from_url(self, url, excel_file, db_manager=None, keyword_id=None, google_rank=0):
        """اسکرپ محتوای یک URL و ذخیره در اکسل و دیتابیس"""
        try:
            logger.info(f"Scraping content from: {url} (Rank: {google_rank})")
            html_content = self.fetch_page_content(url)
            if html_content:
                content = self.extract_content(html_content, url, google_rank)
                if content:
                    self.save_content_to_excel(url, content, excel_file)
                    
                    # Save to database if database manager is provided
                    if db_manager and keyword_id:
                        logger.info(f"Saving to database: {url} (Keyword ID: {keyword_id}, Rank: {google_rank})")
                        db_manager.insert_url_data(keyword_id, content)
                    else:
                        logger.warning("Database manager or keyword_id not provided")
                    
                    return True
            return False
        except Exception as e:
            logger.error(f"Error scraping content from {url}: {str(e)}")
            return False

    def scrape_content_from_excel(self, input_excel_file, output_excel_file, db_manager=None):
        """اسکرپ محتوای لینک‌ها از فایل اکسل با پشتیبانی از دیتابیس"""
        try:
            logger.info(f"Reading links from: {input_excel_file}")
            df = pd.read_excel(input_excel_file)
            
            if 'link' in df.columns and 'keyword' in df.columns:
                unique_links = df[['link', 'keyword']].drop_duplicates()
                total_links = len(unique_links)
                logger.info(f"Found {total_links} unique links to process")
                
                for _, row in unique_links.iterrows():
                    url = row['link']
                    keyword = row['keyword']
                    
                    # Get keyword_id if database manager is provided
                    keyword_id = None
                    if db_manager:
                        keyword_id = db_manager.get_keyword_id(keyword)
                    
                    logger.info(f"Processing link for keyword '{keyword}': {url}")
                    self.scrape_content_from_url(
                        url=url,
                        excel_file=output_excel_file,
                        db_manager=db_manager,
                        keyword_id=keyword_id
                    )
                    time.sleep(random.uniform(2, 4))
                
                logger.info("Content scraping completed successfully")
            else:
                logger.error("Required columns 'link' and 'keyword' not found in the Excel file")
                
        except Exception as e:
            logger.error(f"Error processing Excel file: {str(e)}")