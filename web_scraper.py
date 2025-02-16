import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import time
import random
from datetime import datetime
import logging
import os
import pandas as pd
import json
from pathlib import Path

from config import CONFIG, get_logger

logger = get_logger(__name__)

class WebScraper:
    def __init__(self):
        self.ua = UserAgent()
        self.driver = None
        self.setup_driver()
        if self.driver:
            self.wait = WebDriverWait(self.driver, 15)

        # ایجاد فولدر good_output اگر وجود نداشته باشد
        self.good_output_dir = Path(CONFIG['OUTPUT_DIR'])
        self.good_output_dir.mkdir(parents=True, exist_ok=True)

    def setup_driver(self):
        try:
            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--start-maximized')
            options.add_argument(f'user-agent={self.ua.random}')
            
            # Chrome path
            chrome_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
            ]
            
            chrome_path = None
            for path in chrome_paths:
                if os.path.exists(path):
                    chrome_path = path
                    break

            # Initialize the Chrome driver
            self.driver = uc.Chrome(
                options=options,
                browser_executable_path=chrome_path,
                version_main=None,
                use_subprocess=True
            )
            
            self.driver.set_page_load_timeout(CONFIG['TIMEOUT'])
            logger.info("Browser initialized successfully")

        except Exception as e:
            error_msg = f"Browser initialization error: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def search_google(self, keyword):
        try:
            logger.info(f"Searching for: {keyword}")
            self.driver.get("https://www.google.com")
            time.sleep(3)

            search_box = self.wait.until(EC.presence_of_element_located((By.NAME, "q")))
            search_box.clear()
            
            # Type keyword naturally
            for char in keyword:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))
            
            time.sleep(1)
            search_box.send_keys(Keys.RETURN)
            time.sleep(3)

            results = self.extract_results_from_page()
            time.sleep(2)

            # Try to get results from second page
            try:
                next_button = self.wait.until(EC.element_to_be_clickable((By.ID, "pnnext")))
                self.driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)
                second_page_results = self.extract_results_from_page()
                results.extend(second_page_results)
            except Exception as e:
                logger.warning(f"Could not get second page: {str(e)}")

            # ذخیره نتایج در فایل اکسل و JSON
            self.save_results_to_excel(keyword, results)
            self.save_results_to_json(keyword, results)

            return results[:20]

        except Exception as e:
            logger.error(f"Search error for '{keyword}': {str(e)}")
            return []

    def extract_results_from_page(self):
        results = []
        try:
            elements = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.g")))
            
            for element in elements:
                try:
                    title = element.find_element(By.CSS_SELECTOR, "h3").text
                    link = element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                    try:
                        description = element.find_element(By.CSS_SELECTOR, "div.VwiC3b").text
                    except:
                        description = ""
                    
                    if title and link and self.is_valid_url(link):
                        results.append({
                            'title': title,
                            'link': link,
                            'description': description,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                except:
                    continue

            return results

        except Exception as e:
            logger.error(f"Error extracting results: {str(e)}")
            return []

    def is_valid_url(self, url):
        blacklist = ['google.com', 'youtube.com', 'facebook.com']
        return url and not any(site in url.lower() for site in blacklist)

    def save_results_to_excel(self, keyword, results):
        try:
            df = pd.DataFrame(results)

            if not df.empty:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                excel_filename = self.good_output_dir / f"results_{keyword}_{timestamp}.xlsx"

                df.to_excel(excel_filename, index=False, engine='openpyxl')
                logger.info(f"نتایج با موفقیت در فایل اکسل ذخیره شد: {excel_filename}")
            else:
                logger.warning("هیچ نتیجه‌ای برای ذخیره در اکسل وجود ندارد.")

        except Exception as e:
            logger.error(f"خطا در ذخیره نتایج در اکسل: {str(e)}")

    def save_results_to_json(self, keyword, results):
        try:
            if results:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                json_filename = self.good_output_dir / f"results_{keyword}_{timestamp}.json"

                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                logger.info(f"نتایج با موفقیت در فایل JSON ذخیره شد: {json_filename}")
            else:
                logger.warning("هیچ نتیجه‌ای برای ذخیره در JSON وجود ندارد.")

        except Exception as e:
            logger.error(f"خطا در ذخیره نتایج در JSON: {str(e)}")

    def get_page_source(self):
        try:
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Error getting page source: {str(e)}")
            return None

    def close_browser(self):
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed successfully")
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")

    def __del__(self):
        try:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
                logger.info("Browser closed successfully")
                logger.info("ensuring close")
        except:
            pass