import json
import pandas as pd
from pathlib import Path
import time
from datetime import datetime
from tqdm import tqdm
from config import CONFIG, get_logger
from web_scraper import WebScraper
from content_scraper import ContentScraper
from database_manager import DatabaseManager

logger = get_logger(__name__)

def main():
    try:
        # تعریف output_dir در ابتدای تابع
        output_dir = Path(CONFIG['OUTPUT_DIR'])
        
        # Print banner
        print("=" * 50)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Current Date and Time: {current_time}")
        print(f"Computer Name: {CONFIG['USER']}")
        print(f"Version: {CONFIG['VERSION']}")
        print("=" * 50)
        print()

        # Load keywords
        logger.info("Loading keywords...")
        with open('keywords.txt', 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(keywords)} keywords")

        # Initialize scraper
        logger.info("Initializing scraper...")
        scraper = WebScraper()
        
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Process keywords
        all_results = {}
        for keyword in tqdm(keywords, desc="Processing keywords"):
            try:
                # Store keyword in database
                keyword_id = db_manager.insert_keyword(keyword)
                
                results = scraper.search_google(keyword)
                if results:
                    # Add Google ranking to results
                    for rank, result in enumerate(results, 1):
                        result['google_rank'] = rank
                    
                    all_results[keyword] = results
                    
                    # Scrape content for each result with rank information
                    content_scraper = ContentScraper()
                    output_excel_file = output_dir / f'content_results_{keyword}.xlsx'
                    
                    for result in results:
                        url = result['link']
                        google_rank = result['google_rank']  # Get the rank
                        content_scraper.scrape_content_from_url(
                            url=url,
                            excel_file=str(output_excel_file),
                            db_manager=db_manager,
                            keyword_id=keyword_id,
                            google_rank=google_rank  # Pass the rank to the scraper
                        )
                        time.sleep(2)
                
                time.sleep(2)
            except Exception as e:
                logger.error(f"Error processing keyword '{keyword}': {str(e)}")
                continue

        # Save combined results
        if all_results:
            # Save to Excel
            output_excel_file = output_dir / 'results_keywords.xlsx'
            all_df = pd.DataFrame()
            
            for keyword, results in all_results.items():
                df = pd.DataFrame(results)
                df['keyword'] = keyword
                all_df = pd.concat([all_df, df], ignore_index=True)
            
            all_df.to_excel(output_excel_file, index=False)
            logger.info(f"Results saved to {output_excel_file}")

            # Save to JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_json_file = output_dir / f'results_{timestamp}.json'
            with open(output_json_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=4)
            logger.info(f"Results saved to {output_json_file}")
        else:
            logger.warning("No results were collected")

        # Ask user about content scraping
        scrape_content = input("\nDo you want to scrape content from links? (yes/no): ").strip().lower()
        
        if scrape_content == 'yes':
            content_scraper = ContentScraper()
            input_excel_file = str(output_dir / 'results_keywords.xlsx')
            output_excel_file = str(output_dir / 'content_results.xlsx')
            # ارسال دیتابیس منیجر برای ذخیره در دیتابیس
            content_scraper.scrape_content_from_excel(
                input_excel_file, 
                output_excel_file,
                db_manager=db_manager
            )
            print(f"Content scraping completed. Results saved to {output_excel_file}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
    
    finally:
        try:
            input("\nPress Enter to exit...")
        except EOFError:
            pass  # مدیریت خطای EOFError

if __name__ == "__main__":
    main()