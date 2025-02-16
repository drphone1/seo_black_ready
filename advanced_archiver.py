"""
Parallel Web Archiver - Ultimate Version (Optimized for Speed & Stability)
"""

import os
import asyncio
import logging
import pandas as pd
from urllib.parse import urlparse
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TaskID, BarColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.console import Console
from typing import List, Dict

# ---------------------- Configuration ----------------------
INPUT_EXCEL = r"E:\1-python\SEO-BLACKHOLE\2-project\0-scrap-website\good_output\content_results.xlsx"
OUTPUT_DIR = r"E:\1-python\SEO-BLACKHOLE\2-project\0-scrap-website\OUTPUT"
SINGLE_FILE_PATH = r"E:\1-python\SEO-BLACKHOLE\2-project\0-scrap-website\node_modules\.bin\single-file.cmd"
MAX_CONCURRENT_TASKS = 5  # کاهش تعداد برای پایداری بیشتر
REQUEST_TIMEOUT = 180  # کاهش به 3 دقیقه
DELAY_BETWEEN_REQUESTS = 1  # کاهش تاخیر به 1 ثانیه
# -----------------------------------------------------------

# تنظیمات لاگینگ
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(show_time=True, show_path=False, markup=False)]
)
logger = logging.getLogger("rich")
console = Console()

# ---------------------- Core Functions ----------------------
async def test_single_file():
    """تست اولیه single-file"""
    try:
        test_command = f'"{SINGLE_FILE_PATH}" --version'
        proc = await asyncio.create_subprocess_shell(
            test_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise Exception("single-file در دسترس نیست")
        return True
    except Exception as e:
        console.print(f"[bold red]خطا در تست single-file: {str(e)}[/bold red]")
        return False

async def download_url(url: str, output_file: str, progress: Progress, worker_id: int) -> bool:
    """دانلود یک URL مشخص"""
    command = (
        f'"{SINGLE_FILE_PATH}" '
        f'"{url}" '
        f'"{output_file}" '
        f'--browser-headless '
        f'--browser-wait-until load'  # برگشت به حالت ساده‌تر قبلی
    )
    
    progress.console.print(f"[Worker {worker_id}] [yellow]⏳ شروع دانلود: {url}[/yellow]")
    success = False
    
    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=REQUEST_TIMEOUT)
        
        # بررسی دقیق‌تر نتیجه
        if proc.returncode == 0 and os.path.exists(output_file):
            size = os.path.getsize(output_file)
            if size > 0:
                success = True
                progress.console.print(
                    f"[Worker {worker_id}] [green]✓ دانلود موفق ({size/1024:.1f} KB): {url}[/green]"
                )
                # بررسی محتوای فایل
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if len(content) > 0 and '<html' in content.lower():
                            progress.console.print(
                                f"[Worker {worker_id}] [blue]⚡ فایل HTML معتبر است[/blue]"
                            )
                        else:
                            success = False
                            progress.console.print(
                                f"[Worker {worker_id}] [red]× محتوای HTML نامعتبر است[/red]"
                            )
                except Exception as e:
                    success = False
                    progress.console.print(
                        f"[Worker {worker_id}] [red]× خطا در خواندن فایل: {str(e)}[/red]"
                    )
                
        if not success:
            if stderr:
                error_msg = stderr.decode()
            elif stdout:
                error_msg = stdout.decode()
            else:
                error_msg = "خطای نامشخص"
            progress.console.print(f"[Worker {worker_id}] [red]× خطا: {error_msg}[/red]")
            
        return success
        
    except asyncio.TimeoutError:
        progress.console.print(f"[Worker {worker_id}] [red]× تایم‌اوت: {url}[/red]")
        return False
        
    except Exception as e:
        progress.console.print(f"[Worker {worker_id}] [red]× خطای سیستمی: {str(e)}[/red]")
        return False
        
    finally:
        if not success and os.path.exists(output_file):
            os.remove(output_file)

async def download_worker(queue: asyncio.Queue, progress: Progress, task_id: TaskID, worker_id: int) -> None:
    """کارگر موازی برای دانلود URLها"""
    worker_task = progress.add_task(f"[blue]Worker {worker_id}[/blue]", total=None)
    
    while True:
        try:
            batch = await queue.get()
            if not batch:
                break
                
            url = batch['url']
            progress.update(worker_task, description=f"[blue]Worker {worker_id}:[/blue] {urlparse(url).netloc}")
            
            domain = urlparse(url).netloc.replace('.', '_')
            output_file = os.path.join(OUTPUT_DIR, f"{domain}.html")
            
            success = await download_url(url, output_file, progress, worker_id)
            batch['success'] = success
            
            if success:
                progress.console.print(f"[Worker {worker_id}] ✓ پردازش {url} تمام شد")
            else:
                progress.console.print(f"[Worker {worker_id}] × پردازش {url} با خطا مواجه شد")
            
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            
        except asyncio.CancelledError:
            break
        finally:
            if 'batch' in locals() and batch:
                queue.task_done()
                progress.update(task_id, advance=1)

async def parallel_download(urls: List[str]) -> None:
    """مدیریت دانلود موازی"""
    if not await test_single_file():
        return
        
    queue = asyncio.Queue()
    results = []
    
    MAX_WORKERS = 10  # افزایش به 10 ورکر
    
    # تبدیل URLها به batch
    for url in urls:
        batch = {'url': url, 'success': False}
        results.append(batch)
        await queue.put(batch)
    
    # اضافه کردن سیگنال‌های پایان
    for _ in range(MAX_WORKERS):
        await queue.put(None)
    
    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=True,
        refresh_per_second=10  # افزایش نرخ به‌روزرسانی
    ) as progress:
        total_task = progress.add_task(
            "[bold magenta]⚡ پیشرفت کلی[/bold magenta]",
            total=len(urls)
        )
        
        workers = [
            asyncio.create_task(
                download_worker(queue, progress, total_task, i+1)
            ) 
            for i in range(MAX_WORKERS)
        ]
        
        await asyncio.gather(*workers)
        
        # گزارش نهایی با جزئیات بیشتر
        successful = sum(1 for r in results if r['success'])
        total_size = sum(os.path.getsize(os.path.join(OUTPUT_DIR, f"{urlparse(r['url']).netloc.replace('.', '_')}.html")) 
                        for r in results if r['success'])
        
        progress.console.print(f"\n[bold]📊 گزارش نهایی:[/bold]")
        progress.console.print(f"✓ تعداد فایل‌های دانلود شده: {successful}")
        progress.console.print(f"× تعداد فایل‌های ناموفق: {len(urls) - successful}")
        if successful > 0:
            progress.console.print(f"💾 حجم کل دانلود: {total_size/1024/1024:.2f} MB")
        
        if len(urls) - successful > 0:
            progress.console.print("\n[yellow]❌ لینک‌های ناموفق:[/yellow]")
            for r in results:
                if not r['success']:
                    progress.console.print(f"• {r['url']}")

def archive_pages(urls, output_dir):
    """
    دریافت لیستی از URLها و دانلود کامل صفحات به صورت HTML در پوشه خروجی.
    """
    import requests
    import os
    from datetime import datetime

    for url in urls:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(output_dir, f"archive_{url.split('//')[-1].replace('/', '_')}_{timestamp}.html")
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            if os.path.exists(filename):
                console.print(f"[green]آرشیو انجام شد: [yellow]{url}[/yellow] -> {filename}[/green]")
            else:
                console.print(f"[red]فایل برای [yellow]{url}[/yellow] ذخیره نشد.[/red]")
        except Exception as e:
            console.print(f"[red]Error archiving [yellow]{url}[/yellow]: {str(e)}[/red]")

# ---------------------- Main Execution ----------------------
def main() -> None:
    """اجرای اصلی برنامه"""
    try:
        df = pd.read_excel(INPUT_EXCEL)
        urls = df['url'].dropna().unique().tolist()  # اصلاح متد از dropنا به dropna
        console.print(f"• [cyan]تعداد URLهای شناسایی شده: {len(urls)}[/cyan]")
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        asyncio.run(parallel_download(urls))
        archive_pages(urls, OUTPUT_DIR)
    
    except Exception as e:
        console.print(f"• [bold red]خطای سیستمی: {str(e)}[/bold red]")

if __name__ == "__main__":
    main()