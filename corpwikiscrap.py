"""
CorporationWiki Automated Bulk Scraper - UNLIMITED PAGES
Now with MinIO upload functionality
"""

import asyncio
import random
import time
import csv
import os
from playwright.async_api import async_playwright
from urllib.parse import urljoin
import logging
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.getcwd(), 'corporationwiki_output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

load_dotenv()

CREDENTIALS = {
    'email': os.getenv('CORPORATIONWIKI_EMAIL'),
    'password': os.getenv('CORPORATIONWIKI_PASSWORD')
}

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 's3.us-east-005.oriobjects.cloud',
    'access_key': '005775aede18e2e0000000023',
    'secret_key': 'K005GD3X7YxPdbUEtP9mfYwatqf/ugg',
    'bucket_name': 'holacracydata',
    'folder_path': 'corporation_wiki',  # Folder inside bucket
    'region': 'us-east-1',  # Set region explicitly to avoid permission check
    'secure': True  # Use HTTPS
}


class MinIOUploader:
    """Simple MinIO uploader - no permission checks, just upload"""
    
    def __init__(self, config):
        self.config = config
        self.client = None
        
    def connect(self):
        """Connect to MinIO"""
        try:
            # Set region explicitly to avoid region lookup which requires extra permissions
            self.client = Minio(
                self.config['endpoint'],
                access_key=self.config['access_key'],
                secret_key=self.config['secret_key'],
                secure=self.config['secure'],
                region=self.config.get('region', 'us-east-1')  # Default region
            )
            logger.info("✅ Connected to MinIO")
            return True
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
            return False
    
    def ensure_bucket(self):
        """Skip bucket checks - just return True"""
        logger.info(f"ℹ️  Skipping bucket verification (will test on first upload)")
        return True
    
    def upload_file(self, local_path, remote_name=None):
        """Upload file to MinIO - direct upload, no checks"""
        try:
            if not remote_name:
                remote_name = os.path.basename(local_path)
            
            # Add folder path
            folder_path = self.config.get('folder_path', '')
            if folder_path:
                remote_name = f"{folder_path}/{remote_name}"
            
            bucket_name = self.config['bucket_name']
            
            logger.info(f"📤 Uploading {remote_name} to {bucket_name}...")
            
            # Direct upload attempt
            self.client.fput_object(
                bucket_name,
                remote_name,
                local_path
            )
            
            logger.info(f"✅ Uploaded successfully: {bucket_name}/{remote_name}")
            return True
            
        except S3Error as e:
            error_code = getattr(e, 'code', '')
            error_msg = str(e)
            
            logger.error(f"❌ Upload failed")
            logger.error(f"   Code: {error_code}")
            logger.error(f"   Message: {error_msg}")
            logger.error(f"   Bucket: {bucket_name}")
            logger.error(f"   Object: {remote_name}")
            
            # Provide helpful suggestions
            if 'AccessDenied' in error_msg or 'not entitled' in error_msg:
                logger.error(f"")
                logger.error(f"   💡 TROUBLESHOOTING:")
                logger.error(f"   1. Verify credentials have WRITE permission")
                logger.error(f"   2. Run: mc ls mobius/{bucket_name}/{folder_path}/")
                logger.error(f"   3. Try: mc cp test.csv mobius/{bucket_name}/{folder_path}/test.csv")
                logger.error(f"   4. Check if bucket name is correct")
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Unexpected upload error: {e}")
            return False


class CorporationWikiScraper:
    def __init__(self, credentials, minio_uploader=None):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self.credentials = credentials
        self.minio_uploader = minio_uploader
        self.all_results = []
        self.current_page = 1
        self.total_pages = 0
        self.is_logged_in = False
    
    async def setup(self):
        """Setup browser"""
        logger.info("🚀 Starting browser...")
        
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--window-size=1920,1080'
            ]
        )
        
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        )
        
        self.page = await self.context.new_page()
        
        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)
        
        logger.info("✅ Browser ready")
    
    async def search(self, search_term):
        """Search for a term"""
        logger.info(f"🔍 Searching for: {search_term}")
        
        try:
            await self.page.goto('https://www.corporationwiki.com/', wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(2)
            
            encoded_term = search_term.replace(' ', '+')
            search_url = f"https://www.corporationwiki.com/search/results?term={encoded_term}"
            
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)
            
            await self.page.wait_for_selector('.list-group-item', timeout=10000)
            logger.info("✅ Search results loaded")
            
            return True
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return False
    
    async def handle_auth_popup(self):
        """Handle authentication popup"""
        try:
            logger.info("🔐 Checking for auth popup...")
            await asyncio.sleep(2)
            
            modal_visible = False
            modal_selectors = ['.modal.show', '.modal[style*="display: block"]', '.modal-dialog', '[role="dialog"]']
            
            for selector in modal_selectors:
                try:
                    modal = await self.page.wait_for_selector(selector, timeout=2000)
                    if modal and await modal.is_visible():
                        modal_visible = True
                        logger.info(f"✅ Found modal")
                        break
                except:
                    continue
            
            if not modal_visible:
                logger.info("ℹ️  No auth popup")
                return False
            
            content = await self.page.content()
            
            if 'confirm password' in content.lower() or 'register for a free account' in content.lower():
                logger.info("📝 REGISTER modal detected")
                
                signin_texts = ["Already registered? Sign in here", "Sign in here", "Already registered"]
                
                clicked = False
                for text in signin_texts:
                    try:
                        link = await self.page.wait_for_selector(f'text="{text}"', timeout=3000)
                        if link and await link.is_visible():
                            await link.click()
                            clicked = True
                            logger.info(f"✅ Clicked 'Sign in here'")
                            break
                    except:
                        continue
                
                if not clicked:
                    try:
                        await self.page.click('a:has-text("sign in")', timeout=2000)
                        clicked = True
                    except:
                        pass
                
                if not clicked:
                    logger.error("❌ Could not find sign-in link")
                    return False
                
                await asyncio.sleep(2)
            else:
                logger.info("📝 Already on SIGN IN modal")
            
            return await self.fill_and_submit_login()
            
        except Exception as e:
            logger.error(f"❌ Auth error: {e}")
            return False
    
    async def fill_and_submit_login(self):
        """Fill and submit login form"""
        try:
            logger.info("🔑 Filling login form...")
            await asyncio.sleep(2)
            
            # Email
            email_filled = False
            email_selectors = ['input[name="Email"]', 'input[placeholder*="Email" i]', 'input[id*="Email"]']
            
            for selector in email_selectors:
                try:
                    email_field = await self.page.wait_for_selector(selector, timeout=3000)
                    if email_field and await email_field.is_visible():
                        await email_field.click()
                        await asyncio.sleep(0.3)
                        await email_field.fill('')
                        await asyncio.sleep(0.2)
                        await email_field.type(self.credentials['email'], delay=100)
                        email_filled = True
                        logger.info(f"✅ Email filled")
                        break
                except:
                    continue
            
            if not email_filled:
                logger.error("❌ Could not fill email")
                return False
            
            await asyncio.sleep(0.5)
            
            # Password
            password_filled = False
            password_selectors = ['.modal input[type="password"]', 'input[type="password"]', 'input[name="password"]']
            
            for selector in password_selectors:
                try:
                    password_field = await self.page.wait_for_selector(selector, timeout=3000)
                    if password_field and await password_field.is_visible():
                        await password_field.click()
                        await asyncio.sleep(0.3)
                        await password_field.fill('')
                        await asyncio.sleep(0.2)
                        await password_field.type(self.credentials['password'], delay=100)
                        password_filled = True
                        logger.info(f"✅ Password filled")
                        break
                except:
                    continue
            
            if not password_filled:
                logger.error("❌ Could not fill password")
                return False
            
            await asyncio.sleep(1)
            
            # Submit
            submit_selectors = ['button[type="submit"]', 'button:has-text("Sign in")', 'input[type="submit"]']
            
            submit_success = False
            for selector in submit_selectors:
                try:
                    submit_btn = await self.page.wait_for_selector(selector, timeout=3000)
                    if submit_btn and await submit_btn.is_visible():
                        await submit_btn.click()
                        submit_success = True
                        logger.info(f"✅ Form submitted")
                        break
                except:
                    continue
            
            if not submit_success:
                logger.error("❌ Could not submit")
                return False
            
            logger.info("⏳ Waiting for login...")
            await asyncio.sleep(5)
            
            self.is_logged_in = True
            logger.info("✅ Login completed")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Login error: {e}")
            return False
    
    async def click_next_page(self):
        """Click next arrow - keeps going until disabled (goes beyond page 11!)"""
        try:
            logger.info(f"➡️  Going to page {self.current_page + 1}")
            
            # Find the LAST <li> in pagination (contains the next arrow)
            next_arrow_found = False
            
            # Method 1: Get last li element
            try:
                last_li = await self.page.wait_for_selector('#search_pager li:last-child', timeout=3000)
                
                if last_li:
                    # Check if disabled
                    is_disabled = await last_li.evaluate('li => li.classList.contains("disabled")')
                    
                    if is_disabled:
                        logger.info("✅ Last page reached - next arrow is disabled")
                        return False
                    
                    # Click the <a> inside
                    next_link = await last_li.query_selector('a')
                    if next_link:
                        await next_link.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        await next_link.click()
                        next_arrow_found = True
                        logger.info("✅ Clicked next arrow")
            except Exception as e:
                logger.debug(f"Method 1 failed: {e}")
            
            # Method 2: Find chevron-right icon
            if not next_arrow_found:
                try:
                    chevron_li = await self.page.wait_for_selector('li:has(span.glyphicon-chevron-right)', timeout=3000)
                    
                    if chevron_li:
                        is_disabled = await chevron_li.evaluate('li => li.classList.contains("disabled")')
                        
                        if is_disabled:
                            logger.info("✅ Last page reached")
                            return False
                        
                        next_link = await chevron_li.query_selector('a')
                        if next_link:
                            await next_link.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)
                            await next_link.click()
                            next_arrow_found = True
                            logger.info("✅ Clicked next arrow (chevron)")
                except Exception as e:
                    logger.debug(f"Method 2 failed: {e}")
            
            # Method 3: Direct anchor with chevron
            if not next_arrow_found:
                try:
                    next_link = await self.page.wait_for_selector('a:has(span.glyphicon-chevron-right)', timeout=3000)
                    
                    if next_link:
                        is_disabled = await next_link.evaluate('a => a.closest("li") && a.closest("li").classList.contains("disabled")')
                        
                        if is_disabled:
                            logger.info("✅ Last page reached")
                            return False
                        
                        await next_link.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        await next_link.click()
                        next_arrow_found = True
                        logger.info("✅ Clicked next arrow")
                except Exception as e:
                    logger.debug(f"Method 3 failed: {e}")
            
            # Method 4: Text-based arrows
            if not next_arrow_found:
                for arrow_text in ['›', '»']:
                    try:
                        next_link = await self.page.wait_for_selector(f'a:has-text("{arrow_text}")', timeout=2000)
                        
                        if next_link:
                            is_disabled = await next_link.evaluate('a => a.closest("li") && a.closest("li").classList.contains("disabled")')
                            
                            if is_disabled:
                                logger.info("✅ Last page reached")
                                return False
                            
                            await next_link.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)
                            await next_link.click()
                            next_arrow_found = True
                            logger.info(f"✅ Clicked next arrow ({arrow_text})")
                            break
                    except:
                        continue
            
            if not next_arrow_found:
                logger.info("ℹ️  No next arrow - last page")
                return False
            
            # Wait
            await asyncio.sleep(3)
            
            # Check auth
            auth_handled = await self.handle_auth_popup()
            if auth_handled:
                await asyncio.sleep(3)
            
            # Wait for results
            try:
                await self.page.wait_for_selector('.list-group-item', timeout=10000)
                logger.info("✅ Results loaded")
            except:
                logger.warning("⚠️  Results not found quickly")
            
            self.current_page += 1
            logger.info(f"✅ Now on page {self.current_page}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return False
    
    async def scrape_current_page(self):
        """Scrape current page"""
        logger.info(f"📋 Scraping page {self.current_page}")
        
        try:
            await asyncio.sleep(2)
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            results_container = soup.find('div', {'id': 'results-details'})
            if not results_container:
                results_container = soup.find('div', {'id': 'results'})
            
            if not results_container:
                logger.error("❌ No results container")
                return []
            
            result_items = results_container.find_all('div', class_='list-group-item')
            logger.info(f"📊 Found {len(result_items)} results")
            
            page_results = []
            
            for idx, item in enumerate(result_items, 1):
                result_data = self.parse_result(item)
                if result_data:
                    result_data['page'] = self.current_page
                    result_data['result_on_page'] = idx
                    page_results.append(result_data)
            
            return page_results
            
        except Exception as e:
            logger.error(f"❌ Scraping error: {e}")
            return []
    
    def parse_result(self, item):
        """Parse result item"""
        try:
            result = {
                'company_name': '',
                'company_url': '',
                'location': '',
                'officers': [],
                'total_officers': 0
            }
            
            company_link = item.find('a', class_='ellipsis')
            if company_link:
                result['company_name'] = company_link.get_text(strip=True)
                result['company_url'] = urljoin('https://www.corporationwiki.com', company_link.get('href', ''))
            
            if company_link:
                parent_div = company_link.find_parent('div', class_='col-xs-12 col-lg-5')
                if parent_div:
                    full_text = parent_div.get_text(strip=True)
                    if result['company_name'] in full_text:
                        location = full_text.replace(result['company_name'], '').strip()
                        result['location'] = re.sub(r'^,\s*', '', location)
            
            officers_col = item.find('div', class_='col-xs-12 col-lg-7')
            if officers_col:
                officer_links = officers_col.find_all('a')
                
                for officer_link in officer_links:
                    officer_name = officer_link.get_text(strip=True)
                    officer_url = urljoin('https://www.corporationwiki.com', officer_link.get('href', ''))
                    officer_id = officer_link.get('data-entity-id', '')
                    
                    result['officers'].append({
                        'name': officer_name,
                        'url': officer_url,
                        'entity_id': officer_id
                    })
                
                result['total_officers'] = len(result['officers'])
                
                hidden_span = officers_col.find('span', class_='hidden-officers')
                if hidden_span:
                    hidden_text = hidden_span.get_text(strip=True)
                    match = re.search(r'(\d+)\s+others?', hidden_text)
                    if match:
                        result['total_officers'] += int(match.group(1))
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Parse error: {e}")
            return None
    
    async def scrape_all_pages(self):
        """Scrape ALL pages - no limit!"""
        logger.info("🚀 Scraping ALL pages (unlimited)")
        
        self.all_results = []
        self.current_page = 1
        
        # Page 1
        results = await self.scrape_current_page()
        if results:
            self.all_results.extend(results)
            logger.info(f"✅ Page 1: {len(results)} results (Total: {len(self.all_results)})")
        else:
            logger.error("❌ No results on page 1")
            return []
        
        pages_scraped = 1
        
        # Keep clicking next until it's disabled
        while True:
            if not await self.click_next_page():
                logger.info("ℹ️  No more pages")
                break
            
            results = await self.scrape_current_page()
            if not results:
                logger.info("ℹ️  No results, stopping")
                break
            
            self.all_results.extend(results)
            pages_scraped += 1
            logger.info(f"✅ Page {self.current_page}: {len(results)} results (Total: {len(self.all_results)})")
            
            await asyncio.sleep(random.uniform(3, 6))
        
        logger.info(f"✅ COMPLETE: {pages_scraped} pages, {len(self.all_results)} results")
        
        return self.all_results
    
    def save_results(self, company_name):
        """Save to CSV and upload to MinIO"""
        if not self.all_results:
            logger.warning("⚠️  No results")
            return None
        
        clean_name = re.sub(r'[^\w\s-]', '', company_name).strip()
        clean_name = re.sub(r'[-\s]+', '_', clean_name)
        
        csv_filename = f"{clean_name}.csv"
        csv_path = os.path.join(OUTPUT_DIR, csv_filename)
        
        # Save CSV locally
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['page', 'result_on_page', 'company_name', 'location', 
                         'company_url', 'officer_name', 'officer_url', 'officer_id', 'total_officers']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in self.all_results:
                if result['officers']:
                    for officer in result['officers']:
                        writer.writerow({
                            'page': result['page'],
                            'result_on_page': result['result_on_page'],
                            'company_name': result['company_name'],
                            'location': result['location'],
                            'company_url': result['company_url'],
                            'officer_name': officer['name'],
                            'officer_url': officer['url'],
                            'officer_id': officer['entity_id'],
                            'total_officers': result['total_officers']
                        })
                else:
                    writer.writerow({
                        'page': result['page'],
                        'result_on_page': result['result_on_page'],
                        'company_name': result['company_name'],
                        'location': result['location'],
                        'company_url': result['company_url'],
                        'officer_name': '',
                        'officer_url': '',
                        'officer_id': '',
                        'total_officers': result['total_officers']
                    })
        
        logger.info(f"✅ CSV saved: {csv_filename}")
        
        # Upload to MinIO
        if self.minio_uploader:
            upload_success = self.minio_uploader.upload_file(csv_path, csv_filename)
            if upload_success:
                folder_path = MINIO_CONFIG.get('folder_path', '')
                upload_path = f"{folder_path}/{csv_filename}" if folder_path else csv_filename
                logger.info(f"☁️  Uploaded to MinIO: {MINIO_CONFIG['bucket_name']}/{upload_path}")
        
        return csv_path
    
    async def close(self):
        """Cleanup"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("🔒 Browser closed")
        except:
            pass


def read_companies_from_csv(csv_path):
    """Read companies from CSV"""
    companies = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if 'title' in row and row['title']:
                    companies.append(row['title'].strip())
        
        logger.info(f"📊 Loaded {len(companies)} companies")
        return companies
        
    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")
        return []


async def scrape_company(company_name, company_index, total_companies, minio_uploader):
    """Scrape a single company"""
    print("\n" + "="*80)
    print(f"Company {company_index}/{total_companies}: {company_name}")
    print("="*80 + "\n")
    
    scraper = CorporationWikiScraper(CREDENTIALS, minio_uploader)
    
    try:
        await scraper.setup()
        
        if not await scraper.search(company_name):
            logger.error(f"❌ Search failed")
            return False
        
        results = await scraper.scrape_all_pages()
        
        if results:
            csv_path = scraper.save_results(company_name)
            
            total_officers = sum(len(r.get('officers', [])) for r in results)
            
            print(f"\n✅ SUCCESS!")
            print(f"   Pages: {scraper.current_page}")
            print(f"   Companies: {len(results)}")
            print(f"   Officers: {total_officers}")
            print(f"   File: {os.path.basename(csv_path)}")
            folder_path = MINIO_CONFIG.get('folder_path', '')
            upload_path = f"{folder_path}/{os.path.basename(csv_path)}" if folder_path else os.path.basename(csv_path)
            print(f"   MinIO: {MINIO_CONFIG['bucket_name']}/{upload_path}\n")
            
            return True
        else:
            logger.warning(f"⚠️  No results")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False
    finally:
        await scraper.close()
        await asyncio.sleep(3)


async def main():
    """Main"""
    print("\n" + "="*80)
    print("CorporationWiki Bulk Scraper - UNLIMITED PAGES + MinIO Upload")
    print("="*80)
    
    # Setup MinIO
    print("\n☁️  Connecting to MinIO...")
    minio_uploader = MinIOUploader(MINIO_CONFIG)
    
    if not minio_uploader.connect():
        print("❌ MinIO connection failed. Continue anyway? (y/n): ", end='')
        if input().strip().lower() != 'y':
            return
        minio_uploader = None
    else:
        if minio_uploader.ensure_bucket():
            folder_path = MINIO_CONFIG.get('folder_path', '')
            bucket_display = f"{MINIO_CONFIG['bucket_name']}/{folder_path}" if folder_path else MINIO_CONFIG['bucket_name']
            print(f"✅ MinIO ready - Bucket: {bucket_display}\n")
        else:
            print("⚠️  Bucket setup failed. Continue anyway? (y/n): ", end='')
            if input().strip().lower() != 'y':
                return
    
    input_csv = input("\nEnter CSV file path: ").strip()
    
    if not os.path.exists(input_csv):
        print(f"❌ File not found: {input_csv}")
        return
    
    companies = read_companies_from_csv(input_csv)
    
    if not companies:
        print("❌ No companies found")
        return
    
    print(f"\n📋 Found {len(companies)} companies")
    print(f"📁 Local output: {OUTPUT_DIR}")
    folder_path = MINIO_CONFIG.get('folder_path', '')
    bucket_display = f"{MINIO_CONFIG['bucket_name']}/{folder_path}" if folder_path else MINIO_CONFIG['bucket_name']
    print(f"☁️  MinIO bucket: {bucket_display}\n")
    
    confirm = input("Start? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled")
        return
    
    print("\n🚀 Starting...\n")
    
    successful = 0
    failed = 0
    start_time = time.time()
    
    for index, company_name in enumerate(companies, 1):
        success = await scrape_company(company_name, index, len(companies), minio_uploader)
        
        if success:
            successful += 1
        else:
            failed += 1
        
        remaining = len(companies) - index
        elapsed = time.time() - start_time
        avg_time = elapsed / index
        est_remaining = avg_time * remaining
        
        print(f"📊 Progress: {index}/{len(companies)} | ✅ {successful} | ❌ {failed} | ⏱️  ~{est_remaining/60:.1f}min left\n")
    
    total_time = time.time() - start_time
    
    print("\n" + "="*80)
    print("🎉 COMPLETE!")
    print("="*80)
    print(f"\nTotal: {len(companies)}")
    print(f"Success: {successful}")
    print(f"Failed: {failed}")
    print(f"Time: {total_time/60:.1f} min")
    print(f"\n📁 Local files: {OUTPUT_DIR}")
    folder_path = MINIO_CONFIG.get('folder_path', '')
    bucket_display = f"{MINIO_CONFIG['bucket_name']}/{folder_path}" if folder_path else MINIO_CONFIG['bucket_name']
    print(f"☁️  MinIO bucket: {bucket_display}\n")


if __name__ == "__main__":
    asyncio.run(main())