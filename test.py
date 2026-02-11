"""
CorporationWiki Automated Bulk Scraper - OPTIMIZED FOR SPEED
Reduced delays, smart auth handling, focused on results extraction
"""

import asyncio
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
    'folder_path': 'corporation_wiki',
    'region': 'us-east-1',
    'secure': True
}


class MinIOUploader:
    """Simple MinIO uploader"""
    
    def __init__(self, config):
        self.config = config
        self.client = None
        
    def connect(self):
        """Connect to MinIO"""
        try:
            self.client = Minio(
                self.config['endpoint'],
                access_key=self.config['access_key'],
                secret_key=self.config['secret_key'],
                secure=self.config['secure'],
                region=self.config.get('region', 'us-east-1')
            )
            logger.info("✅ MinIO connected")
            return True
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
            return False
    
    def upload_file(self, local_path, remote_name=None):
        """Upload file to MinIO"""
        try:
            if not remote_name:
                remote_name = os.path.basename(local_path)
            
            folder_path = self.config.get('folder_path', '')
            if folder_path:
                remote_name = f"{folder_path}/{remote_name}"
            
            bucket_name = self.config['bucket_name']
            
            self.client.fput_object(bucket_name, remote_name, local_path)
            logger.info(f"✅ Uploaded: {bucket_name}/{remote_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            return False


class FastCorporationWikiScraper:
    """Optimized scraper with minimal delays"""
    
    def __init__(self, credentials, minio_uploader=None):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self.credentials = credentials
        self.minio_uploader = minio_uploader
        self.all_results = []
        self.current_page = 1
        self.is_logged_in = False
        self.auth_handled = False  # Track if we've handled auth once
    
    async def setup(self):
        """Setup browser - optimized"""
        logger.info("🚀 Starting browser...")
        
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-software-rasterizer',
            ]
        )
        
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        )
        
        self.page = await self.context.new_page()
        
        # Disable images and fonts for faster loading
        await self.page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot}", lambda route: route.abort())
        
        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)
        
        logger.info("✅ Browser ready")
    
    async def search(self, search_term):
        """Search for a term - optimized"""
        logger.info(f"🔍 Searching: {search_term}")
        
        try:
            encoded_term = search_term.replace(' ', '+')
            search_url = f"https://www.corporationwiki.com/search/results?term={encoded_term}"
            
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=20000)
            
            # Wait for results or auth modal (whichever comes first)
            try:
                await self.page.wait_for_selector('.list-group-item, .modal-dialog', timeout=5000)
            except:
                pass
            
            logger.info("✅ Page loaded")
            return True
            
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return False
    
    async def handle_auth_if_needed(self):
        """Handle authentication popup - same logic as working code"""
        
        # If we've already handled auth, check if modal appeared again
        if self.auth_handled:
            try:
                modal = await self.page.query_selector('.modal.show, .modal[style*="display: block"]')
                if not modal:
                    return True  # No modal, continue
            except:
                return True
        
        try:
            logger.info("🔐 Checking for auth popup...")
            await asyncio.sleep(1)
            
            modal_visible = False
            modal_selectors = ['.modal.show', '.modal[style*="display: block"]', '.modal-dialog', '[role="dialog"]']
            
            for selector in modal_selectors:
                try:
                    modal = await self.page.wait_for_selector(selector, timeout=1000)
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
            
            # Check if it's REGISTER modal
            if 'confirm password' in content.lower() or 'register for a free account' in content.lower():
                logger.info("📝 REGISTER modal detected")
                
                signin_texts = ["Already registered? Sign in here"]
                
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
            await asyncio.sleep(1)
            
            # Email
            email_filled = False
            email_selectors = ['input[name="Email"]', 'input[placeholder*="Email" i]', 'input[id*="Email"]']
            
            for selector in email_selectors:
                try:
                    email_field = await self.page.wait_for_selector(selector, timeout=3000)
                    if email_field and await email_field.is_visible():
                        await email_field.click()
                        await asyncio.sleep(0.2)
                        await email_field.fill('')
                        await asyncio.sleep(0.1)
                        await email_field.type(self.credentials['email'], delay=50)
                        email_filled = True
                        logger.info(f"✅ Email filled")
                        break
                except:
                    continue
            
            if not email_filled:
                logger.error("❌ Could not fill email")
                return False
            
            await asyncio.sleep(0.3)
            
            # Password
            password_filled = False
            password_selectors = ['.modal input[type="password"]', 'input[type="password"]', 'input[name="password"]']
            
            for selector in password_selectors:
                try:
                    password_field = await self.page.wait_for_selector(selector, timeout=3000)
                    if password_field and await password_field.is_visible():
                        await password_field.click()
                        await asyncio.sleep(0.2)
                        await password_field.fill('')
                        await asyncio.sleep(0.1)
                        await password_field.type(self.credentials['password'], delay=50)
                        password_filled = True
                        logger.info(f"✅ Password filled")
                        break
                except:
                    continue
            
            if not password_filled:
                logger.error("❌ Could not fill password")
                return False
            
            await asyncio.sleep(0.5)
            
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
            await asyncio.sleep(2)
            
            self.is_logged_in = True
            self.auth_handled = True
            logger.info("✅ Login completed")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Login error: {e}")
            return False
    
    async def click_next_page(self):
        """Fast next page navigation"""
        
        try:
            # Find next arrow - simplified approach
            next_link = await self.page.query_selector('#search_pager li:last-child a')
            
            if not next_link:
                return False
            
            # Check if parent li is disabled
            parent_li = await next_link.evaluate_handle('a => a.closest("li")')
            is_disabled = await parent_li.evaluate('li => li.classList.contains("disabled")')
            
            if is_disabled:
                logger.info("✅ Last page reached")
                return False
            
            # Click next
            await next_link.click()
            
            # Wait for results to load
            await self.page.wait_for_selector('.list-group-item', timeout=10000)
            
            self.current_page += 1
            logger.info(f"➡️  Page {self.current_page}")
            
            return True
            
        except Exception as e:
            logger.debug(f"Next page failed: {e}")
            return False
    
    async def scrape_current_page(self):
        """Fast scraping - parse HTML directly"""
        
        try:
            # Get HTML content
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find results container
            results_container = soup.find('div', {'id': 'results-details'})
            if not results_container:
                return []
            
            # Get all result items
            result_items = results_container.find_all('div', class_='list-group-item')
            
            if not result_items:
                return []
            
            logger.info(f"📋 Page {self.current_page}: {len(result_items)} results")
            
            page_results = []
            
            for idx, item in enumerate(result_items, 1):
                result_data = self.parse_result_fast(item)
                if result_data:
                    result_data['page'] = self.current_page
                    result_data['result_on_page'] = idx
                    page_results.append(result_data)
            
            return page_results
            
        except Exception as e:
            logger.error(f"❌ Scraping error: {e}")
            return []
    
    def parse_result_fast(self, item):
        """Fast result parsing"""
        
        try:
            result = {
                'company_name': '',
                'company_url': '',
                'location': '',
                'officers': [],
                'total_officers': 0
            }
            
            # Company name and URL
            company_link = item.find('a', class_='ellipsis')
            if company_link:
                result['company_name'] = company_link.get_text(strip=True)
                result['company_url'] = urljoin('https://www.corporationwiki.com', company_link.get('href', ''))
            
            # Location
            if company_link:
                parent_div = company_link.find_parent('div', class_='col-xs-12')
                if parent_div:
                    full_text = parent_div.get_text(strip=True)
                    location = full_text.replace(result['company_name'], '').strip()
                    result['location'] = re.sub(r'^,\s*', '', location)
            
            # Officers - get all <a> tags in officers column
            officers_col = item.find('div', class_='col-xs-12 col-lg-7')
            if officers_col:
                # Get all officer links (including hidden ones)
                officer_links = officers_col.find_all('a', attrs={'data-entity-id': True})
                
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
            
            return result
            
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None
    
    async def scrape_all_pages_fast(self):
        """Fast pagination - scrape all pages"""
        
        logger.info("🚀 Fast scraping started")
        
        self.all_results = []
        self.current_page = 1
        
        # Handle auth if needed on first page
        await self.handle_auth_if_needed()
        
        # Scrape first page
        results = await self.scrape_current_page()
        if not results:
            logger.error("❌ No results on first page")
            return []
        
        self.all_results.extend(results)
        logger.info(f"✅ Page 1: {len(results)} results")
        
        # Scrape remaining pages
        page_count = 1
        
        while True:
            # Check auth before each page navigation
            await self.handle_auth_if_needed()
            
            # Try to go to next page
            if not await self.click_next_page():
                break
            
            # Scrape page
            results = await self.scrape_current_page()
            if not results:
                break
            
            self.all_results.extend(results)
            page_count += 1
            
            # Small delay between pages (minimal)
            await asyncio.sleep(0.5)
        
        logger.info(f"✅ COMPLETE: {page_count} pages, {len(self.all_results)} total results")
        
        return self.all_results
    
    def save_results(self, company_name):
        """Save to CSV and upload to MinIO"""
        
        if not self.all_results:
            logger.warning("⚠️  No results to save")
            return None
        
        # Clean filename
        clean_name = re.sub(r'[^\w\s-]', '', company_name).strip()
        clean_name = re.sub(r'[-\s]+', '_', clean_name)
        
        csv_filename = f"{clean_name}.csv"
        csv_path = os.path.join(OUTPUT_DIR, csv_filename)
        
        # Write CSV
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
        
        logger.info(f"💾 Saved: {csv_filename}")
        
        # Upload to MinIO
        if self.minio_uploader:
            self.minio_uploader.upload_file(csv_path, csv_filename)
        
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


async def scrape_company_fast(company_name, company_index, total_companies, minio_uploader):
    """Scrape a single company - fast mode"""
    
    print(f"\n[{company_index}/{total_companies}] {company_name}")
    
    scraper = FastCorporationWikiScraper(CREDENTIALS, minio_uploader)
    
    try:
        await scraper.setup()
        
        if not await scraper.search(company_name):
            logger.error(f"❌ Search failed")
            return False
        
        results = await scraper.scrape_all_pages_fast()
        
        if results:
            scraper.save_results(company_name)
            
            total_officers = sum(len(r.get('officers', [])) for r in results)
            
            print(f"✅ {len(results)} companies, {total_officers} officers, {scraper.current_page} pages")
            
            return True
        else:
            logger.warning(f"⚠️  No results")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False
    finally:
        await scraper.close()


async def main():
    """Main - optimized for speed"""
    
    print("\n" + "="*80)
    print("CorporationWiki Fast Scraper - OPTIMIZED FOR SPEED")
    print("="*80)
    
    # Setup MinIO
    print("\n☁️  Connecting to MinIO...")
    minio_uploader = MinIOUploader(MINIO_CONFIG)
    
    if not minio_uploader.connect():
        print("⚠️  MinIO failed. Continue with local save only? (y/n): ", end='')
        if input().strip().lower() != 'y':
            return
        minio_uploader = None
    else:
        folder_path = MINIO_CONFIG.get('folder_path', '')
        bucket_display = f"{MINIO_CONFIG['bucket_name']}/{folder_path}" if folder_path else MINIO_CONFIG['bucket_name']
        print(f"✅ MinIO ready: {bucket_display}")
    
    input_csv = input("\nEnter CSV file path: ").strip()
    
    if not os.path.exists(input_csv):
        print(f"❌ File not found: {input_csv}")
        return
    
    companies = read_companies_from_csv(input_csv)
    
    if not companies:
        print("❌ No companies found")
        return
    
    print(f"\n📋 Companies to scrape: {len(companies)}")
    print(f"📁 Local output: {OUTPUT_DIR}\n")
    
    confirm = input("Start fast scraping? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled")
        return
    
    print("\n🚀 FAST MODE ACTIVATED\n")
    print("="*80)
    
    successful = 0
    failed = 0
    start_time = time.time()
    
    for index, company_name in enumerate(companies, 1):
        success = await scrape_company_fast(company_name, index, len(companies), minio_uploader)
        
        if success:
            successful += 1
        else:
            failed += 1
        
        # Progress update
        elapsed = time.time() - start_time
        avg_time = elapsed / index
        remaining = len(companies) - index
        est_remaining = avg_time * remaining
        
        if index % 5 == 0 or index == len(companies):
            print(f"\n📊 Progress: {index}/{len(companies)} | ✅ {successful} | ❌ {failed} | "
                  f"⏱️  {elapsed/60:.1f}min elapsed | ~{est_remaining/60:.1f}min remaining")
            print("="*80)
    
    total_time = time.time() - start_time
    
    print("\n" + "="*80)
    print("🎉 SCRAPING COMPLETE!")
    print("="*80)
    print(f"\nTotal companies: {len(companies)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️  Total time: {total_time/60:.1f} minutes")
    print(f"⚡ Average: {total_time/len(companies):.1f} seconds per company")
    print(f"\n📁 Local files: {OUTPUT_DIR}")
    
    if minio_uploader:
        folder_path = MINIO_CONFIG.get('folder_path', '')
        bucket_display = f"{MINIO_CONFIG['bucket_name']}/{folder_path}" if folder_path else MINIO_CONFIG['bucket_name']
        print(f"☁️  MinIO: {bucket_display}")
    
    print()


if __name__ == "__main__":
    asyncio.run(main())