"""
Digital Agency PDF Crawler Engine
Core crawler logic moved from run.py
"""

import os
import json
import time
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
from typing import Set, List

logger = logging.getLogger(__name__)

class CrawlerEngine:
    """Core logic for the Digital Agency PDF Crawler."""
    
    def __init__(self, 
                 entry_url: str = "https://www.digital.go.jp/councils",
                 output_base_dir: Path = Path("data/raw/crawler_downloads"),
                 state_file: Path = Path("data/crawler_state.json"),
                 max_pages: int = 200,
                 request_timeout: int = 10,
                 sleep_interval: float = 1.0):
        
        self.entry_url = entry_url
        self.output_base_dir = output_base_dir
        self.state_file = state_file
        self.max_pages = max_pages
        self.timeout = request_timeout
        self.sleep = sleep_interval
        
        self.seen_urls: Set[str] = set()
        self.new_pdfs_count = 0
        self.found_links_count = 0
        
        # Ensure directories exist
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        if self.state_file.parent:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
        self.load_state()

    def _get_daily_output_dir(self) -> Path:
        """Generate output directory path: data/raw/crawler_downloads/master_raw"""
        # Changed from daily dated folder to single master folder to avoid duplication
        path = self.output_base_dir / "master_raw"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def load_state(self):
        """Load seen URLs from state file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.seen_urls = set(data.get("seen_urls", []))
                logger.info(f"Loaded {len(self.seen_urls)} seen URLs from {self.state_file}")
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                self.seen_urls = set()
        else:
            logger.info("No existing state file. Starting fresh.")

    def save_state(self):
        """Save seen URLs to state file"""
        try:
            data = {"seen_urls": list(self.seen_urls)}
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_soup(self, url: str) -> BeautifulSoup:
        """Fetch URL and return BeautifulSoup object"""
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')

    def is_target_domain(self, url: str) -> bool:
        """Check if URL belongs to digital.go.jp"""
        return "digital.go.jp" in urlparse(url).netloc

    def get_detail_pages(self, soup: BeautifulSoup) -> List[str]:
        """Extract detail page URLs from the entry page"""
        detail_urls = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(self.entry_url, href)
            
            # Simple filter for council pages: contains '/councils/' and is not a file
            if (self.is_target_domain(full_url) and 
                '/councils/' in full_url and 
                not full_url.lower().endswith('.pdf')):
                
                # Exclude the entry page itself if matched
                if full_url != self.entry_url:
                    detail_urls.add(full_url)
        
        return list(detail_urls)

    def extract_pdf_links(self, soup: BeautifulSoup, page_url: str) -> List[str]:
        """Extract PDF URLs from a page"""
        pdf_urls = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Basic PDF detection
            if href.lower().endswith('.pdf'):
                full_url = urljoin(page_url, href)
                if self.is_target_domain(full_url):
                    pdf_urls.add(full_url)
        return list(pdf_urls)

    def download_pdf(self, url: str, output_dir: Path):
        """Download a single PDF"""
        try:
            # Generate unique filename
            parsed = urlparse(url)
            original_name = os.path.basename(parsed.path)
            if not original_name:
                original_name = "unknown.pdf"
                
            # Create prefix hash
            url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()[:12]
            filename = f"{url_hash}_{original_name}"
            
            save_path = output_dir / filename
            
            if save_path.exists():
                logger.info(f"[SKIP] Already exists: {filename}")
                self.seen_urls.add(url)
                return

            logger.info(f"[DL] Downloading: {url} -> {filename}")
            
            # Stream download
            with requests.get(url, stream=True, timeout=self.timeout) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # Update state on success
            self.seen_urls.add(url)
            self.new_pdfs_count += 1
            
        except Exception as e:
            logger.error(f"[ERR] Failed to download {url}: {e}")

    def get_meeting_pages(self, soup: BeautifulSoup, council_url: str) -> List[str]:
        """Extract meeting page URLs (sub-pages) from a council page"""
        meeting_urls = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(council_url, href)
            
            # Meeting page criteria:
            # 1. Be on same domain
            # 2. Start with council_url (child path)
            # 3. Not be the council url itself
            # 4. Not be a PDF
            if (self.is_target_domain(full_url) and 
                full_url.startswith(council_url) and 
                full_url != council_url and
                not full_url.lower().endswith('.pdf')):
                meeting_urls.add(full_url)
        return list(meeting_urls)

    def get_pagination_next(self, soup: BeautifulSoup, current_url: str) -> str:
        """Find the 'Next' page URL from pagination."""
        # 1. <link rel="next"> (Head)
        link_next = soup.find('link', rel='next')
        if link_next and link_next.get('href'):
            return urljoin(current_url, link_next.get('href'))
            
        # 2. Drupal Pager (.pager__item--next > a)
        next_item = soup.find('li', class_='pager__item--next')
        if next_item:
            a = next_item.find('a', href=True)
            if a:
                return urljoin(current_url, a['href'])
        
        # 3. Generic "Next" text or class
        # Look for <a> with class containing "next" or text "次へ"
        for a in soup.find_all('a', href=True):
            # Class check
            classes = a.get('class', [])
            if any('next' in c.lower() for c in classes):
                return urljoin(current_url, a['href'])
            
            # Text check (careful with this)
            text = a.get_text(strip=True)
            if text in ['次へ', 'Next', '>', '次へ >']:
                return urljoin(current_url, a['href'])
                
        return None

    def run(self):
        """Main execution flow"""
        output_dir = self._get_daily_output_dir()
        
        logger.info(f"Starting crawl at: {self.entry_url}")
        logger.info(f"Output directory: {output_dir}")
        
        try:
            # 1. Fetch Entry Page
            soup = self.get_soup(self.entry_url)
            
            # 2. Extract detail pages (Councils)
            detail_pages = self.get_detail_pages(soup)
            logger.info(f"Found {len(detail_pages)} council pages.")
            
            # Limit pages
            crawl_list = detail_pages[:self.max_pages]
            
            # Add entry page itself
            crawl_list.insert(0, self.entry_url)
            
            all_pdfs = set()
            processed_urls = set() # Track visited meeting pages to avoid dupes across councils if any
            
            # 3. Crawl each page
            for i, page_url in enumerate(crawl_list):
                try:
                    logger.info(f"Crawling Council ({i+1}/{len(crawl_list)}): {page_url}")
                    
                    if page_url != self.entry_url:
                        page_soup = self.get_soup(page_url)
                    else:
                        page_soup = soup
                    
                    # 1. PDFs on the council top page (e.g. latest materials)
                    pdfs = self.extract_pdf_links(page_soup, page_url)
                    
                    # 2. Find Meeting Pages (sub-pages)
                    meeting_pages = self.get_meeting_pages(page_soup, page_url)
                    logger.info(f"  > Found {len(meeting_pages)} meeting pages")
                    
                    # 3. Crawl Meeting Pages
                    for mp_url in meeting_pages:
                        if mp_url in processed_urls:
                            continue
                        processed_urls.add(mp_url)
                        
                        try:
                            # Be polite
                            time.sleep(self.sleep)
                            
                            mp_soup = self.get_soup(mp_url)
                            mp_pdfs = self.extract_pdf_links(mp_soup, mp_url)
                            pdfs.extend(mp_pdfs)
                            # logger.info(f"    - {mp_url} : {len(mp_pdfs)} PDFs")
                        except Exception as e:
                            logger.warning(f"[WARN] Error crawling meeting {mp_url}: {e}")
                    
                    # Filter already seen
                    new_found = [u for u in pdfs if u not in self.seen_urls]
                    logger.info(f"  Found {len(pdfs)} PDFs total (New: {len(new_found)})")
                    
                    all_pdfs.update(new_found)
                    
                    # Be polite
                    time.sleep(self.sleep)
                    
                except Exception as e:
                    logger.warning(f"[WARN] Error crawling council {page_url}: {e}")
                    continue

                # --- Pagination Handling ---
                try:
                    current_page_url = page_url
                    current_soup = page_soup
                    page_count = 1
                    
                    while True:
                        next_url = self.get_pagination_next(current_soup, current_page_url)
                        if not next_url or next_url in self.seen_urls:
                            break
                        
                        # Verify domain
                        if not self.is_target_domain(next_url):
                            break
                            
                        logger.info(f"  -> Following pagination (Page {page_count+1}): {next_url}")
                        
                        # Crawl Next Page
                        time.sleep(self.sleep)
                        current_page_url = next_url
                        current_soup = self.get_soup(current_page_url)
                        self.seen_urls.add(current_page_url)
                        
                        # 1. PDFs on this page
                        pdfs_next = self.extract_pdf_links(current_soup, current_page_url)
                        
                        # 2. Meeting Pages on this page
                        meeting_pages_next = self.get_meeting_pages(current_soup, current_page_url)
                        
                        # Process PDFs
                        new_found_next = [u for u in pdfs_next if u not in all_pdfs] # check against current batch set
                        all_pdfs.update(new_found_next)
                        logger.info(f"     Found {len(new_found_next)} new PDFs on page {page_count+1}")
                        
                        # Process Meetings
                        for mp_url in meeting_pages_next:
                            if mp_url in processed_urls:
                                continue
                            processed_urls.add(mp_url)
                            try:
                                time.sleep(self.sleep)
                                mp_soup = self.get_soup(mp_url)
                                mp_pdfs = self.extract_pdf_links(mp_soup, mp_url)
                                all_pdfs.update(mp_pdfs)
                            except Exception as e:
                                logger.warning(f"     [WARN] Error meeting {mp_url}: {e}")
                        
                        page_count += 1
                        if page_count > 10: # Safety limit for pagination depth
                            logger.info(f"  [INFO] Reached pagination limit of 10 pages for {page_url}")
                            break
                            
                except Exception as e:
                    logger.warning(f"[WARN] Error handling pagination for {page_url}: {e}")
            
            # 4. Download new PDFs
            self.found_links_count = len(all_pdfs)
            logger.info(f"Total new PDFs to download: {self.found_links_count}")
            
            for pdf_url in all_pdfs:
                self.download_pdf(pdf_url, output_dir)
                time.sleep(self.sleep)
                
        except Exception as e:
            logger.critical(f"Critical error during execution: {e}")
        finally:
            self.save_state()
            logger.info("="*30)
            logger.info("Execution finished.")
            logger.info(f"Total found links: {self.found_links_count}")
            logger.info(f"Successfully downloaded: {self.new_pdfs_count}")
            logger.info("="*30)
