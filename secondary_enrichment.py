#!/usr/bin/env python3
"""
Secondary Enrichment Script for Restaurant Data
Enriches restaurant data by scraping their websites and public sources.
FIXED: Better error handling for website scraping issues.
"""

import csv
import json
import time
import re
from typing import Optional, Dict, List, Any
from urllib.parse import urljoin, urlparse
import logging

import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
REQUEST_TIMEOUT = 10
RATE_LIMIT_DELAY = 2  # seconds between requests
MAX_GALLERY_IMAGES = 10
MIN_IMAGE_SIZE = 200  # minimum width/height to consider


class RestaurantEnricher:
    """Enriches restaurant data from their websites and public sources."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
    
    def enrich_restaurant(self, restaurant: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single restaurant record.
        
        Args:
            restaurant: Dict with keys: google_place_id, name, area, city, website, address
            
        Returns:
            Dict with enrichment data
        """
        google_place_id = restaurant.get('google_place_id', '')
        name = restaurant.get('name', '')
        website = restaurant.get('website', '').strip()
        
        logger.info(f"Processing: {name} ({google_place_id})")
        
        enrichment = {
            'google_place_id': google_place_id,
            'cover_image': None,
            'menu_url': None,
            'menu_pdf_url': None,
            'gallery_images': [],
            'phone': None,
            'opening_hours': None,
        }
        
        # Try website first
        if website:
            try:
                website_data = self._scrape_website(website)
                enrichment.update(website_data)
                logger.info(f"✓ Scraped website for {name}")
            except Exception as e:
                logger.warning(f"Failed to scrape website for {name}: {str(e)[:100]}")
                # Continue with empty enrichment instead of crashing
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        return enrichment
    
    def _scrape_website(self, url: str) -> Dict[str, Any]:
        """Scrape restaurant website for enrichment data."""
        
        try:
            # Normalize URL
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            # Make request with error handling
            try:
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT, 
                    allow_redirects=True,
                    verify=False  # Skip SSL verification for problematic sites
                )
                response.raise_for_status()
            except requests.exceptions.SSLError:
                logger.warning(f"SSL error for {url}, retrying without verification")
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT, 
                    allow_redirects=True,
                    verify=False
                )
            
            # FIXED: Use response.text instead of response.content
            # This handles encoding automatically
            soup = BeautifulSoup(response.text, 'html.parser')
            base_url = response.url
            
            data = {
                'cover_image': self._safe_extract(self._extract_cover_image, soup, base_url),
                'menu_url': self._safe_extract(self._extract_menu_url, soup, base_url),
                'menu_pdf_url': self._safe_extract(self._extract_menu_pdf, soup, base_url),
                'gallery_images': self._safe_extract(self._extract_gallery_images, soup, base_url) or [],
                'phone': self._safe_extract(self._extract_phone, soup),
                'opening_hours': self._safe_extract(self._extract_opening_hours, soup),
            }
            
            return data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)[:100]}")
            # Return empty data instead of crashing
            return {
                'cover_image': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'opening_hours': None,
            }
    
    def _safe_extract(self, func, *args, **kwargs):
        """Safely execute extraction function with error handling."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Extraction failed in {func.__name__}: {str(e)[:50]}")
            return None
    
    def _extract_cover_image(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract cover/hero image from website."""
        
        # Try OG image first
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            return urljoin(base_url, og_image['content'])
        
        # Try Twitter card image
        twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
        if twitter_image and twitter_image.get('content'):
            return urljoin(base_url, twitter_image['content'])
        
        # Look for hero/banner images
        hero_selectors = [
            'header img',
            '.hero img',
            '.banner img',
            '[class*="hero"] img',
            '[class*="banner"] img',
            'section:first-of-type img',
        ]
        
        for selector in hero_selectors:
            img = soup.select_one(selector)
            if img and img.get('src'):
                return urljoin(base_url, img['src'])
        
        return None
    
    def _extract_menu_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find menu page URL."""
        
        # Look for menu links
        menu_keywords = ['menu', 'menus', 'food', 'carte']
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()
            
            if any(keyword in href or keyword in text for keyword in menu_keywords):
                return urljoin(base_url, link['href'])
        
        return None
    
    def _extract_menu_pdf(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find menu PDF URL."""
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if href.lower().endswith('.pdf') and 'menu' in href.lower():
                return urljoin(base_url, href)
        
        return None
    
    def _extract_gallery_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract gallery images."""
        
        images = []
        seen_urls = set()
        
        # Look for gallery sections
        gallery_selectors = [
            '.gallery img',
            '[class*="gallery"] img',
            '[class*="slider"] img',
            '[id*="gallery"] img',
        ]
        
        for selector in gallery_selectors:
            for img in soup.select(selector):
                src = img.get('src') or img.get('data-src')
                if src:
                    full_url = urljoin(base_url, src)
                    if full_url not in seen_urls and len(images) < MAX_GALLERY_IMAGES:
                        images.append(full_url)
                        seen_urls.add(full_url)
        
        return images
    
    def _extract_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract phone number."""
        
        # Look for tel: links
        tel_link = soup.find('a', href=re.compile(r'tel:'))
        if tel_link:
            return tel_link.get('href').replace('tel:', '').strip()
        
        # Look for phone patterns in text
        phone_pattern = r'\+?\d[\d\s\-\(\)]{8,}'
        for text in soup.stripped_strings:
            match = re.search(phone_pattern, text)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_opening_hours(self, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        """Extract opening hours."""
        
        # This is complex and varies by site
        # For now, just return None
        # Can be enhanced later with more sophisticated parsing
        return None


def main():
    """Process CSV file with restaurant data."""
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python secondary_enrichment.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    enricher = RestaurantEnricher()
    
    # Read input
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        restaurants = list(reader)
    
    logger.info(f"Processing {len(restaurants)} restaurants...")
    
    # Enrich each
    enriched_data = []
    for i, restaurant in enumerate(restaurants, 1):
        logger.info(f"[{i}/{len(restaurants)}] Processing...")
        try:
            enrichment = enricher.enrich_restaurant(restaurant)
            enriched_data.append(enrichment)
        except Exception as e:
            logger.error(f"Failed to enrich restaurant: {e}")
            # Add empty enrichment to keep going
            enriched_data.append({
                'google_place_id': restaurant.get('google_place_id', ''),
                'cover_image': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'opening_hours': None,
            })
    
    # Write output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['google_place_id', 'cover_image', 'menu_url', 'menu_pdf_url', 
                     'gallery_images', 'phone', 'opening_hours']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for data in enriched_data:
            row = data.copy()
            row['gallery_images'] = json.dumps(row['gallery_images']) if row['gallery_images'] else None
            row['opening_hours'] = json.dumps(row['opening_hours']) if row['opening_hours'] else None
            writer.writerow(row)
    
    logger.info(f"✓ Wrote {len(enriched_data)} records to {output_file}")


if __name__ == '__main__':
    main()
