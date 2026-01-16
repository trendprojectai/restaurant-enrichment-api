#!/usr/bin/env python3
"""
Secondary Enrichment Script for Restaurant Data
Enriches restaurant data by scraping their websites and public sources.
NO Google Maps scraping. NO Google Places API.
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
                logger.warning(f"Failed to scrape website for {name}: {e}")
        
        # Fallback: try OpenTable/TheFork if website didn't yield good data
        if not enrichment['menu_url'] and not enrichment['cover_image']:
            area = restaurant.get('area', restaurant.get('city', ''))
            fallback_data = self._try_fallback_sources(name, area)
            
            # Only use fallback data if we didn't get it from website
            if fallback_data:
                if not enrichment['cover_image']:
                    enrichment['cover_image'] = fallback_data.get('cover_image')
                if not enrichment['menu_url']:
                    enrichment['menu_url'] = fallback_data.get('menu_url')
                if not enrichment['gallery_images']:
                    enrichment['gallery_images'] = fallback_data.get('gallery_images', [])
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        return enrichment
    
    def _scrape_website(self, url: str) -> Dict[str, Any]:
        """Scrape restaurant website for enrichment data."""
        
        # Normalize URL
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        response = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        base_url = response.url
        
        data = {
            'cover_image': self._extract_cover_image(soup, base_url),
            'menu_url': self._extract_menu_url(soup, base_url),
            'menu_pdf_url': self._extract_menu_pdf(soup, base_url),
            'gallery_images': self._extract_gallery_images(soup, base_url),
            'phone': self._extract_phone(soup),
            'opening_hours': self._extract_opening_hours(soup),
        }
        
        return data
    
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
        
        menu_keywords = ['menu', 'food', 'drinks', 'brunch', 'dinner', 'lunch', 'carte']
        
        # Look for links containing menu keywords
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            text = link.get_text().lower()
            
            # Check if link text or href contains menu keywords
            if any(keyword in text or keyword in href for keyword in menu_keywords):
                # Avoid external links (social media, etc)
                full_url = urljoin(base_url, link['href'])
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    return full_url
        
        return None
    
    def _extract_menu_pdf(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find menu PDF URL."""
        
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            
            # Check if it's a PDF and likely a menu
            if '.pdf' in href and any(word in href for word in ['menu', 'carte', 'food']):
                return urljoin(base_url, link['href'])
        
        return None
    
    def _extract_gallery_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract gallery images from website."""
        
        images = []
        seen_urls = set()
        
        # Find all images
        for img in soup.find_all('img'):
            src = img.get('src') or img.get('data-src')
            if not src:
                continue
            
            full_url = urljoin(base_url, src)
            
            # Skip if already seen
            if full_url in seen_urls:
                continue
            
            # Skip small images, icons, logos
            if self._is_likely_gallery_image(img, src):
                images.append(full_url)
                seen_urls.add(full_url)
            
            if len(images) >= MAX_GALLERY_IMAGES:
                break
        
        return images
    
    def _is_likely_gallery_image(self, img_tag, src: str) -> bool:
        """Determine if image is likely a gallery/content image vs icon/logo."""
        
        src_lower = src.lower()
        
        # Skip common non-gallery images
        skip_patterns = ['logo', 'icon', 'favicon', 'sprite', 'avatar', 'badge']
        if any(pattern in src_lower for pattern in skip_patterns):
            return False
        
        # Check dimensions if available
        width = img_tag.get('width')
        height = img_tag.get('height')
        
        if width and height:
            try:
                if int(width) < MIN_IMAGE_SIZE or int(height) < MIN_IMAGE_SIZE:
                    return False
            except (ValueError, TypeError):
                pass
        
        # Prefer images in gallery/slider sections
        parent_classes = ' '.join(img_tag.parent.get('class', []))
        if any(word in parent_classes for word in ['gallery', 'slider', 'carousel', 'photo']):
            return True
        
        return True  # Default to including
    
    def _extract_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract phone number from website."""
        
        # Look for tel: links
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('tel:'):
                phone = link['href'].replace('tel:', '').strip()
                return phone
        
        # Look for phone patterns in text
        phone_pattern = r'\+?[\d\s\(\)\-]{10,}'
        for text in soup.stripped_strings:
            if 'phone' in text.lower() or 'tel' in text.lower() or 'call' in text.lower():
                match = re.search(phone_pattern, text)
                if match:
                    return match.group(0).strip()
        
        return None
    
    def _extract_opening_hours(self, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        """Extract opening hours from website."""
        
        hours = {}
        
        # Look for sections containing opening hours
        keywords = ['opening hours', 'hours', 'open', 'horaires']
        
        for element in soup.find_all(['div', 'section', 'table']):
            text = element.get_text().lower()
            
            if any(keyword in text for keyword in keywords):
                # Try to parse day-time patterns
                days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                       'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
                
                for line in element.stripped_strings:
                    line_lower = line.lower()
                    for day in days:
                        if day in line_lower:
                            # Extract time pattern (e.g., "12:00-22:00" or "12:00 - 10:00 PM")
                            time_match = re.search(r'\d{1,2}:\d{2}.*\d{1,2}:\d{2}', line)
                            if time_match:
                                hours[day[:3]] = time_match.group(0)
                
                if hours:
                    return hours
        
        return None
    
    def _try_fallback_sources(self, name: str, area: str) -> Optional[Dict[str, Any]]:
        """Try OpenTable/TheFork as fallback sources."""
        
        logger.info(f"Trying fallback sources for {name} in {area}")
        
        # Try OpenTable
        opentable_data = self._try_opentable(name, area)
        if opentable_data:
            return opentable_data
        
        # Try TheFork
        thefork_data = self._try_thefork(name, area)
        if thefork_data:
            return thefork_data
        
        return None
    
    def _try_opentable(self, name: str, area: str) -> Optional[Dict[str, Any]]:
        """Attempt to find restaurant on OpenTable."""
        
        try:
            # Search OpenTable
            search_url = f"https://www.opentable.com/s/?query={name} {area}"
            response = self.session.get(search_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find first restaurant result
            result = soup.select_one('[data-test="restaurant-card"]')
            if not result:
                return None
            
            # Extract link to restaurant page
            link = result.find('a', href=True)
            if not link:
                return None
            
            restaurant_url = urljoin('https://www.opentable.com', link['href'])
            
            # Fetch restaurant page
            time.sleep(1)  # Be respectful
            page_response = self.session.get(restaurant_url, timeout=REQUEST_TIMEOUT)
            page_soup = BeautifulSoup(page_response.content, 'html.parser')
            
            data = {
                'cover_image': self._extract_cover_image(page_soup, restaurant_url),
                'gallery_images': self._extract_gallery_images(page_soup, restaurant_url)[:5],
                'menu_url': None,  # OpenTable doesn't usually have external menu links
            }
            
            logger.info(f"✓ Found on OpenTable")
            return data
            
        except Exception as e:
            logger.debug(f"OpenTable lookup failed: {e}")
            return None
    
    def _try_thefork(self, name: str, area: str) -> Optional[Dict[str, Any]]:
        """Attempt to find restaurant on TheFork."""
        
        try:
            # TheFork UK search
            search_url = f"https://www.thefork.co.uk/search/?q={name} {area}"
            response = self.session.get(search_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find first result
            result = soup.select_one('[data-test="restaurant-card"]')
            if not result:
                return None
            
            # Extract image
            img = result.find('img')
            cover_image = img['src'] if img else None
            
            data = {
                'cover_image': cover_image,
                'gallery_images': [cover_image] if cover_image else [],
                'menu_url': None,
            }
            
            logger.info(f"✓ Found on TheFork")
            return data
            
        except Exception as e:
            logger.debug(f"TheFork lookup failed: {e}")
            return None


def process_csv(input_file: str, output_file: str):
    """
    Process CSV file and enrich each restaurant.
    
    Args:
        input_file: Path to input CSV
        output_file: Path to output CSV
    """
    enricher = RestaurantEnricher()
    
    # Read input
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        restaurants = list(reader)
    
    logger.info(f"Processing {len(restaurants)} restaurants")
    
    # Enrich each restaurant
    enriched_data = []
    
    for i, restaurant in enumerate(restaurants, 1):
        logger.info(f"Progress: {i}/{len(restaurants)}")
        
        try:
            enrichment = enricher.enrich_restaurant(restaurant)
            enriched_data.append(enrichment)
        except Exception as e:
            logger.error(f"Failed to process {restaurant.get('name', 'Unknown')}: {e}")
            # Add placeholder with just the ID
            enriched_data.append({
                'google_place_id': restaurant.get('google_place_id', ''),
                'cover_image': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'opening_hours': None,
            })
    
    # Write output CSV
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['google_place_id', 'cover_image', 'menu_url', 'menu_pdf_url', 
                     'gallery_images', 'phone', 'opening_hours']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for data in enriched_data:
            # Convert lists and dicts to JSON strings for CSV
            row = data.copy()
            row['gallery_images'] = json.dumps(row['gallery_images']) if row['gallery_images'] else None
            row['opening_hours'] = json.dumps(row['opening_hours']) if row['opening_hours'] else None
            writer.writerow(row)
    
    logger.info(f"✓ Complete! Output written to {output_file}")


def process_json(input_file: str, output_file: str):
    """
    Process JSON file and enrich each restaurant.
    
    Args:
        input_file: Path to input JSON
        output_file: Path to output JSON
    """
    enricher = RestaurantEnricher()
    
    # Read input
    with open(input_file, 'r', encoding='utf-8') as f:
        restaurants = json.load(f)
    
    logger.info(f"Processing {len(restaurants)} restaurants")
    
    # Enrich each restaurant
    enriched_data = []
    
    for i, restaurant in enumerate(restaurants, 1):
        logger.info(f"Progress: {i}/{len(restaurants)}")
        
        try:
            enrichment = enricher.enrich_restaurant(restaurant)
            enriched_data.append(enrichment)
        except Exception as e:
            logger.error(f"Failed to process {restaurant.get('name', 'Unknown')}: {e}")
            enriched_data.append({
                'google_place_id': restaurant.get('google_place_id', ''),
                'cover_image': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'opening_hours': None,
            })
    
    # Write output JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, indent=2)
    
    logger.info(f"✓ Complete! Output written to {output_file}")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python secondary_enrichment.py input.csv output.csv")
        print("   or: python secondary_enrichment.py input.json output.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if input_file.endswith('.csv'):
        process_csv(input_file, output_file)
    elif input_file.endswith('.json'):
        process_json(input_file, output_file)
    else:
        print("Error: Input file must be .csv or .json")
        sys.exit(1)
