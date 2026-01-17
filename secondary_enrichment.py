#!/usr/bin/env python3
"""
Enhanced Secondary Enrichment Script for Restaurant Data
Enriches restaurant data with deep web scraping and social media integration.
Features: Multiple extraction strategies, social media handles, cuisine & pricing detection.
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
MAX_GALLERY_IMAGES = 15  # Increased from 10
MIN_IMAGE_SIZE = 200  # minimum width/height to consider

# Common icon/logo patterns to exclude
ICON_PATTERNS = [
    'icon', 'logo', 'favicon', 'sprite', 'thumbnail',
    'avatar', 'profile', 'badge', 'button'
]


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
            Dict with enrichment data including new fields
        """
        google_place_id = restaurant.get('google_place_id', '')
        name = restaurant.get('name', '')
        website = restaurant.get('website', '').strip()

        logger.info(f"Processing: {name} ({google_place_id})")

        enrichment = {
            'google_place_id': google_place_id,
            'cover_image': None,
            'cover_image_alt': None,
            'menu_url': None,
            'menu_pdf_url': None,
            'gallery_images': [],
            'phone': None,
            'phone_formatted': None,
            'email': None,
            'instagram_handle': None,
            'instagram_url': None,
            'tiktok_handle': None,
            'tiktok_url': None,
            'tiktok_videos': [],
            'facebook_url': None,
            'opening_hours': None,
            'cuisine_type': None,
            'price_range': None,
        }

        # Try website first
        if website:
            try:
                website_data = self._scrape_website(website, name)
                enrichment.update(website_data)
                logger.info(f"✓ Scraped website for {name}")
            except Exception as e:
                logger.warning(f"Failed to scrape website for {name}: {str(e)[:100]}")
                # Continue with empty enrichment instead of crashing

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

        return enrichment

    def _scrape_website(self, url: str, restaurant_name: str = '') -> Dict[str, Any]:
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

            # Use response.text instead of response.content
            soup = BeautifulSoup(response.text, 'html.parser')
            base_url = response.url
            html_text = response.text

            # Extract cover images (primary and alternate)
            cover_image, cover_image_alt = self._safe_extract(
                self._extract_best_cover_images, soup, base_url
            ) or (None, None)

            # Extract phone and format it
            phone = self._safe_extract(self._extract_phone_multi, soup, html_text)
            phone_formatted = self._safe_extract(self._format_uk_phone, phone) if phone else None

            # Extract social media handles
            instagram_handle = self._safe_extract(self._extract_instagram_handle, soup, html_text)
            instagram_url = f"https://www.instagram.com/{instagram_handle}" if instagram_handle else None

            tiktok_handle = self._safe_extract(self._extract_tiktok_handle, soup, html_text, restaurant_name)
            tiktok_url = f"https://www.tiktok.com/@{tiktok_handle}" if tiktok_handle else None

            data = {
                'cover_image': cover_image,
                'cover_image_alt': cover_image_alt,
                'menu_url': self._safe_extract(self._deep_find_menu, soup, base_url),
                'menu_pdf_url': self._safe_extract(self._extract_menu_pdf_enhanced, soup, base_url),
                'gallery_images': self._safe_extract(self._extract_gallery_images_enhanced, soup, base_url) or [],
                'phone': phone,
                'phone_formatted': phone_formatted,
                'email': self._safe_extract(self._extract_email, soup, html_text),
                'instagram_handle': instagram_handle,
                'instagram_url': instagram_url,
                'tiktok_handle': tiktok_handle,
                'tiktok_url': tiktok_url,
                'tiktok_videos': [],  # Ready for future API integration
                'facebook_url': self._safe_extract(self._extract_facebook_url, soup, html_text),
                'opening_hours': self._safe_extract(self._extract_opening_hours_enhanced, soup),
                'cuisine_type': self._safe_extract(self._extract_cuisine_type, soup, html_text),
                'price_range': self._safe_extract(self._extract_price_range, soup, html_text),
            }

            return data

        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)[:100]}")
            # Return empty data instead of crashing
            return {
                'cover_image': None,
                'cover_image_alt': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'phone_formatted': None,
                'email': None,
                'instagram_handle': None,
                'instagram_url': None,
                'tiktok_handle': None,
                'tiktok_url': None,
                'tiktok_videos': [],
                'facebook_url': None,
                'opening_hours': None,
                'cuisine_type': None,
                'price_range': None,
            }

    def _safe_extract(self, func, *args, **kwargs):
        """Safely execute extraction function with error handling."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Extraction failed in {func.__name__}: {str(e)[:50]}")
            return None

    def _is_icon_or_logo(self, url: str, img_tag: Any = None) -> bool:
        """Check if image URL or tag suggests it's an icon/logo."""
        url_lower = url.lower()

        # Check URL for icon patterns
        if any(pattern in url_lower for pattern in ICON_PATTERNS):
            return True

        # Check image tag attributes
        if img_tag:
            alt = (img_tag.get('alt') or '').lower()
            class_str = ' '.join(img_tag.get('class', [])).lower()

            if any(pattern in alt or pattern in class_str for pattern in ICON_PATTERNS):
                return True

        return False

    def _extract_best_cover_images(self, soup: BeautifulSoup, base_url: str) -> tuple:
        """
        Extract best cover image and alternate cover image.
        Returns: (cover_image, cover_image_alt)
        """
        candidates = []

        # Strategy 1: OG image (highest priority for primary)
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            img_url = urljoin(base_url, og_image['content'])
            if not self._is_icon_or_logo(img_url):
                candidates.append(('og', img_url))

        # Strategy 2: Twitter card image
        twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
        if twitter_image and twitter_image.get('content'):
            img_url = urljoin(base_url, twitter_image['content'])
            if not self._is_icon_or_logo(img_url):
                candidates.append(('twitter', img_url))

        # Strategy 3: Hero/banner images
        hero_selectors = [
            'header img',
            '.hero img',
            '.banner img',
            '[class*="hero"] img',
            '[class*="banner"] img',
            '[class*="cover"] img',
            'section:first-of-type img',
            '.main-image img',
        ]

        for selector in hero_selectors:
            imgs = soup.select(selector)
            for img in imgs[:3]:  # Check first 3 matches
                src = img.get('src') or img.get('data-src')
                if src:
                    img_url = urljoin(base_url, src)
                    if not self._is_icon_or_logo(img_url, img):
                        candidates.append(('hero', img_url))

        # Strategy 4: Large images in main content
        main_imgs = soup.select('main img, article img, .content img')
        for img in main_imgs[:5]:
            src = img.get('src') or img.get('data-src')
            if src:
                img_url = urljoin(base_url, src)
                if not self._is_icon_or_logo(img_url, img):
                    # Check if image appears large (width/height attributes)
                    width = img.get('width')
                    height = img.get('height')
                    if width and height:
                        try:
                            if int(width) >= MIN_IMAGE_SIZE and int(height) >= MIN_IMAGE_SIZE:
                                candidates.append(('content', img_url))
                        except:
                            pass
                    else:
                        candidates.append(('content', img_url))

        # Remove duplicates while preserving order
        seen = set()
        unique_candidates = []
        for source, url in candidates:
            if url not in seen:
                seen.add(url)
                unique_candidates.append(url)

        # Return primary and alternate
        cover_image = unique_candidates[0] if len(unique_candidates) > 0 else None
        cover_image_alt = unique_candidates[1] if len(unique_candidates) > 1 else None

        return cover_image, cover_image_alt

    def _deep_find_menu(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Deep menu URL detection with multiple strategies."""

        menu_keywords = ['menu', 'menus', 'food', 'carte', 'dining', 'eat']

        # Strategy 1: Direct links with menu keywords
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower().strip()

            # Check href and text
            if any(keyword in href for keyword in menu_keywords):
                return urljoin(base_url, link['href'])

            if any(keyword == text for keyword in menu_keywords):
                return urljoin(base_url, link['href'])

        # Strategy 2: Check buttons and their parent links
        buttons = soup.find_all(['button', 'a'], class_=re.compile(r'menu|btn', re.I))
        for btn in buttons:
            text = btn.get_text().lower().strip()
            if any(keyword in text for keyword in menu_keywords):
                if btn.name == 'a' and btn.get('href'):
                    return urljoin(base_url, btn['href'])
                # Check parent link
                parent_link = btn.find_parent('a', href=True)
                if parent_link:
                    return urljoin(base_url, parent_link['href'])

        # Strategy 3: Try common menu URL patterns
        parsed_url = urlparse(base_url)
        common_paths = ['/menu', '/menus', '/food-menu', '/our-menu', '/dining']

        for path in common_paths:
            potential_url = f"{parsed_url.scheme}://{parsed_url.netloc}{path}"
            # Note: We don't validate these URLs to avoid extra requests
            # Return first common pattern found in actual links
            for link in soup.find_all('a', href=True):
                full_href = urljoin(base_url, link['href'])
                if any(path in full_href.lower() for path in common_paths):
                    return full_href

        return None

    def _extract_menu_pdf_enhanced(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Enhanced PDF menu detection."""

        # Strategy 1: Direct PDF links with menu keyword
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()

            if href.endswith('.pdf'):
                # Check if it's menu-related
                if any(keyword in href or keyword in text for keyword in ['menu', 'carte', 'food']):
                    return urljoin(base_url, link['href'])

        # Strategy 2: Any PDF link (fallback)
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if href.lower().endswith('.pdf'):
                return urljoin(base_url, href)

        return None

    def _extract_gallery_images_enhanced(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Enhanced gallery extraction with quality filtering."""

        images = []
        seen_urls = set()

        # Look for gallery sections with expanded selectors
        gallery_selectors = [
            '.gallery img',
            '[class*="gallery"] img',
            '[class*="slider"] img',
            '[class*="carousel"] img',
            '[id*="gallery"] img',
            '[id*="slider"] img',
            '.photos img',
            '[class*="photos"] img',
            '.images img',
            'section[class*="image"] img',
        ]

        for selector in gallery_selectors:
            for img in soup.select(selector):
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if src:
                    full_url = urljoin(base_url, src)

                    # Filter out icons and logos
                    if self._is_icon_or_logo(full_url, img):
                        continue

                    # Avoid duplicates
                    if full_url not in seen_urls and len(images) < MAX_GALLERY_IMAGES:
                        images.append(full_url)
                        seen_urls.add(full_url)

        # If we don't have enough, try general images with size filtering
        if len(images) < 5:
            for img in soup.find_all('img'):
                if len(images) >= MAX_GALLERY_IMAGES:
                    break

                src = img.get('src') or img.get('data-src')
                if src:
                    full_url = urljoin(base_url, src)

                    if self._is_icon_or_logo(full_url, img):
                        continue

                    if full_url not in seen_urls:
                        # Try to filter by size
                        width = img.get('width')
                        height = img.get('height')

                        if width and height:
                            try:
                                if int(width) >= MIN_IMAGE_SIZE and int(height) >= MIN_IMAGE_SIZE:
                                    images.append(full_url)
                                    seen_urls.add(full_url)
                            except:
                                pass

        return images

    def _extract_phone_multi(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Multi-strategy phone extraction."""

        # Strategy 1: tel: links (most reliable)
        tel_link = soup.find('a', href=re.compile(r'tel:'))
        if tel_link:
            phone = tel_link.get('href').replace('tel:', '').strip()
            # Clean up
            phone = re.sub(r'[^\d+\s\-\(\)]', '', phone)
            return phone

        # Strategy 2: Schema.org structured data
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    phone = data.get('telephone') or data.get('phone')
                    if phone:
                        return phone
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            phone = item.get('telephone') or item.get('phone')
                            if phone:
                                return phone
            except:
                pass

        # Strategy 3: Common UK phone patterns
        uk_patterns = [
            r'\+44\s?\d{2,4}\s?\d{3,4}\s?\d{3,4}',  # +44 format
            r'0\d{2,4}\s?\d{3,4}\s?\d{3,4}',  # 0xxx format
            r'\(\d{3,5}\)\s?\d{3,4}\s?\d{3,4}',  # (0xxx) format
        ]

        for pattern in uk_patterns:
            match = re.search(pattern, html_text)
            if match:
                return match.group(0).strip()

        # Strategy 4: General phone pattern
        phone_pattern = r'\+?\d[\d\s\-\(\)]{8,}'
        match = re.search(phone_pattern, html_text)
        if match:
            return match.group(0).strip()

        return None

    def _format_uk_phone(self, phone: str) -> Optional[str]:
        """Format phone number to UK format."""
        if not phone:
            return None

        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', phone)

        # If starts with +44, format as +44 XXXX XXXXXX
        if cleaned.startswith('+44'):
            number = cleaned[3:]
            if len(number) >= 10:
                return f"+44 {number[:4]} {number[4:]}"
            return cleaned

        # If starts with 0, format as 0XXXX XXXXXX
        if cleaned.startswith('0'):
            if len(cleaned) >= 10:
                return f"{cleaned[:5]} {cleaned[5:]}"
            return cleaned

        return phone

    def _extract_email(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract email address."""

        # Strategy 1: mailto: links
        mailto_link = soup.find('a', href=re.compile(r'mailto:'))
        if mailto_link:
            email = mailto_link.get('href').replace('mailto:', '').strip()
            # Remove query parameters
            email = email.split('?')[0]
            return email

        # Strategy 2: Email pattern in text
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(email_pattern, html_text)
        if match:
            email = match.group(0)
            # Filter out common false positives
            if not any(exclude in email.lower() for exclude in ['example.com', 'domain.com', 'email.com']):
                return email

        return None

    def _extract_instagram_handle(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract Instagram handle."""

        # Strategy 1: Instagram links
        instagram_pattern = r'instagram\.com/([A-Za-z0-9._]+)'

        # Check links first
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            match = re.search(instagram_pattern, href)
            if match:
                handle = match.group(1).rstrip('/')
                # Filter out instagram.com itself and common non-handle paths
                if handle not in ['', 'p', 'reel', 'tv', 'stories']:
                    return handle

        # Strategy 2: Look for @handle in text near "instagram"
        text_pattern = r'instagram[:\s]*@([A-Za-z0-9._]+)'
        match = re.search(text_pattern, html_text, re.IGNORECASE)
        if match:
            return match.group(1)

        # Strategy 3: Standalone @handle pattern
        handle_pattern = r'@([A-Za-z0-9._]{3,30})'
        matches = re.findall(handle_pattern, html_text)
        if matches:
            # Return first match that looks like Instagram (avoid email prefixes)
            for handle in matches:
                if '.' not in handle[:3]:  # Simple heuristic
                    return handle

        return None

    def _extract_tiktok_handle(self, soup: BeautifulSoup, html_text: str, restaurant_name: str = '') -> Optional[str]:
        """Extract TikTok handle."""

        # Strategy 1: TikTok links
        tiktok_pattern = r'tiktok\.com/@([A-Za-z0-9._]+)'

        # Check links first
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            match = re.search(tiktok_pattern, href)
            if match:
                handle = match.group(1).rstrip('/')
                return handle

        # Strategy 2: Look for @handle in text near "tiktok"
        context_pattern = r'tiktok[:\s]*@([A-Za-z0-9._]+)'
        match = re.search(context_pattern, html_text, re.IGNORECASE)
        if match:
            return match.group(1)

        # Strategy 3: Check plain text for tiktok URLs
        match = re.search(tiktok_pattern, html_text)
        if match:
            return match.group(1)

        return None

    def _extract_facebook_url(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract Facebook page URL."""

        # Look for Facebook links
        fb_patterns = [
            r'facebook\.com/([A-Za-z0-9.]+)',
            r'fb\.com/([A-Za-z0-9.]+)',
        ]

        # Check links first
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            for pattern in fb_patterns:
                match = re.search(pattern, href)
                if match:
                    page = match.group(1).rstrip('/')
                    # Filter out common non-page paths
                    if page not in ['', 'sharer', 'share', 'pages', 'profile.php']:
                        return f"https://www.facebook.com/{page}"

        # Check plain text
        for pattern in fb_patterns:
            match = re.search(pattern, html_text)
            if match:
                page = match.group(1).rstrip('/')
                if page not in ['', 'sharer', 'share', 'pages']:
                    return f"https://www.facebook.com/{page}"

        return None

    def _extract_opening_hours_enhanced(self, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        """Enhanced opening hours extraction."""

        # Strategy 1: Schema.org structured data
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    hours = data.get('openingHours') or data.get('openingHoursSpecification')
                    if hours:
                        return {'raw': str(hours)}
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            hours = item.get('openingHours') or item.get('openingHoursSpecification')
                            if hours:
                                return {'raw': str(hours)}
            except:
                pass

        # Strategy 2: Look for common opening hours sections
        hours_keywords = ['opening hours', 'opening times', 'hours', 'open', 'timings']

        for keyword in hours_keywords:
            # Find sections with these keywords
            sections = soup.find_all(['div', 'section', 'p'], class_=re.compile(keyword.replace(' ', '|'), re.I))
            for section in sections:
                text = section.get_text().strip()
                if text and len(text) < 500:  # Reasonable length
                    return {'raw': text}

        return None

    def _extract_cuisine_type(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract cuisine type."""

        cuisine_keywords = [
            'italian', 'japanese', 'chinese', 'indian', 'french', 'thai',
            'mexican', 'spanish', 'greek', 'turkish', 'vietnamese', 'korean',
            'american', 'british', 'mediterranean', 'asian', 'european',
            'middle eastern', 'caribbean', 'african', 'fusion', 'contemporary',
            'steakhouse', 'seafood', 'pizza', 'sushi', 'burger', 'bbq', 'vegan'
        ]

        # Strategy 1: Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '').lower()
            for cuisine in cuisine_keywords:
                if cuisine in content:
                    return cuisine.title()

        # Strategy 2: Schema.org
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    cuisine = data.get('servesCuisine')
                    if cuisine:
                        return cuisine if isinstance(cuisine, str) else cuisine[0]
            except:
                pass

        # Strategy 3: Page text analysis
        text_lower = html_text.lower()
        for cuisine in cuisine_keywords:
            # Look for cuisine keywords in context
            patterns = [
                f'{cuisine} cuisine',
                f'{cuisine} restaurant',
                f'{cuisine} food',
                f'serving {cuisine}',
                f'authentic {cuisine}',
            ]
            for pattern in patterns:
                if pattern in text_lower:
                    return cuisine.title()

        return None

    def _extract_price_range(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract price range (£, ££, £££, ££££)."""

        # Strategy 1: Look for pound signs
        pound_patterns = [
            r'(£{1,4})\s*[-/]\s*(£{1,4})',  # £ - ££
            r'(£{2,4})',  # ££, £££, ££££
        ]

        for pattern in pound_patterns:
            match = re.search(pattern, html_text)
            if match:
                # Return the highest pound sign count found
                pounds = match.group(0)
                count = pounds.count('£')
                return '£' * min(count, 4)

        # Strategy 2: Schema.org price range
        schema_scripts = soup.find_all('script', type='application/ld+json')
        for script in schema_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    price_range = data.get('priceRange')
                    if price_range and '£' in price_range:
                        count = price_range.count('£')
                        return '£' * min(count, 4)
            except:
                pass

        # Strategy 3: Keywords (approximate)
        if any(word in html_text.lower() for word in ['fine dining', 'luxury', 'premium']):
            return '££££'
        elif any(word in html_text.lower() for word in ['affordable', 'budget', 'cheap']):
            return '£'

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
                'cover_image_alt': None,
                'menu_url': None,
                'menu_pdf_url': None,
                'gallery_images': [],
                'phone': None,
                'phone_formatted': None,
                'email': None,
                'instagram_handle': None,
                'instagram_url': None,
                'tiktok_handle': None,
                'tiktok_url': None,
                'tiktok_videos': [],
                'facebook_url': None,
                'opening_hours': None,
                'cuisine_type': None,
                'price_range': None,
            })

    # Write output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = [
            'google_place_id', 'cover_image', 'cover_image_alt', 'menu_url', 'menu_pdf_url',
            'gallery_images', 'phone', 'phone_formatted', 'email',
            'instagram_handle', 'instagram_url', 'tiktok_handle', 'tiktok_url', 'tiktok_videos',
            'facebook_url', 'opening_hours', 'cuisine_type', 'price_range'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for data in enriched_data:
            row = data.copy()
            # Convert lists/dicts to JSON
            row['gallery_images'] = json.dumps(row['gallery_images']) if row['gallery_images'] else None
            row['opening_hours'] = json.dumps(row['opening_hours']) if row['opening_hours'] else None
            row['tiktok_videos'] = json.dumps(row['tiktok_videos']) if row['tiktok_videos'] else None
            writer.writerow(row)

    logger.info(f"✓ Wrote {len(enriched_data)} records to {output_file}")


if __name__ == '__main__':
    main()
