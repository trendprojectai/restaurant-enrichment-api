#!/usr/bin/env python3
"""
SUPER ENHANCED Secondary Enrichment Script for Restaurant Data
Multi-page intelligent scraper with smart navigation and data merging.

Features:
- Multi-URL navigation (location page + homepage + menu page)
- Intelligent page type detection
- Smart data merging (contact from location, marketing from homepage)
- Comprehensive extraction for all fields
- Robust error handling
"""

import csv
import json
import time
import re
from typing import Optional, Dict, List, Any
from urllib.parse import urljoin, urlparse
import logging

import requests
import cloudscraper  # Cloudflare bypass
from bs4 import BeautifulSoup
from price_parser import Price  # Menu price parsing
from scrapers.tripadvisor_scraper import search_tripadvisor, scrape_tripadvisor_page

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
]
REQUEST_TIMEOUT = 12  # Timeout for requests
RATE_LIMIT_DELAY = 1  # seconds between requests (reduced)
MAX_GALLERY_IMAGES = 10
MIN_IMAGE_SIZE = 200  # minimum width/height to consider
MAX_RETRIES = 2  # Retry failed requests (reduced from 3 to prevent worker timeout)
RETRY_DELAY = 1  # Initial retry delay in seconds (reduced)

# TripAdvisor fallback configuration
TRIPADVISOR_FIELDS = [
    "opening_hours",
    "cuisine_type",
    "price_range",
    "phone"
]


def needs_tripadvisor(enrichment: dict) -> bool:
    """Check if enrichment data is missing key fields that TripAdvisor can fill."""
    return any(
        enrichment.get(field) in (None, [], "")
        for field in TRIPADVISOR_FIELDS
    )


class RestaurantEnricher:
    """SUPER ENHANCED: Multi-page intelligent restaurant data enricher."""

    def __init__(self):
        # Use cloudscraper instead of requests to bypass Cloudflare
        import random
        self.current_user_agent = random.choice(USER_AGENTS)

        # Create cloudscraper session with browser settings
        self.session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )

        # Enhanced headers to avoid anti-bot detection
        self.session.headers.update({
            'User-Agent': self.current_user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        })

    def enrich_restaurant(self, restaurant: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single restaurant record.

        Args:
            restaurant: Dict with keys: google_place_id, name, area, city, website, address

        Returns:
            Dict with comprehensive enrichment data
        """
        google_place_id = restaurant.get('google_place_id', '')
        name = restaurant.get('name', '')
        website = restaurant.get('website', '').strip()

        logger.info(f"Processing: {name} ({google_place_id})")

        # Initialize enrichment with all fields
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

        # Try multi-page scraping
        if website:
            try:
                website_data = self.scrape_restaurant(website)
                enrichment.update(website_data)
                logger.info(f"✓ Enhanced scraping completed for {name}")
            except Exception as e:
                logger.warning(f"Failed to scrape website for {name}: {str(e)[:100]}")

        # TripAdvisor fallback for missing data
        if needs_tripadvisor(enrichment):
            logger.info(f"  → Missing key fields, trying TripAdvisor fallback...")
            try:
                city = restaurant.get('city', 'London')
                ta_url = search_tripadvisor(name, city)

                if ta_url:
                    logger.info(f"  ✓ Found on TripAdvisor: {ta_url}")
                    ta_data = scrape_tripadvisor_page(ta_url)

                    # Fill nulls only - don't overwrite existing data
                    filled_count = 0
                    for key, value in ta_data.items():
                        if enrichment.get(key) in (None, [], "") and value:
                            enrichment[key] = value
                            filled_count += 1

                    if filled_count > 0:
                        logger.info(f"  ✓ Filled {filled_count} fields from TripAdvisor")
                else:
                    logger.info(f"  ⚠ Not found on TripAdvisor")
            except Exception as e:
                logger.warning(f"TripAdvisor fallback failed: {str(e)[:100]}")

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

        return enrichment

    # ============================================================================
    # MULTI-PAGE NAVIGATION STRATEGY
    # ============================================================================

    def scrape_restaurant(self, url: str) -> Dict[str, Any]:
        """
        SUPER ENHANCED: Multi-page intelligent scraper.

        Strategy:
        1. Scrape the provided URL (e.g., location page)
        2. Detect if this is a sub-page (location/branch page)
        3. If sub-page, also scrape the homepage
        4. Try to find menu page if menu not found
        5. Smart merge all data
        """
        all_data = {}

        # Normalize URL
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        logger.info(f"🔍 Starting multi-page scrape for: {url}")

        # Step 1: Scrape the provided URL (primary page - e.g., location page)
        logger.info("  → Scraping primary URL...")
        primary_data = self._scrape_single_page(url)
        all_data.update(primary_data)

        # Step 2: Detect if this is a sub-page (location/branch page)
        is_subpage = self._is_location_or_branch_page(url)

        # Step 3: If sub-page AND primary page had data, also scrape homepage
        # Skip homepage if primary page completely failed (saves time on blocked sites)
        if is_subpage and primary_data:
            logger.info("  ✓ Detected location/branch page - also scraping homepage...")
            homepage_url = self._extract_homepage(url)
            if homepage_url and homepage_url != url:
                homepage_data = self._scrape_single_page(homepage_url)
                # Smart merge: primary data takes precedence for contact, homepage for marketing
                all_data = self._smart_merge(primary_data, homepage_data)
        elif is_subpage and not primary_data:
            logger.info("  ⏭ Skipping homepage (primary page failed - site likely blocking)")

        # Step 4: If menu URL not found, try common menu paths
        if not all_data.get('menu_url'):
            logger.info("  → Menu not found, trying common paths...")
            base_url = self._extract_homepage(url)
            menu_url = self._find_menu_page(base_url)
            if menu_url:
                logger.info(f"  ✓ Found menu page: {menu_url}")
                menu_data = self._scrape_single_page(menu_url)
                # Merge menu data
                for key in ['menu_url', 'menu_pdf_url']:
                    if menu_data.get(key) and not all_data.get(key):
                        all_data[key] = menu_data[key]

        logger.info(f"  ✅ Multi-page scrape complete. Found {len([v for v in all_data.values() if v])} fields")

        return all_data

    def _is_location_or_branch_page(self, url: str) -> bool:
        """Detect if URL is a location/branch page."""
        location_patterns = [
            '/location/', '/locations/',
            '/branch/', '/branches/',
            '/store/', '/stores/',
            '/venue/', '/venues/',
            '/restaurant/', '/restaurants/',
            '/outlet/', '/outlets/',
            '/find-us/', '/visit/',
        ]
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in location_patterns)

    def _extract_homepage(self, url: str) -> str:
        """Extract homepage URL from any URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _find_menu_page(self, base_url: str) -> Optional[str]:
        """Try to find menu page using common URL patterns (DISABLED to prevent timeouts)."""
        # DISABLED: This was causing worker timeouts
        # Menu URLs will be found from page links instead
        logger.debug("  ⏭ Skipping menu path probing (relying on page links)")
        return None

        # # Reduced list to avoid timeouts - only check most common paths
        # menu_paths = [
        #     '/menu', '/menus',
        # ]
        #
        # # Maximum time to spend looking for menu (prevent worker timeout)
        # start_time = time.time()
        # max_search_time = 4  # 4 seconds max
        #
        # for path in menu_paths:
        #     # Stop if we've exceeded max search time
        #     if time.time() - start_time > max_search_time:
        #         logger.debug("  ⏱ Menu search timeout, skipping remaining paths")
        #         break
        #
        #     test_url = base_url.rstrip('/') + path
        #     if self._url_exists(test_url):
        #         return test_url
        #
        # return None

    def _url_exists(self, url: str) -> bool:
        """Check if URL exists (returns 200)."""
        try:
            response = requests.head(
                url,
                timeout=2,  # Reduced from 5 to prevent timeouts
                verify=False,
                allow_redirects=True,
                headers={
                    'User-Agent': USER_AGENT,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                }
            )
            return response.status_code == 200
        except:
            return False

    def _smart_merge(self, primary_data: Dict, secondary_data: Dict) -> Dict:
        """
        Smart data merging strategy.

        Primary data (location page) wins for:
        - phone, email, opening_hours, address (contact info)

        Secondary data (homepage) wins for:
        - cover_image, gallery, menu_url, social media (marketing content)
        """
        merged = {}

        # Contact info from primary (location page) - takes priority
        priority_primary = ['phone', 'phone_formatted', 'email', 'opening_hours']
        for key in priority_primary:
            merged[key] = primary_data.get(key) or secondary_data.get(key)

        # Marketing content from secondary (homepage) - takes priority
        priority_secondary = [
            'cover_image', 'cover_image_alt', 'gallery_images',
            'menu_url', 'menu_pdf_url',
            'instagram_handle', 'instagram_url',
            'tiktok_handle', 'tiktok_url', 'tiktok_videos',
            'facebook_url',
            'cuisine_type', 'price_range'
        ]
        for key in priority_secondary:
            merged[key] = secondary_data.get(key) or primary_data.get(key)

        return merged

    # ============================================================================
    # SINGLE PAGE SCRAPER
    # ============================================================================

    def _scrape_single_page(self, url: str) -> Dict[str, Any]:
        """Scrape a single page and extract all available data with retry logic + Cloudflare bypass."""
        import random

        for attempt in range(MAX_RETRIES):
            try:
                # Rotate user agent on retries
                if attempt > 0:
                    self.current_user_agent = random.choice(USER_AGENTS)
                    logger.info(f"  🔄 Retry attempt {attempt + 1}/{MAX_RETRIES} with new User-Agent")
                    time.sleep(RETRY_DELAY * attempt)  # Exponential backoff

                    # Recreate scraper with new browser signature on retry
                    self.session = cloudscraper.create_scraper(
                        browser={
                            'browser': 'chrome',
                            'platform': 'windows',
                            'desktop': True
                        }
                    )

                # Enhanced headers to avoid anti-bot detection
                headers = {
                    'User-Agent': self.current_user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                }

                # Add small random delay to seem more human (reduced to speed up)
                time.sleep(random.uniform(0.3, 0.8))

                # Cloudscraper automatically handles Cloudflare challenges
                logger.info(f"  🌐 Fetching {url} (cloudscraper - Cloudflare bypass enabled)")
                response = self.session.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    headers=headers
                )

                if response.status_code == 403:
                    logger.warning(f"  ⚠ 403 Forbidden for {url} - attempt {attempt + 1}/{MAX_RETRIES}")
                    if attempt < MAX_RETRIES - 1:
                        continue  # Retry with different user agent
                    logger.error(f"  ✗ Site blocking persists after {MAX_RETRIES} attempts - this site uses heavy anti-bot protection")
                    return {}

                elif response.status_code != 200:
                    logger.warning(f"  ⚠ Got status {response.status_code} for {url}")
                    if attempt < MAX_RETRIES - 1:
                        continue  # Retry
                    return {}

                # Success! Parse the response
                soup = BeautifulSoup(response.text, 'html.parser')
                html_text = response.text
                base_url = response.url

                # Extract all fields with safe wrappers
                data = {
                    'phone': self._safe_extract(self._extract_phone_multi, soup, html_text),
                    'phone_formatted': self._safe_extract(self._extract_phone_formatted, soup, html_text),
                    'email': self._safe_extract(self._extract_email, soup, html_text),
                    'opening_hours': self._safe_extract(self._extract_hours, soup, html_text),
                    'cover_image': self._safe_extract(self._extract_cover_image, soup, base_url),
                    'cover_image_alt': self._safe_extract(self._extract_cover_image_alt, soup),
                    'menu_url': self._safe_extract(self._extract_menu_url, soup, base_url),
                    'menu_pdf_url': self._safe_extract(self._extract_menu_pdf, soup, base_url),
                    'gallery_images': self._safe_extract(self._extract_gallery_images, soup, base_url) or [],
                    'instagram_handle': self._safe_extract(self._extract_instagram_handle, soup, html_text),
                    'instagram_url': self._safe_extract(self._extract_instagram_url, soup, html_text),
                    'tiktok_handle': self._safe_extract(self._extract_tiktok_handle, soup, html_text),
                    'tiktok_url': self._safe_extract(self._extract_tiktok_url, soup, html_text),
                    'tiktok_videos': [],  # Placeholder for future enhancement
                    'facebook_url': self._safe_extract(self._extract_facebook_url, soup, html_text),
                    'cuisine_type': self._safe_extract(self._extract_cuisine_type, soup, html_text),
                    'price_range': self._safe_extract(self._extract_price_range, soup, html_text),
                }

                # Remove None values and return successfully
                logger.info(f"  ✅ Successfully scraped {url}")
                return {k: v for k, v in data.items() if v}

            except cloudscraper.exceptions.CloudflareChallengeError as e:
                logger.warning(f"  ⚠ Cloudflare challenge failed on attempt {attempt + 1}/{MAX_RETRIES}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)  # Wait longer before retry
                    continue
                logger.error(f"  ✗ Could not bypass Cloudflare for {url}")
                return {}

            except requests.exceptions.Timeout:
                logger.warning(f"  ⏱ Timeout on attempt {attempt + 1}/{MAX_RETRIES} for {url}")
                if attempt < MAX_RETRIES - 1:
                    continue
                logger.error(f"  ✗ Timeout after {MAX_RETRIES} attempts")
                return {}

            except requests.exceptions.ConnectionError as e:
                logger.warning(f"  ⚠ Connection error on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)[:50]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                logger.error(f"  ✗ Connection failed after {MAX_RETRIES} attempts")
                return {}

            except Exception as e:
                logger.warning(f"  ⚠ Error on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)[:100]}")
                if attempt < MAX_RETRIES - 1:
                    continue  # Retry
                logger.error(f"  ✗ All retries failed for {url}")
                return {}

    def _safe_extract(self, func, *args, **kwargs):
        """Safely execute extraction function with error handling."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Extraction failed in {func.__name__}: {str(e)[:50]}")
            return None

    # ============================================================================
    # ENHANCED EXTRACTION METHODS
    # ============================================================================

    # -------- PHONE EXTRACTION --------

    def _extract_phone_multi(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Enhanced phone extraction with multiple strategies."""

        # Strategy 1: Try tel: links
        tel_links = soup.find_all('a', href=re.compile(r'tel:'))
        if tel_links:
            phone = tel_links[0]['href'].replace('tel:', '').strip()
            return phone

        # Strategy 2: Try Schema.org
        schema_phone = soup.find(itemprop='telephone')
        if schema_phone:
            return schema_phone.get_text(strip=True)

        # Strategy 3: Regex for UK phone numbers
        uk_pattern = r'(\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3}|(\+44\s?20|\(?020\)?)\s?\d{4}\s?\d{4}|(\+44\s?\d{4}|\(?\d{4}\)?)\s?\d{6}'
        matches = re.search(uk_pattern, html_text)
        if matches:
            phone = matches.group(0).strip()
            return phone

        # Strategy 4: General international phone pattern
        general_pattern = r'\+?\d[\d\s\-\(\)]{8,}'
        matches = re.search(general_pattern, html_text)
        if matches:
            return matches.group(0).strip()

        return None

    def _extract_phone_formatted(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract formatted phone number (same as phone for now)."""
        return self._extract_phone_multi(soup, html_text)

    # -------- EMAIL EXTRACTION --------

    def _extract_email(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract email address."""

        # Strategy 1: mailto: links
        mailto_links = soup.find_all('a', href=re.compile(r'mailto:'))
        if mailto_links:
            email = mailto_links[0]['href'].replace('mailto:', '').strip()
            # Remove query parameters
            email = email.split('?')[0]
            return email

        # Strategy 2: Schema.org
        schema_email = soup.find(itemprop='email')
        if schema_email:
            return schema_email.get_text(strip=True)

        # Strategy 3: Regex pattern for emails
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        matches = re.findall(email_pattern, html_text)
        if matches:
            # Filter out common false positives
            for email in matches:
                if not any(fp in email.lower() for fp in ['example.com', 'sentry.io', 'schema.org']):
                    return email

        return None

    # -------- OPENING HOURS EXTRACTION --------

    def _extract_hours(self, soup: BeautifulSoup, html_text: str) -> Optional[List[str]]:
        """SUPER ENHANCED: Extract opening hours with comprehensive pattern matching."""

        # Strategy 1: Schema.org structured data (JSON-LD)
        try:
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                if script.string:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'openingHoursSpecification' in item:
                                hours = self._parse_opening_hours_spec(item['openingHoursSpecification'])
                                if hours:
                                    return hours
                    elif isinstance(data, dict):
                        if 'openingHoursSpecification' in data:
                            hours = self._parse_opening_hours_spec(data['openingHoursSpecification'])
                            if hours:
                                return hours
                        if 'openingHours' in data:
                            hours_data = data['openingHours']
                            if isinstance(hours_data, list):
                                return hours_data
                            elif isinstance(hours_data, str):
                                return [hours_data]
        except:
            pass

        # Strategy 2: Schema.org itemprop in HTML
        hours_schema = soup.find_all(itemprop='openingHours')
        if hours_schema:
            hours = []
            for h in hours_schema:
                content = h.get('content') or h.get_text(strip=True)
                if content:
                    hours.append(content)
            if hours:
                return hours

        # Strategy 3: Common class/id patterns for hours sections
        hour_selectors = [
            # Class patterns
            '[class*="opening"]', '[class*="hours"]', '[class*="schedule"]',
            '[class*="time"]', '[class*="when-open"]',
            # ID patterns
            '[id*="opening"]', '[id*="hours"]', '[id*="schedule"]',
            # Data attributes
            '[data-hours]', '[data-opening]',
        ]

        for selector in hour_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(strip=True)
                # Check if contains day names
                if any(day in text for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']):
                    # Parse the text for hours
                    parsed_hours = self._parse_hours_from_text(text)
                    if parsed_hours and len(parsed_hours) >= 3:  # At least 3 days
                        return parsed_hours

        # Strategy 4: Look for "Opening Hours" heading + following content
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
        for heading in headings:
            heading_text = heading.get_text(strip=True).lower()
            if any(keyword in heading_text for keyword in ['opening', 'hours', 'opening hours', 'opening times', 'when we', "we're open"]):
                # Get next siblings or parent's text
                parent = heading.find_parent(['div', 'section', 'article'])
                if parent:
                    text = parent.get_text()
                    parsed_hours = self._parse_hours_from_text(text)
                    if parsed_hours and len(parsed_hours) >= 3:
                        return parsed_hours

        # Strategy 5: Regex patterns for day-time combinations in full text
        parsed_hours = self._parse_hours_from_text(html_text)
        if parsed_hours and len(parsed_hours) >= 3:
            return parsed_hours

        return None

    def _parse_opening_hours_spec(self, spec):
        """Parse Schema.org openingHoursSpecification."""
        if not spec:
            return None

        hours = []
        if isinstance(spec, list):
            for item in spec:
                day = item.get('dayOfWeek', '')
                opens = item.get('opens', '')
                closes = item.get('closes', '')
                if day and opens and closes:
                    day_name = day.split('/')[-1] if '/' in day else day
                    hours.append(f"{day_name}: {opens} - {closes}")
        return hours if hours else None

    def _parse_hours_from_text(self, text: str) -> Optional[List[str]]:
        """Parse opening hours from plain text using multiple patterns."""
        hours = []

        # Pattern 1: "Monday: 10:00 AM - 11:00 PM" or "Monday 10:00-23:00"
        pattern1 = r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)[\s:,]+(\d{1,2}(?::\d{2})?\s?(?:am|pm|AM|PM)?)\s?[-–—to]+\s?(\d{1,2}(?::\d{2})?\s?(?:am|pm|AM|PM)?)'
        matches1 = re.findall(pattern1, text, re.IGNORECASE)
        for match in matches1:
            day, open_time, close_time = match
            hours.append(f"{day}: {open_time} - {close_time}")

        # Pattern 2: "Mon-Fri 9am-5pm" or "Monday - Friday: 10:00 - 22:00"
        pattern2 = r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s?[-–to]+\s?(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)[\s:,]+(\d{1,2}(?::\d{2})?\s?(?:am|pm|AM|PM)?)\s?[-–—to]+\s?(\d{1,2}(?::\d{2})?\s?(?:am|pm|AM|PM)?)'
        matches2 = re.findall(pattern2, text, re.IGNORECASE)
        for match in matches2:
            day1, day2, open_time, close_time = match
            hours.append(f"{day1}-{day2}: {open_time} - {close_time}")

        # Pattern 3: "Open Mon-Sun 11am-11pm" or variations
        pattern3 = r'(?:open|hours)[\s:]+([A-Za-z]{3})\s?[-–]\s?([A-Za-z]{3})\s+(\d{1,2}(?::\d{2})?\s?(?:am|pm)?)\s?[-–]\s?(\d{1,2}(?::\d{2})?\s?(?:am|pm)?)'
        matches3 = re.findall(pattern3, text, re.IGNORECASE)
        for match in matches3:
            day1, day2, open_time, close_time = match
            hours.append(f"{day1}-{day2}: {open_time} - {close_time}")

        # Remove duplicates while preserving order
        unique_hours = []
        seen = set()
        for h in hours:
            h_clean = h.lower().replace(' ', '')
            if h_clean not in seen:
                unique_hours.append(h)
                seen.add(h_clean)

        return unique_hours[:7] if unique_hours else None  # Limit to 7 days

    # -------- IMAGE EXTRACTION --------

    def _extract_cover_image(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract cover/hero image from website."""

        # Strategy 1: OG image
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            return urljoin(base_url, og_image['content'])

        # Strategy 2: Twitter card image
        twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
        if twitter_image and twitter_image.get('content'):
            return urljoin(base_url, twitter_image['content'])

        # Strategy 3: Hero/banner images
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
            if img:
                src = img.get('src') or img.get('data-src')
                if src:
                    return urljoin(base_url, src)

        return None

    def _extract_cover_image_alt(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract alt text for cover image."""

        # Try OG image alt
        og_image = soup.find('meta', property='og:image')
        if og_image:
            og_alt = soup.find('meta', property='og:image:alt')
            if og_alt:
                return og_alt.get('content')

        # Try hero image alt
        hero_selectors = [
            'header img',
            '.hero img',
            '.banner img',
            '[class*="hero"] img',
        ]

        for selector in hero_selectors:
            img = soup.select_one(selector)
            if img and img.get('alt'):
                return img['alt']

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
            '[class*="carousel"] img',
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

        return images if images else None

    # -------- MENU EXTRACTION --------

    def _extract_menu_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find menu page URL."""

        menu_keywords = ['menu', 'menus', 'food', 'carte', 'eat']

        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()

            if any(keyword in href or keyword in text for keyword in menu_keywords):
                full_url = urljoin(base_url, link['href'])
                # Avoid social media links
                if not any(sm in full_url for sm in ['facebook.com', 'instagram.com', 'twitter.com']):
                    return full_url

        return None

    def _extract_menu_pdf(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find menu PDF URL."""

        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text().lower()

            if href.lower().endswith('.pdf') and ('menu' in href.lower() or 'menu' in text):
                return urljoin(base_url, href)

        return None

    # -------- SOCIAL MEDIA EXTRACTION --------

    def _extract_instagram_url(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract Instagram URL."""

        # Look for Instagram links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'instagram.com' in href:
                return href

        # Regex fallback
        ig_pattern = r'https?://(?:www\.)?instagram\.com/[A-Za-z0-9._]+'
        match = re.search(ig_pattern, html_text)
        if match:
            return match.group(0)

        return None

    def _extract_instagram_handle(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract Instagram handle."""

        ig_url = self._extract_instagram_url(soup, html_text)
        if ig_url:
            # Extract handle from URL
            match = re.search(r'instagram\.com/([A-Za-z0-9._]+)', ig_url)
            if match:
                return '@' + match.group(1).rstrip('/')

        return None

    def _extract_tiktok_url(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract TikTok URL."""

        # Look for TikTok links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'tiktok.com' in href:
                return href

        # Regex fallback
        tt_pattern = r'https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9._]+'
        match = re.search(tt_pattern, html_text)
        if match:
            return match.group(0)

        return None

    def _extract_tiktok_handle(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract TikTok handle."""

        tt_url = self._extract_tiktok_url(soup, html_text)
        if tt_url:
            # Extract handle from URL
            match = re.search(r'tiktok\.com/@([A-Za-z0-9._]+)', tt_url)
            if match:
                return '@' + match.group(1).rstrip('/')

        return None

    def _extract_facebook_url(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """Extract Facebook URL."""

        # Look for Facebook links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'facebook.com' in href:
                # Clean up URL
                clean_url = href.split('?')[0]  # Remove query params
                return clean_url

        # Regex fallback
        fb_pattern = r'https?://(?:www\.)?facebook\.com/[A-Za-z0-9._-]+'
        match = re.search(fb_pattern, html_text)
        if match:
            return match.group(0).split('?')[0]

        return None

    # -------- CUISINE & PRICE EXTRACTION --------

    def _extract_cuisine_type(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """ENHANCED: Extract cuisine type with multiple strategies."""

        # Strategy 1: Schema.org
        schema_cuisine = soup.find(itemprop='servesCuisine')
        if schema_cuisine:
            return schema_cuisine.get_text(strip=True)

        # Strategy 2: Meta tags
        meta_cuisine = soup.find('meta', attrs={'name': 'cuisine'})
        if meta_cuisine and meta_cuisine.get('content'):
            return meta_cuisine['content']

        # Strategy 3: Common cuisine keywords in HTML
        cuisines_keywords = {
            'Italian': ['italian', 'pasta', 'pizza', 'risotto', 'osso buco', 'carbonara', 'bolognese'],
            'Japanese': ['japanese', 'sushi', 'ramen', 'sashimi', 'tempura', 'teriyaki', 'izakaya'],
            'Chinese': ['chinese', 'dim sum', 'wonton', 'szechuan', 'cantonese', 'peking duck'],
            'Indian': ['indian', 'curry', 'tandoori', 'biryani', 'masala', 'naan', 'tikka'],
            'Thai': ['thai', 'pad thai', 'tom yum', 'green curry', 'massaman'],
            'French': ['french', 'bistro', 'brasserie', 'coq au vin', 'bouillabaisse', 'cassoulet'],
            'Mexican': ['mexican', 'tacos', 'burritos', 'enchiladas', 'quesadilla', 'guacamole'],
            'Spanish': ['spanish', 'tapas', 'paella', 'chorizo', 'jamón', 'sangria'],
            'Greek': ['greek', 'souvlaki', 'moussaka', 'tzatziki', 'gyros', 'feta'],
            'Turkish': ['turkish', 'kebab', 'mezze', 'baklava', 'doner'],
            'Lebanese': ['lebanese', 'hummus', 'falafel', 'shawarma', 'tabbouleh'],
            'Korean': ['korean', 'kimchi', 'bibimbap', 'bulgogi', 'korean bbq'],
            'Vietnamese': ['vietnamese', 'pho', 'banh mi', 'spring rolls'],
            'Mediterranean': ['mediterranean', 'mezze', 'hummus', 'olives'],
            'American': ['american', 'burger', 'steak', 'ribs', 'hot dog'],
            'British': ['british', 'fish and chips', 'sunday roast', 'pie and mash'],
            'Seafood': ['seafood', 'oysters', 'lobster', 'crab', 'mussels'],
        }

        html_lower = html_text.lower()

        # Check keywords with scoring
        scores = {}
        for cuisine, keywords in cuisines_keywords.items():
            score = sum(1 for keyword in keywords if keyword in html_lower)
            if score > 0:
                scores[cuisine] = score

        if scores:
            # Return cuisine with highest score
            best_cuisine = max(scores.items(), key=lambda x: x[1])
            if best_cuisine[1] >= 2:  # At least 2 keywords
                return best_cuisine[0]

        return None

    def _extract_price_range(self, soup: BeautifulSoup, html_text: str) -> Optional[str]:
        """ENHANCED: Extract price range with price-parser."""

        # Strategy 1: Schema.org
        schema_price = soup.find(itemprop='priceRange')
        if schema_price:
            return schema_price.get_text(strip=True)

        # Strategy 2: Parse actual menu prices if present
        try:
            prices = []

            # Find all price patterns
            price_patterns = re.findall(r'£\s?\d+(?:\.\d{2})?', html_text)

            for pattern in price_patterns[:50]:  # Limit to avoid processing too many
                parsed = Price.fromstring(pattern)
                if parsed.amount:
                    try:
                        amount = float(parsed.amount)
                        # Filter reasonable restaurant prices (£3-£150)
                        if 3 <= amount <= 150:
                            prices.append(amount)
                    except:
                        pass

            if len(prices) >= 3:  # Need at least 3 prices for reliable estimate
                avg_price = sum(prices) / len(prices)

                # Determine range based on average
                if avg_price < 12:
                    return '£'
                elif avg_price < 25:
                    return '££'
                elif avg_price < 45:
                    return '£££'
                else:
                    return '££££'
        except Exception as e:
            logger.debug(f"Price parsing failed: {str(e)[:50]}")

        # Strategy 3: Look for £ symbols
        pound_pattern = r'(£{1,4})\b'
        match = re.search(pound_pattern, html_text)
        if match:
            return match.group(1)

        # Strategy 4: Look for "price range" text
        price_pattern = r'(cheap|budget|affordable|moderate|mid-range|expensive|luxury|fine dining|upscale)'
        match = re.search(price_pattern, html_text, re.IGNORECASE)
        if match:
            price_word = match.group(1).lower()
            # Map words to price symbols
            if price_word in ['cheap', 'budget', 'affordable']:
                return '£'
            elif price_word in ['moderate', 'mid-range']:
                return '££'
            elif price_word in ['expensive', 'upscale']:
                return '£££'
            elif price_word in ['luxury', 'fine dining']:
                return '££££'
            return match.group(1).title()

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

    logger.info(f"🚀 SUPER ENHANCED SCRAPER - Processing {len(restaurants)} restaurants...")

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
    fieldnames = [
        'google_place_id', 'cover_image', 'cover_image_alt',
        'menu_url', 'menu_pdf_url', 'gallery_images',
        'phone', 'phone_formatted', 'email',
        'instagram_handle', 'instagram_url',
        'tiktok_handle', 'tiktok_url', 'tiktok_videos',
        'facebook_url', 'opening_hours',
        'cuisine_type', 'price_range'
    ]

    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for data in enriched_data:
            row = data.copy()
            # Convert lists/dicts to JSON
            row['gallery_images'] = json.dumps(row.get('gallery_images', [])) if row.get('gallery_images') else None
            row['opening_hours'] = json.dumps(row.get('opening_hours', [])) if row.get('opening_hours') else None
            row['tiktok_videos'] = json.dumps(row.get('tiktok_videos', [])) if row.get('tiktok_videos') else None
            writer.writerow(row)

    logger.info(f"✅ COMPLETE! Wrote {len(enriched_data)} enriched records to {output_file}")


if __name__ == '__main__':
    main()
