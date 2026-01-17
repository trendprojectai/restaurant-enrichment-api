import requests
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}


def missing_critical_fields(data: dict) -> bool:
    """Check if critical fields are missing from scraped data."""
    return any(
        data.get(f) in (None, [], "")
        for f in ["opening_hours", "cuisine_type", "price_range", "phone"]
    )


def extract_tripadvisor_hours(soup):
    """Extract opening hours from TripAdvisor page using multiple selectors."""
    # Common TripAdvisor selectors
    selectors = [
        "[data-testid*='hours']",
        "[class*='hours']",
        "[class*='Hours']"
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(" ", strip=True)
            if any(day in text for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
                return [text]

    return None


def search_tripadvisor(name: str, city: str = "London"):
    query = quote(f"{name} {city}")
    url = f"https://www.tripadvisor.co.uk/Search?q={query}"

    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")

    candidates = soup.select("a[href*='/Restaurant_Review']")

    for link in candidates:
        text = link.get_text(" ", strip=True).lower()
        if name.lower() in text:
            return "https://www.tripadvisor.co.uk" + link["href"]

    return None


def extract_basic_tripadvisor_fields(soup):
    """Extract fields from TripAdvisor page using lightweight HTML scraping."""
    data = {}

    # Price range
    price = soup.find(string=re.compile("£"))
    if price:
        data["price_range"] = price.strip()

    # Cuisine
    cuisine = soup.select_one("a[href*='cuisine']")
    if cuisine:
        data["cuisine_type"] = cuisine.get_text(strip=True)

    # Phone
    phone = soup.find("a", href=re.compile(r"tel:"))
    if phone:
        data["phone"] = phone["href"].replace("tel:", "")

    # Opening hours
    hours = extract_tripadvisor_hours(soup)
    if hours:
        data["opening_hours"] = hours

    return data


def extract_tripadvisor_json(soup):
    """Extract structured data from TripAdvisor JSON-LD scripts."""
    data = {}

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(script.string)
            if obj.get("@type") == "Restaurant":
                # Cuisine
                cuisine = obj.get("servesCuisine")
                if cuisine:
                    data["cuisine_type"] = (
                        cuisine[0] if isinstance(cuisine, list) else cuisine
                    )

                # Price range
                if obj.get("priceRange"):
                    data["price_range"] = obj.get("priceRange")

                # Phone
                if obj.get("telephone"):
                    data["phone"] = obj.get("telephone")

                # Opening hours
                hours = obj.get("openingHoursSpecification")
                if hours:
                    data["opening_hours"] = [
                        f"{h.get('dayOfWeek')}: {h.get('opens')}–{h.get('closes')}"
                        for h in hours
                        if h.get("dayOfWeek")
                    ]
        except Exception:
            continue

    return data


def scrape_tripadvisor_page(url: str):
    """Scrape TripAdvisor page with two-layer approach: HTML first, then JSON-LD fallback."""
    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")

    data = {}

    # Layer 1: Existing lightweight HTML scraping
    data.update(extract_basic_tripadvisor_fields(soup))

    # Layer 2: Structured JSON fallback (only if critical fields are still missing)
    if missing_critical_fields(data):
        json_data = extract_tripadvisor_json(soup)
        # Fill nulls only - don't overwrite existing data
        for key, value in json_data.items():
            if data.get(key) in (None, [], "") and value:
                data[key] = value

    return data
