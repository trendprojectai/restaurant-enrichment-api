import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}


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


def scrape_tripadvisor_page(url: str):
    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")

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
