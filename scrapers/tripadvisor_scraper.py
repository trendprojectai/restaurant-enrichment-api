import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

def search_tripadvisor(name: str, city: str = "London"):
    query = quote(f"{name} {city}")
    url = f"https://www.tripadvisor.co.uk/Search?q={query}"

    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")

    link = soup.select_one("a[href*='/Restaurant_Review']")
    if not link:
        return None

    return "https://www.tripadvisor.co.uk" + link["href"]


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
    hours_block = soup.find(string=re.compile("Hours"))
    if hours_block:
        parent = hours_block.find_parent()
        if parent:
            data["opening_hours"] = [parent.get_text(" ", strip=True)]

    return data
