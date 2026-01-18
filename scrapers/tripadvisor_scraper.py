import requests
import re
import json
import math
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from urllib.parse import quote
from typing import Optional, Dict, List, Tuple

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

# Stopwords to remove for name normalization
STOPWORDS = {"the", "restaurant", "kitchen", "bar", "grill", "cafe", "bistro", "brasserie"}

# Constants
MAX_CANDIDATES = 5
MIN_NAME_SIMILARITY = 0.80
MAX_DISTANCE_METERS = 1000
MIN_CONFIDENCE_SCORE = 0.75


def normalize_name(name: str) -> str:
    """
    Normalize restaurant name for comparison.
    - Lowercase
    - Strip punctuation
    - Remove stopwords
    """
    if not name:
        return ""

    # Lowercase
    name = name.lower()

    # Remove punctuation
    name = re.sub(r'[^\w\s]', ' ', name)

    # Split into tokens and remove stopwords
    tokens = [t for t in name.split() if t and t not in STOPWORDS]

    return ' '.join(tokens)


def calculate_name_similarity(name1: str, name2: str) -> float:
    """
    Calculate token-based similarity between two names.
    Returns: 0.0 - 1.0
    """
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    if not norm1 or not norm2:
        return 0.0

    return SequenceMatcher(None, norm1, norm2).ratio()


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two lat/lng points in meters using Haversine formula.
    """
    # Earth radius in meters
    R = 6371000

    # Convert to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def extract_candidate_details(link_element, base_url="https://www.tripadvisor.co.uk") -> Optional[Dict]:
    """
    Extract details for a single TripAdvisor candidate.
    Returns: { url, name, lat, lng } or None
    """
    try:
        href = link_element.get('href', '')
        if '/Restaurant_Review-' not in href:
            return None

        url = base_url + href if href.startswith('/') else href
        name = link_element.get_text(" ", strip=True)

        # Try to extract lat/lng from the page URL or data attributes
        # TripAdvisor URLs often contain location codes we can parse
        # For now, return None for lat/lng - will be extracted from detail page

        return {
            'url': url,
            'name': name,
            'lat': None,
            'lng': None,
            'address': None
        }
    except Exception as e:
        return None


def scrape_candidate_geolocation(url: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Scrape the candidate's detail page to extract lat/lng and address.
    Returns: (lat, lng, address)
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        lat, lng, address = None, None, None

        # Try JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                obj = json.loads(script.string)
                if obj.get("@type") == "Restaurant":
                    # Extract lat/lng
                    geo = obj.get("geo", {})
                    if isinstance(geo, dict):
                        lat = geo.get("latitude")
                        lng = geo.get("longitude")

                    # Extract address
                    addr = obj.get("address", {})
                    if isinstance(addr, dict):
                        address = addr.get("streetAddress", "")
                        city = addr.get("addressLocality", "")
                        if city:
                            address = f"{address}, {city}".strip(", ")

                    if lat and lng:
                        break
            except Exception:
                continue

        # Fallback: try meta tags or data attributes
        if not lat or not lng:
            # Look for data-lat/data-lng attributes
            for elem in soup.find_all(attrs={"data-lat": True}):
                try:
                    lat = float(elem.get("data-lat"))
                    lng = float(elem.get("data-lng"))
                    break
                except:
                    pass

        return (float(lat) if lat else None,
                float(lng) if lng else None,
                address)
    except Exception as e:
        return None, None, None


def check_area_match(area: str, address: str) -> bool:
    """
    Check if area keywords appear in the candidate's address.
    """
    if not area or not address:
        return False

    area_normalized = normalize_name(area)
    address_normalized = normalize_name(address)

    # Check if any area token appears in address
    area_tokens = area_normalized.split()
    return any(token in address_normalized for token in area_tokens if len(token) > 2)


def calculate_confidence_score(name_sim: float, area_match: bool, distance_m: Optional[float]) -> float:
    """
    Calculate overall confidence score.

    Formula:
    confidence = (name_similarity * 0.5) + (area_match * 0.3) + (distance_score * 0.2)

    Where distance_score = 1 - (distance_m / 1000), clamped to 0-1
    """
    score = name_sim * 0.5
    score += (1.0 if area_match else 0.0) * 0.3

    if distance_m is not None:
        distance_score = max(0.0, min(1.0, 1.0 - (distance_m / MAX_DISTANCE_METERS)))
        score += distance_score * 0.2

    return round(score, 2)


def search_tripadvisor_validated(
    name: str,
    city: str = "London",
    area: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None
) -> Dict:
    """
    Search TripAdvisor with multi-candidate validation.

    Returns:
    {
        'url': validated URL or None,
        'status': 'found' | 'not_found',
        'confidence': 0.0-1.0 or None,
        'distance_m': distance in meters or None,
        'match_notes': explanation string
    }
    """
    query = quote(f"{name} {city}")
    search_url = f"https://www.tripadvisor.co.uk/Search?q={query}"

    try:
        r = requests.get(search_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        return {
            'url': None,
            'status': 'not_found',
            'confidence': None,
            'distance_m': None,
            'match_notes': f'Search request failed: {str(e)[:50]}'
        }

    # STEP 1: Collect up to MAX_CANDIDATES candidate links
    candidates = []
    for link in soup.select("a[href*='/Restaurant_Review']"):
        if len(candidates) >= MAX_CANDIDATES:
            break

        candidate = extract_candidate_details(link)
        if candidate:
            candidates.append(candidate)

    if not candidates:
        return {
            'url': None,
            'status': 'not_found',
            'confidence': None,
            'distance_m': None,
            'match_notes': 'No restaurant candidates found in search results'
        }

    # STEP 2-6: Score each candidate
    scored_candidates = []

    for candidate in candidates:
        # STEP 2: Name similarity
        name_sim = calculate_name_similarity(name, candidate['name'])

        # HARD RULE: Reject if similarity < MIN_NAME_SIMILARITY
        if name_sim < MIN_NAME_SIMILARITY:
            continue

        # STEP 3: Fetch geolocation from candidate page
        cand_lat, cand_lng, cand_address = scrape_candidate_geolocation(candidate['url'])
        candidate['lat'] = cand_lat
        candidate['lng'] = cand_lng
        candidate['address'] = cand_address

        # STEP 4: Geographic distance validation
        distance_m = None
        if latitude and longitude and cand_lat and cand_lng:
            distance_m = haversine_distance(latitude, longitude, cand_lat, cand_lng)

            # HARD RULE: Reject if distance > MAX_DISTANCE_METERS
            if distance_m > MAX_DISTANCE_METERS:
                continue

        # STEP 5: Area match
        area_match = check_area_match(area, cand_address) if area and cand_address else False

        # STEP 6: Confidence score
        confidence = calculate_confidence_score(name_sim, area_match, distance_m)

        scored_candidates.append({
            'candidate': candidate,
            'name_similarity': name_sim,
            'distance_m': distance_m,
            'area_match': area_match,
            'confidence': confidence
        })

    if not scored_candidates:
        return {
            'url': None,
            'status': 'not_found',
            'confidence': None,
            'distance_m': None,
            'match_notes': f'All {len(candidates)} candidates rejected (name similarity < {MIN_NAME_SIMILARITY} or distance > {MAX_DISTANCE_METERS}m)'
        }

    # STEP 7: Select best candidate
    best = max(scored_candidates, key=lambda x: x['confidence'])

    # HARD RULE: Accept only if confidence >= MIN_CONFIDENCE_SCORE
    if best['confidence'] < MIN_CONFIDENCE_SCORE:
        return {
            'url': None,
            'status': 'not_found',
            'confidence': None,
            'distance_m': None,
            'match_notes': f'Best candidate confidence ({best["confidence"]}) below threshold ({MIN_CONFIDENCE_SCORE})'
        }

    # ACCEPTED!
    notes_parts = []
    notes_parts.append(f"name_sim={best['name_similarity']:.2f}")
    if best['area_match']:
        notes_parts.append("area_match=true")
    if best['distance_m'] is not None:
        notes_parts.append(f"distance={best['distance_m']:.0f}m")

    return {
        'url': best['candidate']['url'],
        'status': 'found',
        'confidence': best['confidence'],
        'distance_m': best['distance_m'],
        'match_notes': ' | '.join(notes_parts)
    }


def search_tripadvisor(name: str, city: str = "London"):
    """
    Legacy function for backward compatibility.
    Returns URL only (or None).
    """
    result = search_tripadvisor_validated(name, city)
    return result['url']


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
