from flask import Flask, request, jsonify
from flask_cors import CORS
import csv
from secondary_enrichment import RestaurantEnricher
import tempfile
import os
import json

app = Flask(__name__)

# Enable CORS - allow all origins
CORS(app, resources={r"/*": {"origins": "*"}})

# Tertiary snapshot storage (immutable once created)
tertiary_snapshot = []
tertiary_snapshot_locked = False


def create_tertiary_snapshot(secondary_data):
    """
    Create an immutable snapshot of restaurants that need TripAdvisor fallback.
    This snapshot is based ONLY on secondary scrape results and should not be recomputed.
    """
    snapshot = []

    for r in secondary_data:
        # Check if ANY of the critical fields are missing
        if (
            not r.get("opening_hours") or
            not r.get("cuisine_type") or
            not r.get("price_range") or
            not r.get("phone")
        ):
            snapshot.append({
                "google_place_id": r.get("google_place_id"),
                "name": r.get("name"),
                "city": r.get("city"),
                "website": r.get("website"),
                # Store existing values to preserve them during merge
                "existing_opening_hours": r.get("opening_hours"),
                "existing_cuisine_type": r.get("cuisine_type"),
                "existing_price_range": r.get("price_range"),
                "existing_phone": r.get("phone"),
            })

    return snapshot


@app.route('/', methods=['GET'])
def home():
    """Root endpoint - shows API is running"""
    return jsonify({
        'message': 'Restaurant Enrichment API is running!',
        'status': 'ok',
        'endpoints': {
            '/health': 'Health check',
            '/enrich': 'POST - Enrich restaurant data (secondary scraping)',
            '/tertiary/snapshot': 'POST - Create immutable tertiary snapshot from secondary results',
            '/tertiary/snapshot/status': 'GET - Get tertiary snapshot status',
            '/tertiary/enrich': 'POST - Run TripAdvisor enrichment on snapshot'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'Server is running'}), 200

@app.route('/enrich', methods=['POST', 'OPTIONS'])
def enrich():
    """Enrich restaurant data from CSV"""
    
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        # Get CSV data from request
        data = request.get_json()
        if not data or 'csv_data' not in data:
            return jsonify({'error': 'Missing csv_data in request'}), 400
        
        csv_data = data['csv_data']
        
        # Write to temp input file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as temp_input:
            temp_input.write(csv_data)
            temp_input_path = temp_input.name
        
        # Create temp output file path
        temp_output_path = temp_input_path.replace('.csv', '_enriched.csv')

        # Run enrichment using the existing script (TripAdvisor disabled for secondary stage)
        enricher = RestaurantEnricher(enable_tripadvisor=False)

        # Read input CSV
        with open(temp_input_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            restaurants = list(reader)

        print(f"Processing {len(restaurants)} restaurants (secondary enrichment only)...")
        
        # Enrich each restaurant
        enriched_data = []
        for i, restaurant in enumerate(restaurants):
            try:
                print(f"Enriching {i+1}/{len(restaurants)}: {restaurant.get('name', 'Unknown')}")
                enrichment = enricher.enrich_restaurant(restaurant)
                enriched_data.append(enrichment)
            except Exception as e:
                print(f"Error enriching {restaurant.get('name', 'Unknown')}: {e}")
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
        
        # Write output CSV
        with open(temp_output_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = [
                'google_place_id', 'cover_image', 'cover_image_alt',
                'menu_url', 'menu_pdf_url', 'gallery_images',
                'phone', 'phone_formatted', 'email',
                'instagram_handle', 'instagram_url',
                'tiktok_handle', 'tiktok_url', 'tiktok_videos',
                'facebook_url', 'opening_hours',
                'cuisine_type', 'price_range'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for data in enriched_data:
                row = data.copy()
                # Convert lists/dicts to JSON
                row['gallery_images'] = json.dumps(row.get('gallery_images', [])) if row.get('gallery_images') else None
                row['opening_hours'] = json.dumps(row.get('opening_hours', [])) if row.get('opening_hours') else None
                row['tiktok_videos'] = json.dumps(row.get('tiktok_videos', [])) if row.get('tiktok_videos') else None
                writer.writerow(row)
        
        # Read enriched output
        with open(temp_output_path, 'r', encoding='utf-8') as f:
            enriched_csv = f.read()
        
        # Cleanup
        os.unlink(temp_input_path)
        os.unlink(temp_output_path)
        
        print(f"✓ Successfully enriched {len(enriched_data)} restaurants")
        
        return jsonify({
            'enriched_csv': enriched_csv,
            'success': True,
            'count': len(enriched_data)
        })
    
    except Exception as e:
        print(f"Error in enrich endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/tertiary/snapshot', methods=['POST', 'OPTIONS'])
def create_snapshot():
    """Create tertiary snapshot from secondary enrichment results"""
    global tertiary_snapshot, tertiary_snapshot_locked

    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200

    try:
        # Get secondary enrichment data
        data = request.get_json()
        if not data or 'secondary_data' not in data:
            return jsonify({'error': 'Missing secondary_data in request'}), 400

        secondary_data = data['secondary_data']

        # Create the snapshot
        snapshot = create_tertiary_snapshot(secondary_data)

        # Lock the snapshot
        tertiary_snapshot = snapshot
        tertiary_snapshot_locked = True

        print(f"✓ Tertiary snapshot locked with {len(tertiary_snapshot)} items")

        return jsonify({
            'success': True,
            'snapshot_count': len(tertiary_snapshot),
            'locked': tertiary_snapshot_locked,
            'message': f'Snapshot created with {len(tertiary_snapshot)} restaurants needing TripAdvisor fallback'
        })

    except Exception as e:
        print(f"Error creating tertiary snapshot: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/tertiary/snapshot/status', methods=['GET'])
def snapshot_status():
    """Get status of tertiary snapshot"""
    return jsonify({
        'locked': tertiary_snapshot_locked,
        'count': len(tertiary_snapshot),
        'snapshot': tertiary_snapshot if len(tertiary_snapshot) <= 100 else tertiary_snapshot[:100]
    })


@app.route('/tertiary/enrich', methods=['POST', 'OPTIONS'])
def enrich_tertiary():
    """Run TripAdvisor enrichment on tertiary snapshot"""
    global tertiary_snapshot

    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200

    try:
        if not tertiary_snapshot_locked:
            return jsonify({'error': 'Tertiary snapshot not created yet. Call /tertiary/snapshot first'}), 400

        if len(tertiary_snapshot) == 0:
            return jsonify({'message': 'No restaurants in tertiary snapshot', 'success': True, 'count': 0})

        print(f"Running TripAdvisor enrichment on {len(tertiary_snapshot)} restaurants...")

        # Import TripAdvisor scraper
        from scrapers.tripadvisor_scraper import search_tripadvisor, scrape_tripadvisor_page

        enriched_data = []

        for i, restaurant in enumerate(tertiary_snapshot):
            try:
                name = restaurant.get('name', 'Unknown')
                city = restaurant.get('city', 'London')
                google_place_id = restaurant.get('google_place_id', '')

                print(f"TripAdvisor enriching {i+1}/{len(tertiary_snapshot)}: {name}")

                # Search TripAdvisor
                ta_url = search_tripadvisor(name, city)

                if ta_url:
                    print(f"  ✓ Found on TripAdvisor: {ta_url}")
                    ta_data = scrape_tripadvisor_page(ta_url)

                    # Merge with existing data (fill nulls only)
                    result = {
                        'google_place_id': google_place_id,
                        'opening_hours': ta_data.get('opening_hours') if not restaurant.get('existing_opening_hours') else restaurant.get('existing_opening_hours'),
                        'cuisine_type': ta_data.get('cuisine_type') if not restaurant.get('existing_cuisine_type') else restaurant.get('existing_cuisine_type'),
                        'price_range': ta_data.get('price_range') if not restaurant.get('existing_price_range') else restaurant.get('existing_price_range'),
                        'phone': ta_data.get('phone') if not restaurant.get('existing_phone') else restaurant.get('existing_phone'),
                    }

                    enriched_data.append(result)
                else:
                    print(f"  ⚠ Not found on TripAdvisor")
                    # Return existing data
                    enriched_data.append({
                        'google_place_id': google_place_id,
                        'opening_hours': restaurant.get('existing_opening_hours'),
                        'cuisine_type': restaurant.get('existing_cuisine_type'),
                        'price_range': restaurant.get('existing_price_range'),
                        'phone': restaurant.get('existing_phone'),
                    })

            except Exception as e:
                print(f"Error enriching {restaurant.get('name', 'Unknown')}: {e}")
                # Return existing data on error
                enriched_data.append({
                    'google_place_id': restaurant.get('google_place_id', ''),
                    'opening_hours': restaurant.get('existing_opening_hours'),
                    'cuisine_type': restaurant.get('existing_cuisine_type'),
                    'price_range': restaurant.get('existing_price_range'),
                    'phone': restaurant.get('existing_phone'),
                })

        print(f"✓ Completed TripAdvisor enrichment for {len(enriched_data)} restaurants")

        return jsonify({
            'success': True,
            'count': len(enriched_data),
            'data': enriched_data
        })

    except Exception as e:
        print(f"Error in tertiary enrichment: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Railway sets PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
