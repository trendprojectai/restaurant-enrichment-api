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

# Final enriched dataset (after tertiary scrape completion)
final_enriched_dataset = []
secondary_dataset = []  # Store secondary results for merging
final_csv_path = None  # Path to the persisted final CSV file

# CANONICAL CSV SCHEMA - used by ALL CSV writers in the pipeline
# This is the SINGLE source of truth for ALL CSV operations
# Includes input fields, enrichment fields, and tracking fields
CSV_FIELDNAMES = [
    # Core identifiers (input fields)
    'google_place_id',
    'name',
    'website',
    'address',
    'city',
    'area',
    'latitude',
    'longitude',

    # Secondary enrichment fields
    'cover_image',
    'cover_image_alt',
    'menu_url',
    'menu_pdf_url',
    'gallery_images',
    'phone',
    'phone_formatted',
    'email',
    'instagram_handle',
    'instagram_url',
    'tiktok_handle',
    'tiktok_url',
    'tiktok_videos',
    'facebook_url',
    'opening_hours',
    'cuisine_type',
    'price_range',

    # Tertiary (TripAdvisor) enrichment fields
    'tripadvisor_url',
    'tripadvisor_status',
    'tripadvisor_confidence',
    'tripadvisor_distance_m',
    'tripadvisor_match_notes',
    'tertiary_updates',
]


def ensure_csv_compatibility(row: dict) -> dict:
    """
    Ensure backward compatibility when reading CSVs.
    Adds missing fields with None defaults for old CSVs that predate schema changes.

    Args:
        row: Dict from CSV reader

    Returns:
        Dict with all CSV_FIELDNAMES guaranteed to exist
    """
    for field in CSV_FIELDNAMES:
        row.setdefault(field, None)
    return row


def merge_enriched_results(base_dataset, fallback_results):
    """
    Merge tertiary (TripAdvisor) results into secondary dataset.

    Args:
        base_dataset: Secondary enrichment results (complete records)
        fallback_results: TripAdvisor fallback results (partial records with only critical fields)

    Returns:
        Complete merged dataset ready for export/media/video injection
    """
    # Create lookup for fallback results by google_place_id
    fallback_lookup = {
        item['google_place_id']: item
        for item in fallback_results
    }

    merged = []

    for record in base_dataset:
        place_id = record.get('google_place_id')

        # If we have TripAdvisor fallback data for this restaurant, merge it
        if place_id in fallback_lookup:
            fallback = fallback_lookup[place_id]

            # Apply TripAdvisor results ONLY to missing fields (fill nulls only)
            if not record.get('opening_hours'):
                record['opening_hours'] = fallback.get('opening_hours')
            if not record.get('cuisine_type'):
                record['cuisine_type'] = fallback.get('cuisine_type')
            if not record.get('price_range'):
                record['price_range'] = fallback.get('price_range')
            if not record.get('phone'):
                record['phone'] = fallback.get('phone')

            # Merge TripAdvisor tracking fields
            record['tripadvisor_url'] = fallback.get('tripadvisor_url')
            record['tripadvisor_status'] = fallback.get('tripadvisor_status')
            record['tertiary_updates'] = fallback.get('tertiary_updates')
            record['tripadvisor_confidence'] = fallback.get('tripadvisor_confidence')
            record['tripadvisor_distance_m'] = fallback.get('tripadvisor_distance_m')
            record['tripadvisor_match_notes'] = fallback.get('tripadvisor_match_notes')

        merged.append(record)

    return merged


def write_final_csv(dataset):
    """
    Write the final enriched dataset to a persistent CSV file.
    This ensures single CSV continuity across all downstream stages.

    Args:
        dataset: Final merged dataset to write

    Returns:
        Path to the written CSV file
    """
    global final_csv_path

    # Create a persistent temp file (won't be auto-deleted)
    final_csv_path = os.path.join(tempfile.gettempdir(), 'final_enriched_dataset.csv')

    with open(final_csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        for record in dataset:
            # Convert lists/dicts to JSON
            record_copy = record.copy()
            record_copy['gallery_images'] = json.dumps(record_copy.get('gallery_images', [])) if record_copy.get('gallery_images') else None
            record_copy['opening_hours'] = json.dumps(record_copy.get('opening_hours', [])) if record_copy.get('opening_hours') else None
            record_copy['tiktok_videos'] = json.dumps(record_copy.get('tiktok_videos', [])) if record_copy.get('tiktok_videos') else None
            record_copy['tertiary_updates'] = json.dumps(record_copy.get('tertiary_updates', {})) if record_copy.get('tertiary_updates') else None

            # SAFE ROW WRITE: only include fields in canonical schema
            safe_row = {key: record_copy.get(key) for key in CSV_FIELDNAMES}
            writer.writerow(safe_row)

    print(f"✓ Final CSV written to: {final_csv_path}")
    return final_csv_path


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
                "area": r.get("area"),
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
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
            '/tertiary/enrich': 'POST - Run TripAdvisor enrichment on snapshot',
            '/media/inject': 'POST - Inject media into final enriched dataset',
            '/export/push': 'POST - Export final enriched dataset as CSV',
            '/video-injector/push': 'POST - Send final enriched dataset to video injector'
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

        # Read input CSV with backward compatibility
        with open(temp_input_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            restaurants = [ensure_csv_compatibility(row) for row in reader]

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
                # Create fallback record with input fields preserved
                fallback_record = {
                    # Preserve input fields
                    'google_place_id': restaurant.get('google_place_id', ''),
                    'name': restaurant.get('name'),
                    'website': restaurant.get('website'),
                    'address': restaurant.get('address'),
                    'city': restaurant.get('city'),
                    'area': restaurant.get('area'),
                    'latitude': restaurant.get('latitude'),
                    'longitude': restaurant.get('longitude'),

                    # All enrichment fields as None
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
                    'tripadvisor_url': None,
                    'tripadvisor_status': None,
                    'tripadvisor_confidence': None,
                    'tripadvisor_distance_m': None,
                    'tripadvisor_match_notes': None,
                    'tertiary_updates': None,
                }
                enriched_data.append(fallback_record)
        
        # Write output CSV using canonical schema
        with open(temp_output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()

            for data in enriched_data:
                # Convert lists/dicts to JSON
                data_copy = data.copy()
                data_copy['gallery_images'] = json.dumps(data_copy.get('gallery_images', [])) if data_copy.get('gallery_images') else None
                data_copy['opening_hours'] = json.dumps(data_copy.get('opening_hours', [])) if data_copy.get('opening_hours') else None
                data_copy['tiktok_videos'] = json.dumps(data_copy.get('tiktok_videos', [])) if data_copy.get('tiktok_videos') else None
                data_copy['tertiary_updates'] = json.dumps(data_copy.get('tertiary_updates', {})) if data_copy.get('tertiary_updates') else None

                # SAFE ROW WRITE: only include fields in canonical schema
                safe_row = {key: data_copy.get(key) for key in CSV_FIELDNAMES}
                writer.writerow(safe_row)
        
        # Read enriched output
        with open(temp_output_path, 'r', encoding='utf-8') as f:
            enriched_csv = f.read()

        # Store secondary dataset globally for tertiary merge
        global secondary_dataset
        secondary_dataset = enriched_data.copy()

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
        from scrapers.tripadvisor_scraper import search_tripadvisor_validated, scrape_tripadvisor_page

        enriched_data = []

        for i, restaurant in enumerate(tertiary_snapshot):
            try:
                name = restaurant.get('name', 'Unknown')
                city = restaurant.get('city', 'London')
                area = restaurant.get('area')
                latitude = restaurant.get('latitude')
                longitude = restaurant.get('longitude')
                google_place_id = restaurant.get('google_place_id', '')

                # Convert lat/lng to float if they're strings
                try:
                    if latitude is not None and not isinstance(latitude, float):
                        latitude = float(latitude)
                    if longitude is not None and not isinstance(longitude, float):
                        longitude = float(longitude)
                except (ValueError, TypeError):
                    latitude = None
                    longitude = None

                print(f"TripAdvisor enriching {i+1}/{len(tertiary_snapshot)}: {name}")

                # Search TripAdvisor with validation
                ta_result = search_tripadvisor_validated(
                    name=name,
                    city=city,
                    area=area,
                    latitude=latitude,
                    longitude=longitude
                )

                if ta_result['status'] == 'found':
                    print(f"  ✓ Found on TripAdvisor: {ta_result['url']}")
                    print(f"    Confidence: {ta_result['confidence']}, Distance: {ta_result['distance_m']}m")

                    # Scrape the validated page
                    ta_data = scrape_tripadvisor_page(ta_result['url'])

                    # Track which fields were updated
                    updates = {}

                    # Merge with existing data (fill nulls only) and track updates
                    result = {
                        'google_place_id': google_place_id,
                        'tripadvisor_url': ta_result['url'],
                        'tripadvisor_status': 'found',
                        'tripadvisor_confidence': ta_result['confidence'],
                        'tripadvisor_distance_m': ta_result['distance_m'],
                        'tripadvisor_match_notes': ta_result['match_notes'],
                    }

                    # Opening hours
                    if not restaurant.get('existing_opening_hours') and ta_data.get('opening_hours'):
                        result['opening_hours'] = ta_data.get('opening_hours')
                        updates['opening_hours'] = 'filled_from_tripadvisor'
                    else:
                        result['opening_hours'] = restaurant.get('existing_opening_hours')

                    # Cuisine type
                    if not restaurant.get('existing_cuisine_type') and ta_data.get('cuisine_type'):
                        result['cuisine_type'] = ta_data.get('cuisine_type')
                        updates['cuisine_type'] = 'filled_from_tripadvisor'
                    else:
                        result['cuisine_type'] = restaurant.get('existing_cuisine_type')

                    # Price range
                    if not restaurant.get('existing_price_range') and ta_data.get('price_range'):
                        result['price_range'] = ta_data.get('price_range')
                        updates['price_range'] = 'filled_from_tripadvisor'
                    else:
                        result['price_range'] = restaurant.get('existing_price_range')

                    # Phone
                    if not restaurant.get('existing_phone') and ta_data.get('phone'):
                        result['phone'] = ta_data.get('phone')
                        updates['phone'] = 'filled_from_tripadvisor'
                    else:
                        result['phone'] = restaurant.get('existing_phone')

                    # Store updates
                    result['tertiary_updates'] = updates if updates else None

                    enriched_data.append(result)
                else:
                    print(f"  ⚠ Not found on TripAdvisor: {ta_result['match_notes']}")
                    # Return existing data with not_found status
                    enriched_data.append({
                        'google_place_id': google_place_id,
                        'opening_hours': restaurant.get('existing_opening_hours'),
                        'cuisine_type': restaurant.get('existing_cuisine_type'),
                        'price_range': restaurant.get('existing_price_range'),
                        'phone': restaurant.get('existing_phone'),
                        'tripadvisor_url': None,
                        'tripadvisor_status': 'not_found',
                        'tertiary_updates': None,
                        'tripadvisor_confidence': None,
                        'tripadvisor_distance_m': None,
                        'tripadvisor_match_notes': ta_result['match_notes'],
                    })

            except Exception as e:
                print(f"Error enriching {restaurant.get('name', 'Unknown')}: {e}")
                import traceback
                traceback.print_exc()
                # Return existing data on error with error status
                enriched_data.append({
                    'google_place_id': restaurant.get('google_place_id', ''),
                    'opening_hours': restaurant.get('existing_opening_hours'),
                    'cuisine_type': restaurant.get('existing_cuisine_type'),
                    'price_range': restaurant.get('existing_price_range'),
                    'phone': restaurant.get('existing_phone'),
                    'tripadvisor_url': None,
                    'tripadvisor_status': 'error',
                    'tertiary_updates': None,
                    'tripadvisor_confidence': None,
                    'tripadvisor_distance_m': None,
                    'tripadvisor_match_notes': f'Error: {str(e)[:100]}',
                })

        print(f"✓ Completed TripAdvisor enrichment for {len(enriched_data)} restaurants")

        # Merge tertiary results with secondary dataset to create final enriched dataset
        global final_enriched_dataset
        final_enriched_dataset = merge_enriched_results(
            base_dataset=secondary_dataset,
            fallback_results=enriched_data
        )

        print(f"✓ Final enriched dataset created with {len(final_enriched_dataset)} restaurants")

        # Write final CSV to disk to ensure single CSV continuity
        csv_path = write_final_csv(final_enriched_dataset)

        return jsonify({
            'success': True,
            'count': len(enriched_data),
            'data': enriched_data,
            'final_dataset_count': len(final_enriched_dataset),
            'csv_path': csv_path
        })

    except Exception as e:
        print(f"Error in tertiary enrichment: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================================================
# POST-TERTIARY DOWNSTREAM ACTIONS
# ============================================================================

@app.route('/media/inject', methods=['POST', 'OPTIONS'])
def inject_media():
    """
    Inject media (images/videos/etc) into final enriched dataset.
    This is a pass-through operation on the complete dataset.
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200

    try:
        if len(final_enriched_dataset) == 0:
            return jsonify({'error': 'No final enriched dataset available. Run tertiary enrichment first.'}), 400

        print(f"Media injection requested for {len(final_enriched_dataset)} restaurants...")
        print(f"  → Using persisted CSV from: {final_csv_path}")

        # TODO: Implement actual media injection logic
        # For now, this is a pass-through that accepts the dataset
        dataset = final_enriched_dataset.copy()

        print(f"✓ Media injection completed for {len(dataset)} restaurants")

        return jsonify({
            'success': True,
            'count': len(dataset),
            'message': 'Media injection completed',
            'dataset_sample': dataset[:5] if len(dataset) > 0 else []
        })

    except Exception as e:
        print(f"Error in media injection: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/export/push', methods=['POST', 'OPTIONS'])
def push_to_export():
    """
    Push final enriched dataset to export stage.
    Serializes dataset to CSV and makes it available for export.
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200

    try:
        if len(final_enriched_dataset) == 0:
            return jsonify({'error': 'No final enriched dataset available. Run tertiary enrichment first.'}), 400

        print(f"Export push requested for {len(final_enriched_dataset)} restaurants...")
        print(f"  → Using persisted CSV from: {final_csv_path}")

        # Create CSV from final dataset using canonical schema
        import io
        output = io.StringIO()

        writer = csv.DictWriter(output, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        for record in final_enriched_dataset:
            # Convert lists/dicts to JSON
            record_copy = record.copy()
            record_copy['gallery_images'] = json.dumps(record_copy.get('gallery_images', [])) if record_copy.get('gallery_images') else None
            record_copy['opening_hours'] = json.dumps(record_copy.get('opening_hours', [])) if record_copy.get('opening_hours') else None
            record_copy['tiktok_videos'] = json.dumps(record_copy.get('tiktok_videos', [])) if record_copy.get('tiktok_videos') else None
            record_copy['tertiary_updates'] = json.dumps(record_copy.get('tertiary_updates', {})) if record_copy.get('tertiary_updates') else None

            # SAFE ROW WRITE: only include fields in canonical schema
            safe_row = {key: record_copy.get(key) for key in CSV_FIELDNAMES}
            writer.writerow(safe_row)

        csv_output = output.getvalue()

        print(f"✓ Export CSV created for {len(final_enriched_dataset)} restaurants")

        return jsonify({
            'success': True,
            'count': len(final_enriched_dataset),
            'csv_data': csv_output,
            'message': 'Dataset exported successfully'
        })

    except Exception as e:
        print(f"Error in export push: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/video-injector/push', methods=['POST', 'OPTIONS'])
def push_to_video_injector():
    """
    Push final enriched dataset to Video Injector pipeline.
    Sends dataset to video injection processing.
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200

    try:
        if len(final_enriched_dataset) == 0:
            return jsonify({'error': 'No final enriched dataset available. Run tertiary enrichment first.'}), 400

        print(f"Video injector push requested for {len(final_enriched_dataset)} restaurants...")
        print(f"  → Using persisted CSV from: {final_csv_path}")

        # TODO: Implement actual video injector integration
        # For now, this is a pass-through that accepts the dataset
        dataset = final_enriched_dataset.copy()

        print(f"✓ Video injector push completed for {len(dataset)} restaurants")

        return jsonify({
            'success': True,
            'count': len(dataset),
            'message': 'Dataset sent to video injector',
            'dataset_sample': dataset[:5] if len(dataset) > 0 else []
        })

    except Exception as e:
        print(f"Error in video injector push: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Railway sets PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
