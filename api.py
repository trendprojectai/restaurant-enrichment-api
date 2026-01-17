from flask import Flask, request, jsonify
from flask_cors import CORS
import csv
from secondary_enrichment import RestaurantEnricher
import tempfile
import os
import json

app = Flask(__name__)

# FIXED: Proper CORS configuration for all origins
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Accept"],
        "expose_headers": ["Content-Type"]
    }
})

@app.route('/', methods=['GET'])
def home():
    """Root endpoint - shows API is running"""
    return jsonify({
        'message': 'Restaurant Enrichment API is running!',
        'status': 'ok',
        'endpoints': {
            '/health': 'Health check',
            '/enrich': 'POST - Enrich restaurant data'
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
        
        # Run enrichment using the existing script
        enricher = RestaurantEnricher()
        
        # Read input CSV
        with open(temp_input_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            restaurants = list(reader)
        
        print(f"Processing {len(restaurants)} restaurants...")
        
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
                    'menu_url': None,
                    'menu_pdf_url': None,
                    'gallery_images': [],
                    'phone': None,
                    'opening_hours': None,
                })
        
        # Write output CSV
        with open(temp_output_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['google_place_id', 'cover_image', 'menu_url', 'menu_pdf_url', 
                         'gallery_images', 'phone', 'opening_hours']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for data in enriched_data:
                row = data.copy()
                row['gallery_images'] = json.dumps(row['gallery_images']) if row['gallery_images'] else None
                row['opening_hours'] = json.dumps(row['opening_hours']) if row['opening_hours'] else None
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

if __name__ == '__main__':
    # Railway sets PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
