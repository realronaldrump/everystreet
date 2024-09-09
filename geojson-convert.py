import json

input_file = 'static/Waco-Streets.geojson'
output_file = 'filtered.geojson'
search_term = 'Valley Mills'

try:
    with open(input_file, 'r') as infile:
        data = json.load(infile)

    # Ensure 'features' is a list and filter features
    if 'features' in data and isinstance(data['features'], list):
        filtered_features = [
            feature for feature in data['features']
            if feature.get('properties', {}).get('name') and search_term in feature['properties']['name']
        ]

        filtered_data = {
            'type': 'FeatureCollection',
            'name': data.get('name', ''),
            'crs': data.get('crs', {}),
            'features': filtered_features
        }

        with open(output_file, 'w') as outfile:
            json.dump(filtered_data, outfile, indent=4)

        print(f'Filtered GeoJSON saved to {output_file}')
    else:
        print(f'No valid "features" found in {input_file}')

except json.JSONDecodeError:
    print(f'Error decoding JSON from {input_file}')
except FileNotFoundError:
    print(f'File {input_file} not found')
except Exception as e:
    print(f'An error occurred: {e}')