import requests

print("===== Testing Backend API =====")

# Test backend health
try:
    response = requests.get("http://localhost:8000/api/tables/channel", timeout=5)
    print(f"\nChannel table response status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        if 'error' in data:
            print(f"Error: {data['error']}")
        else:
            print(f"Success! Found {len(data.get('data', []))} rows")
            if data.get('data'):
                print(f"Columns: {data.get('columns', [])[:5]}...")  # Show first 5 columns
    else:
        print(f"HTTP Error: {response.text}")
except Exception as e:
    print(f"Connection Error: {e}")
    print("\n⚠️ Backend server might not be running on port 8000")
