import requests

url = "https://esgoo.net/api-mst/0319020730.htm"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")
