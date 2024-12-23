import requests
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os

# Fetch connection string from environment variable
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Raise an error if the connection string is not set
if not connection_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set or is invalid.")


# Fetch weather data from API
def fetch_weather_data():
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q=London&appid={os.getenv('WEATHER_API_KEY')}"
    response = requests.get(api_url)
    return response.json()

# Upload data to Azure Blob Storage
def upload_to_blob(data):
    
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set or is invalid.")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "rawweatherdata"
    blob_name = f"weatherdata_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(str(data), overwrite=True)
    print(f"Data saved as {blob_name}.")

if __name__ == "__main__":
    data = fetch_weather_data()
    upload_to_blob(data)
