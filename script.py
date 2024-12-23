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
    weather_data = response.json()
    print(weather_data)  # Add this to verify the API response
    return weather_data

# Upload data to Azure Blob Storage
def upload_to_blob(data):
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = "rawweatherdata"  # Ensure this matches your Azure container name
        blob_name = f"weatherdata_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"

        # Upload to Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(str(data), overwrite=True)
        print(f"Data successfully saved as {blob_name}.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")


if __name__ == "__main__":
    data = fetch_weather_data()
    upload_to_blob(data)
