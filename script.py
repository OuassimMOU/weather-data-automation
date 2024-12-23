import os
import requests
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def fetch_weather_data():
    """
    Fetch weather data from OpenWeatherMap API.
    """
    try:
        # Retrieve API key from environment variable
        api_key = os.getenv("WEATHER_API_KEY")  # Use the correct environment variable name
        if not api_key:
            raise ValueError("Environment variable WEATHER_API_KEY is not set or is invalid.")

        # API endpoint
        city = "London"  # Replace with your desired city
        api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

        # Send API request
        response = requests.get(api_url)
        response.raise_for_status()  # Raise exception for HTTP errors
        weather_data = response.json()

        print("Weather data fetched successfully:")
        print(weather_data)  # Print the fetched data for debugging
        return weather_data

    except Exception as e:
        print(f"Error fetching weather data: {e}")
        raise

def upload_to_blob(data):
    """
    Upload data to Azure Blob Storage.
    """
    try:
        # Retrieve Azure Storage connection string from environment variable
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")  # Use the correct environment variable name
        if not connection_string:
            raise ValueError("Environment variable AZURE_STORAGE_CONNECTION_STRING is not set or is invalid.")

        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = "rawweatherdata"  # Replace with your container name
        blob_name = f"weatherdata_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"

        # Upload data to the specified container
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(str(data), overwrite=True)

        print(f"Data successfully uploaded as {blob_name}.")

    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")
        raise

if __name__ == "__main__":
    try:
        # Fetch weather data from the API
        data = fetch_weather_data()

        # Upload fetched data to Azure Blob Storage
        upload_to_blob(data)

    except Exception as e:
        print(f"Script execution failed: {e}")
