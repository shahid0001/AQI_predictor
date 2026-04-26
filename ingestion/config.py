import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# API Keys
API_KEY = os.getenv("API_KEY")

# Location
LAT = float(os.getenv("LAT"))
LON = float(os.getenv("LON"))

# Weather Dates (Open-Meteo)
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")

# Pollution Timestamps (OpenWeather)
START = int(os.getenv("START_TS"))
END = int(os.getenv("END_TS"))

# Optional: Validation (VERY IMPORTANT)
if API_KEY is None:
    raise ValueError("❌ API_KEY not found in .env")

if LAT is None or LON is None:
    raise ValueError("❌ LAT/LON missing in .env")