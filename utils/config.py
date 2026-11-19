"""Project paths and database configuration."""
from pathlib import Path

#Path to data
PATH = Path(__file__).resolve().parents[1]/'data'/'archive.zip'
#Path to cleaned data
CLEANED_PATH = Path(__file__).resolve().parents[1]/'data'/'cleaned_data.csv'

TABLE_NAME = 'reviews'