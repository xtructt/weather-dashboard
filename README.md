# Weather ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline for processing weather forecast data. This pipeline processes weather data from JSON input, performs various transformations and statistical calculations, and outputs structured CSV files for further analysis.

## Features

- Reads and validates weather forecast data from JSON input
- Processes location, current temperature, and forecast information
- Calculates temperature statistics and forecast accuracy
- Handles data updates with insert-overwrite strategy
- Outputs structured CSV files for analysis
- Supports timezone conversion to UTC
- Comprehensive logging system

## Prerequisites

- Python ≥ 3.11
- Dependencies (from pyproject.toml):
  - pandas ≥ 2.2.3
  - plotly ≥ 5.24.1
  - polars ≥ 1.14.0
  - pydantic ≥ 2.10.0
  - ruff ≥ 0.7.4
  - streamlit ≥ 1.40.1
  - pytz (for timezone handling)

## Installation

Install dependencies using pip:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Run the pipeline with default input file:

```bash
python data_processing.py
```

### Custom Input File

Specify a custom input JSON file:

```bash
python data_processing.py -i path/to/your/input.json
```

### Command Line Arguments

- `-i`, `--input`: Path to the input JSON file (optional, defaults to 'ETL_developer_Case.json')
- `-h`, `--help`: Show help message

## Input Data Format

The input JSON should be structured as a dictionary where keys are location identifiers (e.g., "Bangkok, Thailand") and values contain weather data. Here's an example structure:

```json
{
  "Bangkok, Thailand": {
    "location": {
      "name": "Bangkok",
      "region": "Krung Thep",
      "country": "Thailand",
      "lat": 13.75,
      "lon": 100.5167,
      "tz_id": "Asia/Bangkok",
      "localtime_epoch": 1728266500,
      "localtime": "2024-10-07 09:01"
    },
    "current": {
      "last_updated": "2024-10-07 09:00",
      "temp_c": 29.3,
      "temp_f": 84.7,
      "is_day": 1,
      "condition": {
        "text": "Partly cloudy",
        "code": 1003
      }
    },
    "forecast": {
      "forecastday": [
        {
          "date": "2024-10-07",
          "date_epoch": 1728259200,
          "day": {
            "maxtemp_c": 32.9,
            "mintemp_c": 25.5,
            "avgtemp_c": 28.7,
            "maxwind_kph": 10.8,
            "totalprecip_mm": 1.09,
            "avghumidity": 72,
            "daily_chance_of_rain": 86,
            "condition": {
              "text": "Patchy rain nearby",
              "code": 1063
            },
            "uv": 10.0
          }
        }
      ]
    }
  }
}
```

## Output Files

The pipeline generates five CSV files in the `output` directory:

1. `location.csv`: Location information including coordinates and timezone
2. `current_temp.csv`: Current temperature readings
3. `forecast_temp.csv`: Forecast temperature data
4. `merged.csv`: Combined current and forecast data with calculated differences
5. `stats.csv`: Statistical analysis of temperature data

### Key Calculations

The pipeline performs several calculations including:

- Temperature differences between forecast and actual readings
- Day-over-day temperature changes
- Minimum and maximum temperatures
- Forecast accuracy metrics
- Average temperatures and trends
- Precipitation probabilities
- UV index tracking

## Logging

The pipeline logs all operations to both console and file (`weather_process.log`). Log messages include:

- Pipeline start and completion
- File operations
- Data validation steps
- Error messages (if any)

## Error Handling

The pipeline includes comprehensive error handling for:

- File not found errors
- JSON parsing errors
- Data validation errors
- File write errors
- Schema validation errors

## Development

This project uses:

- `pydantic` for data validation
- `polars` for efficient data processing
- `pytz` for timezone handling
- `ruff` for code formatting and linting

## Project Structure

```
weather-etl/
├── weather_etl.py       # Main ETL script
├── requirements.txt     # Project dependencies
├── pyproject.toml      # Project configuration
├── README.md           # This file
└── output/             # Generated CSV files
    ├── location.csv
    ├── current_temp.csv
    ├── forecast_temp.csv
    ├── merged.csv
    └── stats.csv
```
