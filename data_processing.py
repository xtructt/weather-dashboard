import json
import logging
import os
from typing import Tuple
from pydantic import BaseModel
from datetime import date, datetime
import polars as pl
import pytz
import argparse

logger = logging.getLogger(__name__)
"""
Constant Number of milliseconds in a day use for date different calulation (as the result of date substraction is in ms)
"""
MS_PER_DAY = 24 * 60 * 60 * 1000


"""
Schema structure for JSON validation using pydantic
"""


class Location(BaseModel):
    name: str
    region: str
    country: str
    lat: float
    lon: float
    tz_id: str
    localtime_epoch: int
    localtime: datetime


class Condition(BaseModel):
    text: str
    code: int


class CurrentTemp(BaseModel):
    last_updated: datetime
    temp_c: float
    temp_f: float
    is_day: int
    condition: Condition


class ForcastDetail(BaseModel):
    maxtemp_c: float
    maxtemp_f: float
    mintemp_c: float
    mintemp_f: float
    avgtemp_c: float
    avgtemp_f: float
    maxwind_mph: float
    maxwind_kph: float
    totalprecip_mm: float
    totalprecip_in: float
    totalsnow_cm: float
    avgvis_km: float
    avgvis_miles: float
    avghumidity: float
    daily_will_it_rain: bool
    daily_chance_of_rain: int
    daily_will_it_snow: bool
    daily_chance_of_snow: int
    condition: Condition
    uv: float


class ForcastDay(BaseModel):
    date: date
    date_epoch: int
    day: ForcastDetail


class Forecast(BaseModel):
    forecastday: list[ForcastDay]


class InputData(BaseModel):
    location: Location
    current: CurrentTemp
    forecast: Forecast


class WeatherDataProcess:
    def __init__(self):
        self.logger = logger

    def _read_prev_data(self, filename: str) -> pl.DataFrame | None:
        """Read existing file if it exists, return None if it doesn't"""
        file_path = f"output/{filename}"
        if os.path.exists(file_path):
            return pl.read_csv(file_path)
        return None

    def _read_json(self, file_path: str) -> dict:
        """Read json file from given path

        Args:
            file_path (str): Path to JSON file

        Raises:
            FileNotFoundError: Raise error when file not found and stop the pipeline

        Returns:
            dict: Loaded json as dictionary
        """
        try:
            self.logger.info(f"Reading input file at: {file_path}")
            if not os.path.exists(file_path):
                raise FileNotFoundError(
                    f"Provided json file does not exist: {file_path}"
                )

            with open(file_path, "r", encoding="utf-8") as data_file:
                return json.load(data_file)

        except FileNotFoundError as e:
            self.logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path}: {e}")
            raise

    def _validate_input_data(self, data: dict) -> InputData:
        """
        Using pydantic and the above schema to validate the input json record.
        If success it will retuen InputData object. Otherwise, raise error.

        Args:
            data (dict): a record from the input JSON file

        Returns:
            InputData: InputData object
        """
        return InputData(**data)

    def _local_time_to_utc(self, local_time: datetime, tz_id: str) -> datetime:
        """Take the datetime and convert it to UTC time

        Args:
            local_time (datetime): Local time from the data
            tz_id (str): timezone ID from the data

        Returns:
            datetime: UTC time of the provided local time given the timezone id
        """
        local_tz = pytz.timezone(tz_id)
        local_time = local_tz.localize(local_time)
        return local_time.astimezone(pytz.UTC)

    def _preprocessing_and_validation(
        self,
        data: dict,
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """
        1. Validate input dictionary (check if the schema of each record is valid)
        2. Seperate data into location, current temerature and forecast temperature
        3. Return transformed data as polar dataframe

        Args:
            data (dict): dictionary from the JSON input file

        Returns:
            location_df: DataFrame containing location information
            current_temp_df: DataFrame containing current temperature data
            forecast_data_df: DataFrame containing forecast information
        """
        location_data = []
        current_temp_data = []
        forecast_data = []
        for k, v in data.items():
            checked_data = self._validate_input_data(v)
            # Check if all required key present in the data
            name = checked_data.location.name
            region = checked_data.location.region
            country = checked_data.location.country
            created_date_local = checked_data.location.localtime
            created_date_utc = self._local_time_to_utc(
                created_date_local, checked_data.location.tz_id
            )

            location_data.append(checked_data.location.model_dump())
            current_temp = checked_data.current.model_dump()
            current_temp.update(
                {
                    "name": name,
                    "region": region,
                    "country": country,
                    "created_date_local": created_date_local,
                    "created_date_utc": created_date_utc,
                }
            )
            current_temp_data.append(current_temp)
            for fc_day in checked_data.forecast.forecastday:
                fc_day_data = fc_day.model_dump()
                fc_day_data.update(
                    {
                        "name": name,
                        "region": region,
                        "country": country,
                        "created_date_local": created_date_local,
                        "created_date_utc": created_date_utc,
                    }
                )
                forecast_data.append(fc_day_data)

        location_df = pl.DataFrame(location_data)
        current_temp_df = pl.DataFrame(current_temp_data).unnest("condition")
        forecast_data_df = pl.DataFrame(forecast_data).unnest("day").unnest("condition")
        return location_df, current_temp_df, forecast_data_df

    def _stats_cal(
        self, current_temp_df: pl.DataFrame, forecast_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Calculate temperature statistics by comparing current and forecast temperatures.

        Args:
            current_temp_df (pl.DataFrame): DataFrame containing current temperature data
            forecast_df (pl.DataFrame): DataFrame containing forecast temperature data

        Returns:
            Tuple[pl.DataFrame, pl.DataFrame]:
                - merged_df: Combined current and forecast data with calculated differences
                - stats_df: Aggregated statistics by location
        """
        # Convert the created_date_local column to date format in forecast dataframe
        forecast_df = forecast_df.with_columns(
            created_date_local=pl.col("created_date_local").dt.date()
        )
        # Convert last_updated to date format and rename it to data_date in current temperature dataframe
        current_temp_df = current_temp_df.with_columns(
            pl.col("last_updated").dt.date().alias("data_date")
        )
        # Merge forecast and current temperature dataframes based on location and date information
        merged_df = forecast_df.join(
            current_temp_df,
            how="left",
            left_on=["name", "region", "country", "created_date_local"],
            right_on=["name", "region", "country", "data_date"],
        )
        # Add calculated columns to the merged dataframe
        merged_df = merged_df.with_columns(
            [
                # First calculate day_diff
                ((pl.col("date") - pl.col("created_date_local")) / MS_PER_DAY)
                .cast(pl.Int32)
                .alias("day_diff")
            ]
        ).with_columns(
            [
                # Then calculate forecast_current_temp_diff only when day_diff = 0
                pl.when(pl.col("day_diff") == 0)
                .then(pl.col("temp_c") - pl.col("avgtemp_c"))
                .otherwise(None)  # or could use null
                .round(2)
                .alias("forecast_current_temp_diff")
            ]
        )
        # Select relevant columns and rename them for clarity
        merged_df = merged_df.select(
            pl.col(
                [
                    "name",
                    "region",
                    "country",
                    "created_date_local",
                    "created_date_utc",
                    "date",
                    "last_updated",
                    "temp_c",
                    "uv",
                    "avgtemp_c",
                    "maxtemp_c",
                    "mintemp_c",
                    "day_diff",
                    "forecast_current_temp_diff",
                    "daily_chance_of_rain",
                    "daily_chance_of_snow",
                    "maxwind_kph",
                ]
            )
        ).rename(
            {
                "date": "forecast_date",
                "last_updated": "current_temp_last_updated",
                "temp_c": "current_temp_c",
                "avgtemp_c": "forecast_avgtemp_c",
                "maxtemp_c": "forecast_maxtemp_c",
                "mintemp_c": "forecast_mintemp_c",
                "maxwind_kph": "forecast_maxwind_kph",
            }
        )
        # Create a new dataframe with selected columns for statistical analysis
        stats_df = merged_df.select(
            pl.col(
                [
                    "name",
                    "region",
                    "country",
                    "created_date_local",
                    "created_date_utc",
                    "forecast_date",
                    "current_temp_c",
                    "forecast_avgtemp_c",
                    "forecast_maxtemp_c",
                    "forecast_mintemp_c",
                    "day_diff",
                    "forecast_current_temp_diff",
                ]
            )
        )

        # Calculate aggregate statistics grouped by location
        stats_df = stats_df.group_by(["name", "region", "country"]).agg(
            [
                # Get the current temperature
                pl.col("current_temp_c").first(),
                # Find the minimum forecasted temperature
                pl.col("forecast_mintemp_c").min().alias("min_forecast"),
                # Find the maximum forecasted temperature
                pl.col("forecast_maxtemp_c").max().alias("max_forecast"),
                # Calculate the mean forecasted temperature
                pl.col("forecast_avgtemp_c").mean().round(2).alias("mean_forecast"),
                # Determine the date of the highest temperature
                # If current temp is highest, use created_date_local
                # Otherwise, use the date of the highest forecast
                pl.when(
                    pl.col("current_temp_c").first()
                    >= pl.col("forecast_maxtemp_c").max()
                )
                .then(pl.col("created_date_local").first())
                .otherwise(
                    pl.col("forecast_date")
                    .filter(
                        pl.col("forecast_maxtemp_c")
                        == pl.col("forecast_maxtemp_c").max()
                    )
                    .first()
                )
                .alias("highest_temp_date"),
            ]
        )
        # Add columns for absolute minimum and maximum temperatures
        # These consider both current and forecasted temperatures
        stats_df = stats_df.with_columns(
            pl.min_horizontal("min_forecast", "current_temp_c").alias("min_temp"),
            pl.max_horizontal("max_forecast", "current_temp_c").alias("max_temp"),
        )
        return merged_df, stats_df

    def _update_location(self, new_location_df: pl.DataFrame) -> pl.DataFrame:
        """Update location data - Insert overwrite"""
        existing_df = self._read_prev_data("location.csv")

        if existing_df is None:
            return new_location_df

        updated_df = existing_df.join(
            new_location_df, how="full", on=["name", "region", "country"], suffix="_new"
        )
        # For each column (except join keys), take the new value if available
        update_columns = [
            "name",
            "region",
            "country",
            "lat",
            "lon",
            "tz_id",
            "localtime_epoch",
            "localtime",
        ]
        coalesce_exprs = [
            pl.coalesce([pl.col(f"{col}_new"), pl.col(col)]).alias(col)
            for col in update_columns
        ]
        return updated_df.select(
            [
                *coalesce_exprs,  # lat, lon, tz_id, localtime_epoch, localtime
            ]
        )

    def _update_current_temp(self, new_current_df: pl.DataFrame) -> pl.DataFrame:
        """
        Update current temperature data using insert-overwrite strategy:
        - Update existing records with same location and date
        - Insert new records
        """
        existing_df = self._read_prev_data("current_temp.csv")

        if existing_df is None:
            return new_current_df

        existing_df = existing_df.with_columns(
            [
                pl.col("created_date_local").str.to_datetime(),
                pl.col("created_date_utc").str.to_datetime(),
                pl.col("last_updated").str.to_datetime(),
            ]
        )

        # Join on location and created_date_local to update matching records
        updated_df = existing_df.join(
            new_current_df,
            on=["name", "region", "country", "created_date_local"],
            how="full",
            suffix="_new",
        )

        # List of columns to update (excluding join keys)
        update_columns = [
            "name",
            "region",
            "country",
            "created_date_local",
            "created_date_utc",
            "last_updated",
            "temp_c",
            "temp_f",
            "is_day",
            "text",
            "code",
        ]

        # Create coalesce expressions for each column
        coalesce_exprs = [
            pl.coalesce([pl.col(f"{col}_new"), pl.col(col)]).alias(col)
            for col in update_columns
        ]

        # Select final columns in proper order
        return updated_df.select([*coalesce_exprs]).sort(
            ["name", "region", "country", "created_date_local"]
        )

    def _update_forecast(self, new_forecast_df: pl.DataFrame) -> pl.DataFrame:
        """
        Update forecast data using insert-overwrite strategy:
        - Update existing forecasts for same location, creation date, and forecast date
        - Insert new forecast records
        """
        existing_df = self._read_prev_data("forecast_temp.csv")

        if existing_df is None:
            return new_forecast_df
        existing_df = existing_df.with_columns(
            [
                pl.col("created_date_local").str.to_datetime(),
                pl.col("created_date_utc").str.to_datetime(),
                pl.col("date").str.to_date(),
            ]
        )
        # Join on location, creation date, and forecast date
        updated_df = existing_df.join(
            new_forecast_df,
            on=["name", "region", "country", "created_date_local", "date"],
            how="full",
            suffix="_new",
        )

        # List of columns to update (excluding join keys)
        update_columns = [
            "name",
            "region",
            "country",
            "created_date_local",
            "date",
            "created_date_utc",
            "date_epoch",
            "maxtemp_c",
            "maxtemp_f",
            "mintemp_c",
            "mintemp_f",
            "avgtemp_c",
            "avgtemp_f",
            "maxwind_mph",
            "maxwind_kph",
            "totalprecip_mm",
            "totalprecip_in",
            "totalsnow_cm",
            "avgvis_km",
            "avgvis_miles",
            "avghumidity",
            "daily_will_it_rain",
            "daily_chance_of_rain",
            "daily_will_it_snow",
            "daily_chance_of_snow",
            "text",
            "code",
            "uv",
        ]

        # Create coalesce expressions for each column
        coalesce_exprs = [
            pl.coalesce([pl.col(f"{col}_new"), pl.col(col)]).alias(col)
            for col in update_columns
        ]

        # Select final columns in proper order
        return updated_df.select(*coalesce_exprs).sort(
            ["name", "region", "country", "created_date_local", "date"]
        )

    def _update_merged(self, new_merged_df: pl.DataFrame) -> pl.DataFrame:
        """
        Update merged data using insert-overwrite strategy:
        - Update records for same location, creation date, and forecast date
        - Maintains historical forecast accuracy data
        """
        existing_df = self._read_prev_data("merged.csv")

        if existing_df is None:
            return new_merged_df

        # Convert dates if needed
        existing_df = existing_df.with_columns(
            [
                pl.col("created_date_local").str.to_date(),
                pl.col("created_date_utc").str.to_datetime(),
                pl.col("forecast_date").str.to_date(),
                pl.col("current_temp_last_updated").str.to_datetime(),
            ]
        )

        # Join on location, creation date, and forecast date
        updated_df = existing_df.join(
            new_merged_df,
            on=["name", "region", "country", "created_date_local", "forecast_date"],
            how="full",
            suffix="_new",
        )

        # List of columns to update (excluding join keys)
        update_columns = [
            "name",
            "region",
            "country",
            "created_date_local",
            "forecast_date",
            "created_date_utc",
            "current_temp_last_updated",
            "current_temp_c",
            "uv",
            "forecast_avgtemp_c",
            "forecast_maxtemp_c",
            "forecast_mintemp_c",
            "day_diff",
            "forecast_current_temp_diff",
            "daily_chance_of_rain",
            "daily_chance_of_snow",
            "forecast_maxwind_kph",
        ]

        # Create coalesce expressions for each column
        coalesce_exprs = [
            pl.coalesce([pl.col(f"{col}_new"), pl.col(col)]).alias(col)
            for col in update_columns
        ]

        # Select final columns in proper order and sort
        updated_df = updated_df.select(
            [
                *coalesce_exprs,
            ]
        ).sort(["name", "region", "country", "created_date_local", "forecast_date"])

        # Recalculate day_diff and forecast_current_temp_diff
        updated_df = updated_df.with_columns(
            [
                # First calculate day_diff
                ((pl.col("forecast_date") - pl.col("created_date_local")) / MS_PER_DAY)
                .cast(pl.Int32)
                .alias("day_diff")
            ]
        ).with_columns(
            [
                # Then calculate forecast_current_temp_diff only when day_diff = 0
                pl.when(pl.col("day_diff") == 0)
                .then(pl.col("current_temp_c") - pl.col("forecast_avgtemp_c"))
                .otherwise(None)  # or could use null
                .round(2)
                .alias("forecast_current_temp_diff")
            ]
        )

        return updated_df

    def _update_stats(self, new_stats_df: pl.DataFrame) -> pl.DataFrame:
        """
        Update statistics using insert-overwrite strategy:
        - Update stats for same location and date
        - Maintains historical stats
        """
        existing_df = self._read_prev_data("stats.csv")

        if existing_df is None:
            return new_stats_df

        # Convert dates if needed
        existing_df = existing_df.with_columns(
            [
                pl.col("highest_temp_date").str.to_date(),
            ]
        )

        # Join on location and created_date_local
        updated_df = existing_df.join(
            new_stats_df,
            on=["name", "region", "country"],
            how="full",
            suffix="_new",
        )

        # List of columns to update (excluding join keys)
        update_columns = [
            "current_temp_c",
            "min_forecast",
            "max_forecast",
            "mean_forecast",
            "highest_temp_date",
            "min_temp",
            "max_temp",
        ]

        # Create coalesce expressions for each column
        coalesce_exprs = [
            pl.coalesce([pl.col(f"{col}_new"), pl.col(col)]).alias(col)
            for col in update_columns
        ]

        # Select final columns in proper order
        return updated_df.select(["name", "region", "country", *coalesce_exprs]).sort(
            ["name", "region", "country"]
        )

    def _save_files(
        self,
        location: pl.DataFrame,
        current: pl.DataFrame,
        forecast: pl.DataFrame,
        merged: pl.DataFrame,
        stats: pl.DataFrame,
    ) -> None:
        try:
            os.makedirs("./output", exist_ok=True)

            with pl.Config(float_precision=2):
                location.write_csv("./output/location.csv")
                current.write_csv("./output/current_temp.csv")
                forecast.write_csv("./output/forecast_temp.csv")
                merged.write_csv("./output/merged.csv")
                stats.write_csv("./output/stats.csv")
            self.logger.info("All files saved successfully")
        except Exception as e:
            self.logger.error(f"Error while saving data: {e}")
            raise

    def run(self, input_file: str) -> None:
        """
        The pipeline will follow this process:
        1. Read CSV
        2. Validate and transform data into df
        3. Calculate stats
        4. Save as CSV files

        Args:
            input_file (str): path to source JSON file
        """

        # 1. Read Json input
        data = self._read_json(input_file)
        self.logger.info("Input file read successfully!")
        self.logger.info("Validation data")

        # 2. Validate and transform data into df
        location_df, current_temp_df, forecast_data_df = (
            self._preprocessing_and_validation(data)
        )

        # 3. Calculate stats
        self.logger.info("Cooking")
        merged_df, stats_df = self._stats_cal(current_temp_df, forecast_data_df)
        location_df = self._update_location(location_df)
        current_temp_df = self._update_current_temp(current_temp_df)
        forecast_data_df = self._update_forecast(forecast_data_df)
        merged_df = self._update_merged(merged_df)
        stats_df = self._update_stats(stats_df)
        # 4. Save as CSV files
        self._save_files(
            location_df, current_temp_df, forecast_data_df, merged_df, stats_df
        )


def main(args) -> None:
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [WeatherETL] - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler("weather_process.log"),  # Log to file
        ],
    )
    wdp = WeatherDataProcess()
    # Default value for input file name
    default_input_file = "ETL_developer_Case.json"

    # check if user define input or not, if not use default
    input_file = default_input_file if not args.input else args.input

    # Run the pipeline
    logger.info("The pipeline is running")
    logger.info(f"Input file: {input_file}")
    wdp.run(input_file)


if __name__ == "__main__":
    # Create parser
    parser = argparse.ArgumentParser(
        description=(
            "ETL pipeline for processing weather forecast data. This script: "
            "1) Reads weather data from a JSON file containing location, current temperature, and forecast information. "
            "2) Validates the input data structure using Pydantic models. "
            "3) Transforms the data into structured DataFrames. "
            "4) Calculates various statistics including temperature differences and forecast accuracy. "
            "5) Outputs five CSV files (location.csv, current_temp.csv, forecast_temp.csv, merged.csv, stats.csv). "
            "If no input file is specified, defaults to 'ETL_developer_Case.json'."
        )
    )

    # Add arguments
    parser.add_argument(
        "-i",
        "--input",
        help="Path to the input JSON file containing weather data. Should include location, current, and forecast fields.",
    )

    # Parse arguments
    args = parser.parse_args()

    # Run main with parsed arguments
    main(args)
