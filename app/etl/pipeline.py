import io
from datetime import date
from pathlib import Path

import polars as pl
from pypdf import PdfReader
from sqlmodel import Session, SQLModel

from app.data_access.database import engine


class DataExtractor:
    """Handles data ingestion from various source formats in the Bronze layer.
    
    This class supports structured data (CSV, JSON) and unstructured data (PDF)
    as required by the technical challenge specifications.
    """

    @staticmethod
    def read_csv(file_path: Path) -> pl.DataFrame:
        """Reads a CSV file into a Polars DataFrame."""
        return pl.read_csv(file_path)

    @staticmethod
    def read_json(file_path: Path) -> pl.DataFrame:
        """Reads a JSON file into a Polars DataFrame."""
        return pl.read_json(file_path)

    @staticmethod
    def extract_pdf_text(file_path: Path) -> str:
        """Extracts raw text content from a PDF file for unstructured data processing."""
        try:
            reader = PdfReader(file_path)
            return "".join([page.extract_text() or "" for page in reader.pages])
        except Exception as e:
            print(f"Error reading PDF {file_path}: {e}")
            return ""

    @staticmethod
    def convert_text_to_df(raw_text: str, header_identifier: str) -> pl.DataFrame:
        """Extracts a CSV-like block from raw text and converts it to a Polars DataFrame.
        Allows treating unstructured PDF content as a structured source in-memory.
        """
        try:
            start_index = raw_text.find(header_identifier)
            if start_index == -1:
                return pl.DataFrame()

            csv_content = raw_text[start_index:].strip()
            return pl.read_csv(io.StringIO(csv_content))
        except Exception as e:
            print(f"Error converting text to DataFrame: {e}")
            return pl.DataFrame()

class DataTransformer:
    """Implements core ETL transformations using the Polars framework.
    
    Handles the 'Silver Layer' logic, ensuring that raw Bronze data
    is cleaned and typed correctly for the Cloud Asset Star Schema.
    """

    @staticmethod
    def clean_entities(df: pl.DataFrame) -> pl.DataFrame:
        """Transforms raw entity data by converting date strings to Python date objects.

        Args:
            df (pl.DataFrame): The raw entity dataframe from the Bronze layer.

        Returns:
            pl.DataFrame: The cleaned dataframe with proper date types.
        """
        return df.with_columns(pl.col("created_at").str.to_date())

    @staticmethod
    def clean_metrics(df: pl.DataFrame) -> pl.DataFrame:
        """Ensures metric data types are correct before loading to SQL."""
        return df.with_columns([
            pl.col("cpu_usage_avg").cast(pl.Float64),
            pl.col("hourly_cost").cast(pl.Float64)
        ])

class DateDimensionGenerator:
    """Utility for generating a comprehensive Calendar Table (Date Dimension).
    Fulfills Optional Feature #1.
    """

    @staticmethod
    def generate_range(start_year: int, end_year: int) -> pl.DataFrame:
        """Generates a DimDate DataFrame with Smart IDs (YYYYMMDD)
        and all required analytical columns.
        """
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)

        # 1. Create the base date range
        df = pl.date_range(
            start_date,
            end_date,
            interval="1d",
            eager=True
        ).alias("date_obj").to_frame()

        # 2. Generate columns including the Smart ID
        return df.with_columns([
            # ID: 2023-01-01 -> 20230101
            pl.col("date_obj").dt.strftime("%Y%m%d").cast(pl.Int32).alias("id"),

            pl.col("date_obj").alias("full_date"),
            pl.col("date_obj").dt.year().alias("year"),
            pl.col("date_obj").dt.month().alias("month"),
            pl.col("date_obj").dt.strftime("%B").alias("month_name"),
            pl.col("date_obj").dt.day().alias("day"),
            pl.col("date_obj").dt.weekday().alias("day_of_week"),
            pl.col("date_obj").dt.strftime("%A").alias("day_name"),
            pl.col("date_obj").dt.quarter().alias("quarter"),
            pl.col("date_obj").dt.weekday().is_in([6, 7]).alias("is_weekend")
        ]).drop("date_obj").select([
            "id", "full_date", "year", "month", "month_name",
            "day", "day_of_week", "day_name", "quarter", "is_weekend"
        ])

class DataLoader:
    """Handles the 'Load' phase of the ETL process."""

    @staticmethod
    def load_to_sql(df: pl.DataFrame, model_class: type[SQLModel]) -> None:
        """Persists a Polars DataFrame into the SQL Warehouse using SQLModel.

        Args:
            df (pl.DataFrame): The transformed data to load.
            model_class (Type[SQLModel]): The SQLModel class representing the target table.
        
        Returns:
            None
        """
        records = df.to_dicts()
        if not records:
            return

        with Session(engine) as session:
            # Efficiently add all records in a single transaction
            session.add_all([model_class(**rec) for rec in records])
            session.commit()
