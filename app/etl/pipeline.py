import polars as pl
from pypdf import PdfReader
from pathlib import Path
from datetime import date
from typing import List, Type, Optional
from sqlmodel import Session
from app.data_access.database import engine

class DataExtractor:
    """
    Handles data ingestion from various source formats in the Bronze layer.
    
    This class supports structured data (CSV, JSON) and unstructured data (PDF)
    as required by the technical challenge specifications.
    """

    @staticmethod
    def read_csv(file_path: Path) -> pl.DataFrame:
        """
        Reads a CSV file into a Polars DataFrame.

        Args:
            file_path (Path): Path to the source CSV file.

        Returns:
            pl.DataFrame: The loaded data.
        """
        return pl.read_csv(file_path)

    @staticmethod
    def read_json(file_path: Path) -> pl.DataFrame:
        """
        Reads a JSON file into a Polars DataFrame.

        Args:
            file_path (Path): Path to the source JSON file.

        Returns:
            pl.DataFrame: The loaded data.
        """
        return pl.read_json(file_path)

    @staticmethod
    def extract_pdf_text(file_path: Path) -> str:
        """
        Extracts raw text content from a PDF file for unstructured data processing.

        Args:
            file_path (Path): Path to the PDF file.

        Returns:
            str: The extracted text content. Returns an empty string if extraction fails.
        """
        try:
            reader = PdfReader(file_path)
            return "".join([page.extract_text() or "" for page in reader.pages])
        except Exception as e:
            print(f"Error reading PDF {file_path}: {e}")
            return ""

class DataTransformer:
    """
    Implements core ETL transformations using the Polars framework.
    
    This class handles the 'Silver Layer' logic, ensuring that raw Bronze data
    is cleaned, typed correctly for SQLite, and resolved against existing dimensions.
    """

    @staticmethod
    def clean_orders(df: pl.DataFrame) -> pl.DataFrame:
        """
        Transforms raw order data by converting date strings to Python date objects.

        Args:
            df (pl.DataFrame): The raw orders dataframe from the Bronze layer.

        Returns:
            pl.DataFrame: The cleaned dataframe with proper date types for SQLite.
        """
        return df.with_columns(pl.col("order_date").str.to_date())

    @staticmethod
    def clean_sales_people(df: pl.DataFrame) -> pl.DataFrame:
        """
        Transforms raw salesperson data by converting hire_date strings to date objects.

        Args:
            df (pl.DataFrame): The raw sales people dataframe.

        Returns:
            pl.DataFrame: The cleaned dataframe.
        """
        return df.with_columns(pl.col("hire_date").str.to_date())

    @staticmethod
    def resolve_products(products_df: pl.DataFrame, categories_df: pl.DataFrame) -> pl.DataFrame:
        """
        Resolves the relationship between Products and Categories.

        Maps the 'category_name' in the product data to the 'id' of the 
        Category dimension to maintain Star Schema integrity.

        Args:
            products_df (pl.DataFrame): Raw product data.
            categories_df (pl.DataFrame): Category dimension data retrieved from SQL.

        Returns:
            pl.DataFrame: Resolved products containing the surrogate 'category_id'.
        """
        return (
            products_df.join(categories_df, left_on="category_name", right_on="name")
            .rename({"id": "category_id"})
            .select(["sku", "name", "description", "price", "category_id", "category_name"])
        )

class DateDimensionGenerator:
    """
    Utility for generating a comprehensive Calendar Table (Date Dimension).
    """

    @staticmethod
    def generate_range(start_year: int, end_year: int) -> pl.DataFrame:
        """
        Generates a DimDate DataFrame with all required analytical columns.

        Fulfills the 'Calendar Table' requirement by providing fields for 
        year, month, day, quarter, and weekend flags.

        Args:
            start_year (int): The starting year for the calendar.
            end_year (int): The ending year for the calendar.

        Returns:
            pl.DataFrame: A complete date dimension table.
        """
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        
        df = pl.date_range(
            start_date, 
            end_date, 
            interval="1d", 
            eager=True
        ).alias("date_obj").to_frame()

        return df.with_columns([
            pl.col("date_obj").alias("full_date"),
            pl.col("date_obj").dt.year().alias("year"),
            pl.col("date_obj").dt.month().alias("month"),
            pl.col("date_obj").dt.strftime("%B").alias("month_name"),
            pl.col("date_obj").dt.day().alias("day"),
            pl.col("date_obj").dt.weekday().alias("day_of_week"),
            pl.col("date_obj").dt.strftime("%A").alias("day_name"),
            pl.col("date_obj").dt.quarter().alias("quarter"),
            pl.col("date_obj").dt.weekday().is_in([6, 7]).alias("is_weekend")
        ]).drop("date_obj")

class DataLoader:
    """
    Handles the 'Load' phase of the ETL process.
    """

    @staticmethod
    def load_to_sql(df: pl.DataFrame, model_class: Type):
        """
        Persists a Polars DataFrame into the SQL Gold Layer using SQLModel.

        Uses bulk insertion for optimized performance during the seeding process.

        Args:
            df (pl.DataFrame): The transformed data to load.
            model_class (Type): The SQLModel class representing the target table.
        """
        records = df.to_dicts()
        if not records:
            return
            
        with Session(engine) as session:
            # Efficiently add all records in a single transaction
            session.add_all([model_class(**rec) for rec in records])
            session.commit()