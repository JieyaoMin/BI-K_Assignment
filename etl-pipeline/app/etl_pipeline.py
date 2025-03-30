import pandas as pd
import psycopg2
from psycopg2 import sql
# from datetime import datetime
from datetime import date
from app.logger import setup_logger
import os
from app.config import settings


class ETLError(Exception):
    pass

class ETLPipeline:
    def __init__(self, csv_file='sample_data.csv'):
        self.logger = setup_logger('etl_pipeline')
        self.df = None
        self.cleaned_df = None
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.csv_path = os.path.join(self.base_dir, "data", csv_file)

        # Database connection parameters set in docker-compose.yml (otherwise with default)
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'etl_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'port': os.getenv('DB_PORT', '5432')
        }

    def extract(self):
        # Extract data from CSV file
        try:
            self.logger.info(f"Extracting data from {self.csv_path}")
            self.df = pd.read_csv(self.csv_path)
            self.logger.info(f"Successfully extracted {len(self.df)} records")
            return True
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise ETLError(f"Extraction failed: {str(e)}")

    def transform(self):
        # Transform and clean the data
        if self.df is None:
            self.logger.error("No data to transform.")
            raise ETLError("No data to transform.")

        try:
            self.logger.info("Starting data transformation")
            self.cleaned_df = self.df.copy()
            
            # Standardize date formats
            self._standardize_dates()
            
            # Handle missing values
            self._handle_missing_values()
            
            # Remove duplicates
            self._remove_duplicates()
            
            # Validate data types
            self._validate_data_types()

            # Handle missing values again after column type conversion
            self._handle_missing_values()
            
            self.logger.info(f"Transformation complete. {len(self.cleaned_df)} clean records remaining")
            return True
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise ETLError(f"Transformation failed: {str(e)}")

    def _standardize_dates(self):
        # Convert various date formats to YYYY-MM-DD
        # Assumption: the date columns having "date" (case insensitive) in column name
        date_columns = [col for col in self.cleaned_df.columns if 'date' in col.lower()]
        
        for col in date_columns:
            try:
                self.cleaned_df[col] = pd.to_datetime(
                    self.cleaned_df[col],
                    errors='coerce',
                    # infer_datetime_format=True
                ).dt.strftime('%Y-%m-%d')
                
                # Log how many dates couldn't be parsed
                na_count = self.cleaned_df[col].isna().sum()
                if na_count > 0:
                    self.logger.warning(
                        f"There are {na_count} values could not parse in column '{col}', and would be handled as missing values."
                    )
            except Exception as e:
                self.logger.warning(f"Error standardizing dates in column '{col}': {str(e)}")

    def _handle_missing_values(self):
        # Handle missing values based on column type
        for col in self.cleaned_df.columns:
            # Skip if no missing values
            if not self.cleaned_df[col].isna().any():
                continue
                
            na_count = self.cleaned_df[col].isna().sum()
            
            # Numeric columns: fill with median here
            if pd.api.types.is_numeric_dtype(self.cleaned_df[col]):
                median = self.cleaned_df[col].median()
                self.cleaned_df[col].fillna(median, inplace=True)
                self.logger.info(
                    f"Filled {na_count} missing values in numeric column '{col}' with median: {median}"
                )
            # Date columns: fill with a specific date (today) here
            elif 'date' in col.lower():
                specific_date = date.today()
                self.cleaned_df[col].fillna(specific_date, inplace=True)
                self.logger.info(
                    f"Column '{col}' has {na_count} missing date values. Setting as today."
                )
            # Categorical or text columns: fill with mode or 'Unknown'
            else:
                mode = self.cleaned_df[col].mode()[0] if not self.cleaned_df[col].mode().empty else 'Unknown'
                self.cleaned_df[col].fillna(mode, inplace=True)
                self.logger.info(
                    f"Filled {na_count} missing values in categorical column '{col}' with mode: '{mode}'"
                )

    def _remove_duplicates(self):
        # Remove duplicate records
        initial_count = len(self.cleaned_df)
        self.cleaned_df.drop_duplicates(inplace=True)
        duplicates_removed = initial_count - len(self.cleaned_df)
        
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate records")

    def _validate_data_types(self):
    # Validate and convert data types based on the majority votes
        for col in self.cleaned_df.columns:
            # Skip if no values to analyze
            if len(self.cleaned_df[col].dropna()) == 0:
                self.logger.warning(f"Column '{col}' is empty - cannot determine data type")
                continue

            # Get sample (<100) of non-null values for analysis
            sample_values = self.cleaned_df[col].dropna().sample(min(100, len(self.cleaned_df[col])), random_state=42)
            
            # Determine the most appropriate type based on values
            detected_type = self._detect_column_type(sample_values)
            
            # Convert column to the detected type
            self._convert_column_type(col, detected_type)

    def _detect_column_type(self, sample_series):
        # Determine the most appropriate data type for a column based on sample values
        # Numeric
        if self._is_numeric(sample_series):
            # if self._is_integer(sample_series):
            #     return 'integer'
            return 'float'
        
        # Datetime
        if self._is_datetime(sample_series):
            return 'datetime'
        
        # Try to detect boolean
        if self._is_boolean(sample_series):
            return 'boolean'
        
        # Default to string
        return 'string'

    def _is_numeric(self, sample_series):
        # Check if majority of values can be converted to numeric
        numeric_count = 0
        for val in sample_series:
            try:
                float(val)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        return numeric_count / len(sample_series) > 0.5  # threshold

    # def _is_integer(self, sample_series):
    #     # Check if majority of numeric values are integers
    #     int_count = 0
    #     numeric_count = 0
    #     for val in sample_series:
    #         try:
    #             float_val = float(val)
    #             numeric_count += 1
    #             if float_val.is_integer():
    #                 int_count += 1
    #         except (ValueError, TypeError):
    #             pass
        
    #     if numeric_count == 0:
    #         return False
    #     return int_count / numeric_count > 0.7 # higher threshold, for the float

    def _is_datetime(self, sample_series):
        # Check if majority of values can be parsed as dates
        date_count = 0
        for val in sample_series:
            try:
                pd.to_datetime(val, errors='raise')
                date_count += 1
            except (ValueError, TypeError):
                pass
        return date_count / len(sample_series) > 0.5

    def _is_boolean(self, sample_series):
        # Check if majority of values can be interpreted as boolean.
        bool_count = 0
        true_values = {'true', 'yes', 'y', '1', 1, True}
        false_values = {'false', 'no', 'n', '0', 0, False}
        
        for val in sample_series:
            if isinstance(val, str):
                val = val.lower().strip()
            if val in true_values or val in false_values:
                bool_count += 1
        return bool_count / len(sample_series) > 0.5

    def _convert_column_type(self, col_name, target_type):
        # Convert column to the specified type
        original_type = str(self.cleaned_df[col_name].dtype)
        
        try:
            if target_type == 'integer':
                self.cleaned_df[col_name] = pd.to_numeric(self.cleaned_df[col_name], errors='coerce').astype('Int64')
            elif target_type == 'float':
                self.cleaned_df[col_name] = pd.to_numeric(self.cleaned_df[col_name], errors='coerce')
            elif target_type == 'datetime':
                self.cleaned_df[col_name] = pd.to_datetime(self.cleaned_df[col_name], errors='coerce')
            elif target_type == 'boolean':
                self.cleaned_df[col_name] = self.cleaned_df[col_name].apply(
                    lambda x: True if str(x).lower().strip() in {'true', 'yes', 'y', '1'} else (
                        False if str(x).lower().strip() in {'false', 'no', 'n', '0'} else pd.NA
                    )
                ).astype('boolean')
            else:  # string
                self.cleaned_df[col_name] = self.cleaned_df[col_name].astype('string')
            
            new_type = str(self.cleaned_df[col_name].dtype)
            self.logger.info(f"Converted column '{col_name}' from {original_type} to {new_type}")
                
        except Exception as e:
            self.logger.error(f"Failed to convert column '{col_name}' to {target_type}: {str(e)}")


    def load(self, table_name='clean_data'):
        # Load cleaned data into PostgreSQL
        if self.cleaned_df is None:
            self.logger.error("No cleaned data to load.")
            raise ETLError("No cleaned data to load.")

        try:
            self.logger.info(f"Loading data to PostgreSQL table '{table_name}'")
            
            # Connect to PostgreSQL
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cursor:
                    # Create table if it doesn't exist
                    self._create_table(cursor, table_name)
                    
                    # Prepare data for insertion
                    columns = self.cleaned_df.columns.tolist()
                    values = [tuple(x) for x in self.cleaned_df.to_numpy()]
                    
                    # Build the INSERT SQL statement
                    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        sql.SQL(', ').join([sql.Placeholder()] * len(columns))
                    )
                    
                    # Execute the INSERT statement
                    cursor.executemany(insert_sql, values)
                    conn.commit()
                    
                    self.logger.info(f"Successfully loaded {len(self.cleaned_df)} records into {table_name}")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Loading failed: {str(e)}")
            raise ETLError(f"Loading failed: {str(e)}")

    def _create_table(self, cursor, table_name):
        # Create the table with appropriate column types
        drop_table_sql = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(table_name)
        )
        cursor.execute(drop_table_sql)
        
        # Generate CREATE TABLE SQL
        columns_with_types = []
        
        for col in self.cleaned_df.columns:
            # Determine PostgreSQL data type based on pandas dtype
            if pd.api.types.is_numeric_dtype(self.cleaned_df[col]):
                pg_type = 'NUMERIC'
            elif pd.api.types.is_datetime64_any_dtype(self.cleaned_df[col]):
                pg_type = 'DATE'
            else:
                # For strings and other types, use TEXT
                pg_type = 'TEXT'
            
            columns_with_types.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(col),
                    sql.SQL(pg_type)
                )
            )
        
        create_table_sql = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(columns_with_types)
        )
        
        cursor.execute(create_table_sql)

    def run_pipeline(self, table_name='clean_data'):
        # Run the complete ETL pipeline
        try:
            self.extract()
            self.transform()
            self.load(table_name)
            self.logger.info("ETL pipeline completed successfully")
            return True
        except ETLError as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            return False


if __name__ == "__main__":
    pipeline = ETLPipeline(settings['CSV_FILE'])
    pipeline.run_pipeline(settings['TABLE_NAME'])
