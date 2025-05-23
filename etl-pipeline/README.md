# ETL Pipeline with Data Quality Controls

## Features

- Data extraction from a CSV file (etl-pipeline/data)
- Data transformation including:
  - Date format standardization (Assumption: column name including case insensitive "date", 
                                 e.g., "admission_date" in data/sample_data.csv)
  - Missing value handling
    - Numeric columns: fill with median (e.g., "sum_assured" and "age" in data/sample_data.csv))
    - Date columns: fill with specific date (today, e.g., "admission_date" in data/sample_data.csv)
    - Categorical or text columns: fill with mode or 'Unknown' (e.g., "gender" and "department" in data/sample_data.csv, although it's not appropriate in this context)
  - Duplicate removal (e.g., John Doe records in data/sample_data.csv)
  - Data type validation (majority vote convert column type, do the missing value handling again after the conversion)
- Data loading to PostgreSQL
- Logging (etl-pipeline/logs)
- Dockerized setup


## Prerequisites

- Docker
- Docker Compose

## Dependencies

- requierements.txt


## Setup Instructions

1. Clone this repository:
   ```bash
   git clone https://github.com/JieyaoMin/BI-K_Assignment.git
   cd BI-K_Assignment/etl-pipeline

2. Update app/config.py and docker-compose.yml if necessary

3. Build and run the containers:
   ```bash
   docker-compose up --build

4. The ETL pipeline will automatically process the sample data in data/sample_data.csv and load it into PostgreSQL.


## Test Instructions 

1. New another terminal

2. Check the container id
   ```bash
   docker ps

3. Access the PostgreSQL Container
   ```bash
   docker exec -it <container_id> psql -U postgres -d etl_db

4. Check loaded data
   ```bash
   SELECT * FROM clean_data;
