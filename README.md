# Airline Delay Data - Batch Processing Pipeline

This project implements a batch processing pipeline for airline delay data using Docker Compose to orchestrate multiple microservices.

## Architecture

The application consists of 5 microservices, each handling a specific part of the data pipeline:

1. **Extraction Service**: Extracts raw data from source systems
2. **Transformation Service**: Transforms the raw data into a standardized format
3. **Validation Service**: Validates the transformed data for quality and consistency
4. **Aggregation Service**: Aggregates the validated data for analysis
5. **Loading Service**: Loads the processed data into the target storage system

## Technologies

- **Docker Compose**: Orchestrates the multi-container application [Docker Compose](https://docs.docker.com/compose/)
- **Python**: Used for implementing the data processing logic in each microservice
  
## Prerequisites

- Docker and Docker Compose installed on your system
- At least 4GB of RAM available for Docker
- Airline delay dataset CSV file with columns:
  - FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM, ORIGIN, DEST, CRS_DEP_TIME, DEP_TIME, 
    DEP_DELAY, TAXI_OUT, WHEELS_OFF, WHEELS_ON, TAXI_IN, CRS_ARR_TIME, ARR_TIME, 
    ARR_DELAY, CANCELLED, CANCELLATION_CODE, DIVERTED, CRS_ELAPSED_TIME, 
    ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE, CARRIER_DELAY, WEATHER_DELAY, 
    NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY
    
## Getting Started

1. Clone the repository
2. Place your CSV file in the `data/input/` directory
3. Build and run the Docker containers
4. Check the results in the `data/output/` directory

## Output

The pipeline generates several outputs:

- CSV files with statistical analysis by carrier, origin, and destination
- Visualizations of delay patterns
- A summary report with key findings
  
## Customization

You can modify the processing parameters by editing the Python files in each service directory:

- Change chunk size in `extractor/app.py` based on your system's memory
- Adjust validation rules in `validator/app.py`
- Modify aggregation metrics in `aggregator/app.py`
- Change visualization types in `loader/app.py`

 ## Troubleshooting

- If the process fails due to memory issues, try reducing the chunk size in `extractor/app.py`
- Check the Docker logs for detailed error messages: `docker compose logs`
- Ensure your CSV file has the expected column names
  
 ## Acknowledgments

- This project uses Docker Compose for orchestrating the microservices
- Data processing is performed using pandas and numpy
- Visualizations are created with matplotlib
  
 ## Running the Application

```bash
# Start all services
docker compose up

# Run in detached mode
docker compose up -d

# View logs
docker compose logs

# Stop all services
docker compose down
