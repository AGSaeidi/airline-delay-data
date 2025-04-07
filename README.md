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

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your system

### Running the Application

```bash
# Start all services
docker compose up

# Run in detached mode
docker compose up -d

# View logs
docker compose logs

# Stop all services
docker compose down
