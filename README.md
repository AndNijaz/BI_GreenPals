# GreenPals IoT Data Engineering Pipeline

## Overview

This project implements a comprehensive data engineering pipeline for the GreenPals IoT ecosystem, designed to process and analyze energy consumption data from smart plugs across both residential and enterprise environments. The system demonstrates modern data warehouse architecture principles with Apache Airflow orchestration, implementing medallion architecture (Bronze-Silver-Gold layers) and dimensional modeling techniques.

**University Project** - Business Intelligence & Data Warehousing  
**Authors**: ETL Team  
**Technologies**: Apache Airflow, PostgreSQL, Docker, Python, SQL

## Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Operational   │    │   Analytical    │    │   External APIs │
│    Database     │───▶│    Database     │◀───│  (CO2, Prices) │
│  (db_operational)│    │ (db_analytical) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ Apache Airflow  │
                    │   Orchestration │
                    └─────────────────┘
```

### Data Layer Architecture

1. **Bronze Layer (Landing)**: Raw data ingestion from operational systems and APIs
2. **Silver Layer (Cleaned + Archive)**: Data validation, cleaning, and SCD2 historical tracking
3. **Gold Layer (Star Schema)**: Dimensional model optimized for analytics and reporting

### Database Schemas

#### Operational Database (`db_operational`)

- **public**: Individual user data (users, smart_plugs, readings, etc.)
- **company_schema**: Enterprise data (companies, departments, company_users, etc.)

#### Analytical Database (`db_analytical`)

- **landing**: Raw data staging area
- **cleaned**: Validated and cleaned data
- **archive**: Historical data with SCD2 implementation
- **archive_cleaned**: Historical cleaned data
- **public**: Star schema (dimensions and fact tables)

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Git

### Setup Instructions

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd BI_GreenPals
   ```

2. **Start the environment**

   ```bash
   docker-compose up -d
   ```

3. **Access services**

   - **Airflow WebUI**: http://localhost:8080 (admin/admin)
   - **Operational DB**: localhost:5433 (postgres/napoleonlm10)
   - **Analytical DB**: localhost:5434 (postgres/napoleonlm10)

4. **Initialize data**
   - Operational database will auto-populate from CSV files in `/data`
   - Analytical database schemas will be created automatically

## ETL Pipeline Overview

### Airflow DAGs

| DAG Name                | Purpose                                      | Schedule       | Status    |
| ----------------------- | -------------------------------------------- | -------------- | --------- |
| `star_schema_etl`       | Create and populate star schema (Gold layer) | [PLACEHOLDER]  | Manual    |
| `full_scd2_etl`         | Full load from operational to analytical DB  | [PLACEHOLDER]  | Manual    |
| `incremental_scd2_etl`  | Incremental load with SCD2 updates           | [PLACEHOLDER]  | Manual    |
| `total_incremental_etl` | End-to-end incremental pipeline              | `*/15 * * * *` | Scheduled |
| `etl_full_to_cleaned`   | Landing to cleaned layer transformation      | [PLACEHOLDER]  | Manual    |
| `populate_cleaned`      | Populate cleaned from archive_raw            | [PLACEHOLDER]  | Manual    |
| `api_to_landing`        | External API data ingestion                  | `0 * * * *`    | Scheduled |

### Data Flow

```
Operational DB ──┐
                 ├─▶ Landing Layer ──▶ Archive Layer ──▶ Cleaned Layer ──▶ Star Schema
External APIs ───┘                     (SCD2)           (Quality)        (Analytics)
```

### ETL Process Details

1. **Data Ingestion**

   - **Full Load**: Complete data refresh from operational database
   - **Incremental Load**: Delta changes based on `updated_at` timestamps
   - **API Integration**: External CO2 factors and electricity prices

2. **Data Quality & Transformation**

   - Data validation and cleaning in the Silver layer
   - SCD2 (Slowly Changing Dimensions Type 2) for historical tracking
   - Referential integrity maintenance

3. **Analytics Layer**
   - Star schema with fact and dimension tables
   - Optimized for OLAP queries and reporting
   - Support for time-based analysis and trend identification

## Directory Structure

```
├── api/                          # External API services
│   ├── co2_factors_api.py       # CO2 emission factors API
│   └── electricity_prices_api.py # Electricity pricing API
├── dags/                        # Airflow DAG definitions
│   ├── star_schema_etl.py      # Star schema ETL pipeline
│   ├── full_scd2_etl.py        # Full load ETL process
│   ├── incremental_scd2_etl.py # Incremental ETL process
│   ├── api_to_landing_dag.py   # API data ingestion
│   ├── utils/                  # Utility functions
│   └── star-schema/            # Star schema SQL scripts
├── data/                       # Initial data files (CSV)
├── init-scripts/              # Operational DB initialization
├── init-scripts-analytical/   # Analytical DB initialization
├── docker-compose.yaml        # Infrastructure definition
└── README.md                  # This file
```

## Key Features

### 1. Multi-Tenant Architecture

- **Residential Users**: Individual smart plug monitoring
- **Enterprise Clients**: Company-wide energy management with departments

### 2. Historical Data Tracking

- SCD2 implementation for tracking changes over time
- Maintains data lineage and audit trails
- Supports temporal queries and trend analysis

### 3. External Data Integration

- **CO2 Emission Factors**: Environmental impact calculations
- **Electricity Prices**: Cost analysis and optimization

### 4. Scalable ETL Architecture

- Configurable full/incremental load strategies
- Parallel processing capabilities
- Error handling and retry mechanisms

### 5. Analytics-Ready Data Model

- Star schema optimized for reporting
- Pre-aggregated metrics for performance
- Support for drill-down and slice-and-dice operations

## Data Model

### Fact Tables

- `fact_readings`: Energy consumption measurements
- `fact_company_readings`: Enterprise energy consumption
- `fact_plug_assignment`: Device-room assignments
- `fact_device_events`: Device lifecycle events

### Dimension Tables

- `dim_time`: Time dimension with various granularities
- `dim_user`: User information with SCD2
- `dim_location`: Geographic locations
- `dim_room`: Room details and hierarchy
- `dim_device`: Device catalog and specifications
- `dim_company`: Company information
- `dim_department`: Organizational structure

## Development Guidelines

### Adding New DAGs

1. Create DAG file in `/dags` directory
2. Add comprehensive docstring with scheduling placeholder
3. Implement proper error handling and logging
4. Test with manual trigger before scheduling

### Database Changes

1. Update initialization scripts in appropriate directory
2. Create migration scripts if needed
3. Update data model documentation

### API Integration

1. Add new API endpoints in `/api` directory
2. Update landing layer schema
3. Modify ingestion DAGs accordingly

## Monitoring & Maintenance

### Health Checks

- Database connectivity monitoring
- Airflow task success/failure tracking
- Data quality validation

### Performance Optimization

- Index optimization for analytical queries
- Partition strategies for large fact tables
- ETL performance tuning

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify database services are running
2. **Memory Issues**: Adjust Docker resource allocation
3. **Data Consistency**: Check ETL execution order and dependencies

### Logs Location

- Airflow logs: `/logs` directory
- Database logs: Docker container logs

## Future Enhancements

- [ ] Real-time streaming data processing
- [ ] Machine learning integration for predictive analytics
- [ ] Advanced data quality monitoring
- [ ] Multi-region deployment support
- [ ] API rate limiting and caching

## Contact

For questions or contributions regarding this university project, please contact the ETL Team.

---

**Note**: This project is developed for educational purposes as part of a Business Intelligence and Data Warehousing course. All scheduling placeholders should be configured based on specific requirements and operational needs.
