# GreenPals ETL - Folder Structure

## Current Structure Analysis

The current folder structure has some organization but could be improved for better maintainability and professional standards.

### Current Root Structure

```
BI_GreenPals/
├── api/                     # External API services
├── dags/                    # Airflow DAGs (mixed with scripts)
├── data/                    # Initial data files (CSV)
├── init-scripts/           # Operational DB initialization
├── init-scripts-analytical/ # Analytical DB initialization
├── logs/                   # Airflow logs
├── plugins/                # Airflow plugins (empty)
├── docker-compose.yaml     # Infrastructure definition
├── README.md              # Project documentation
└── *.sql files            # Large SQL inserts (should be in data/)
```

### Current DAGs Structure Issues

```
dags/
├── utils/                  # ✅ Good - utility modules
├── star-schema/           # ✅ Good - SQL scripts organized
├── *.py files             # ❌ Mixed: DAGs + ETL scripts + configs
└── *.sql files           # ❌ Mixed: SQL files scattered
```

## Proposed Improved Structure

### Professional Data Engineering Structure

```
BI_GreenPals/
├── airflow/
│   ├── dags/              # Pure Airflow DAG definitions only
│   ├── plugins/           # Airflow custom plugins
│   └── config/            # Airflow-specific configurations
├── src/
│   ├── etl/               # ETL business logic
│   │   ├── extract/       # Data extraction modules
│   │   ├── transform/     # Data transformation modules
│   │   ├── load/          # Data loading modules
│   │   └── __init__.py
│   ├── utils/             # Shared utilities
│   │   ├── database/      # Database utilities
│   │   ├── logging/       # Logging utilities
│   │   ├── config/        # Configuration management
│   │   └── __init__.py
│   ├── models/            # Data models and schemas
│   └── __init__.py
├── sql/
│   ├── ddl/               # Database schema definitions
│   │   ├── operational/   # Operational DB schemas
│   │   └── analytical/    # Analytical DB schemas
│   ├── dml/               # Data manipulation scripts
│   │   ├── star_schema/   # Star schema ETL scripts
│   │   ├── scd2/          # SCD2 scripts
│   │   └── cleanup/       # Data cleanup scripts
│   └── migration/         # Database migration scripts
├── data/
│   ├── raw/               # Raw data files
│   ├── processed/         # Processed data files
│   └── sample/            # Sample/test data
├── config/
│   ├── environments/      # Environment-specific configs
│   │   ├── development.yaml
│   │   ├── production.yaml
│   │   └── testing.yaml
│   └── database.yaml     # Database configurations
├── docs/
│   ├── architecture/      # System architecture docs
│   ├── user_guides/       # User documentation
│   └── api/               # API documentation
├── tests/
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── fixtures/          # Test data fixtures
├── scripts/
│   ├── setup/             # Setup and installation scripts
│   ├── deployment/        # Deployment scripts
│   └── maintenance/       # Maintenance scripts
├── logs/                  # Application logs
├── docker/
│   ├── airflow/           # Airflow Docker configurations
│   ├── databases/         # Database Docker configurations
│   └── api/               # API Docker configurations
├── requirements/
│   ├── base.txt           # Base requirements
│   ├── dev.txt            # Development requirements
│   └── prod.txt           # Production requirements
├── .env.example           # Environment variables template
├── docker-compose.yaml    # Infrastructure definition
├── Makefile              # Common tasks automation
└── README.md             # Project documentation
```

## Migration Strategy

### Phase 1: Minimal Reorganization (Safe)

1. Create new directory structure alongside existing
2. Move files gradually without breaking current functionality
3. Update import paths incrementally

### Phase 2: Full Migration (After testing)

1. Migrate all scripts to new structure
2. Update Airflow DAG paths
3. Update Docker volume mounts
4. Remove old structure

## Benefits of Improved Structure

### 🎯 **Separation of Concerns**

- **`airflow/`**: Pure orchestration logic
- **`src/`**: Business logic and reusable code
- **`sql/`**: All SQL organized by purpose
- **`config/`**: Environment management

### 🔧 **Better Maintainability**

- Clear module boundaries
- Easier testing and debugging
- Professional development standards
- Better code reusability

### 📚 **Enhanced Documentation**

- Self-documenting folder structure
- Clear responsibility boundaries
- Easier onboarding for new developers

### 🚀 **Scalability**

- Easy to add new ETL processes
- Modular architecture
- Environment-specific configurations
- CI/CD friendly structure

## Implementation Priority

### High Priority (Do Now)

1. ✅ Create `src/utils/` structure (Already done)
2. ✅ Centralize constants and configuration (Already done)
3. 🔄 Separate DAGs from ETL scripts
4. 🔄 Organize SQL files by purpose

### Medium Priority (After testing)

1. Create comprehensive `config/` management
2. Add proper testing structure
3. Environment-specific configurations
4. Documentation improvements

### Low Priority (Future)

1. Advanced CI/CD structure
2. Kubernetes deployment structure
3. Monitoring and observability setup

## Next Steps

1. **Test current improvements** with existing structure
2. **Gradually migrate** critical files to new structure
3. **Update documentation** as we progress
4. **Maintain backward compatibility** during transition
