# SQL Scripts Organization

This directory contains all SQL scripts organized by purpose and database layer.

## Directory Structure

```
sql/
├── ddl/                    # Data Definition Language (Schema creation)
│   ├── operational/        # Operational database schemas
│   └── analytical/         # Analytical database schemas
├── dml/                    # Data Manipulation Language (ETL operations)
│   ├── star_schema/        # Star schema creation and population
│   ├── scd2/              # SCD2 operations
│   └── cleanup/           # Data cleanup operations
└── migration/             # Database migration scripts
```

## Current File Mapping

### From `dags/star-schema/` → `sql/dml/star_schema/`

- All star schema ETL scripts
- Dimension table operations
- Fact table operations

### From `dags/*.sql` → `sql/dml/scd2/`

- SCD2 update scripts
- Archive operations

### From `init-scripts/` → `sql/ddl/operational/`

- Operational database schema creation

### From `init-scripts-analytical/` → `sql/ddl/analytical/`

- Analytical database schema creation

## Benefits of This Organization

1. **Clear Separation**: DDL vs DML operations
2. **Easy Navigation**: Find scripts by purpose
3. **Better Maintenance**: Logical grouping
4. **Version Control**: Track changes by category
5. **Documentation**: Self-documenting structure

## Usage in DAGs

DAGs should reference SQL files using relative paths:

```python
sql="sql/dml/star_schema/01_create_dims.sql"
```

## Migration Notes

- Original files preserved during transition
- Gradual migration to new structure
- Update DAG references incrementally
