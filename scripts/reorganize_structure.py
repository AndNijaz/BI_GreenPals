#!/usr/bin/env python3
"""
Folder Structure Reorganization Script

This script safely reorganizes the GreenPals ETL project structure
while preserving original files and maintaining backward compatibility.
"""

import os
import shutil
import argparse
from pathlib import Path
from datetime import datetime

class StructureReorganizer:
    """Handles safe reorganization of project structure."""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.root_dir = Path.cwd()
        self.backup_suffix = f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    def log(self, message: str):
        """Log reorganization actions."""
        prefix = "[DRY RUN] " if self.dry_run else "[EXECUTING] "
        print(f"{prefix}{message}")
    
    def create_directory(self, path: Path):
        """Create directory if it doesn't exist."""
        if not self.dry_run:
            path.mkdir(parents=True, exist_ok=True)
        self.log(f"Create directory: {path}")
    
    def copy_file(self, source: Path, destination: Path):
        """Copy file to new location."""
        if source.exists():
            self.create_directory(destination.parent)
            if not self.dry_run:
                shutil.copy2(source, destination)
            self.log(f"Copy: {source} → {destination}")
        else:
            self.log(f"WARNING: Source file not found: {source}")
    
    def reorganize_sql_files(self):
        """Reorganize SQL files by purpose."""
        self.log("\n=== Reorganizing SQL Files ===")
        
        # Create SQL directory structure
        sql_dirs = [
            "sql/ddl/operational",
            "sql/ddl/analytical", 
            "sql/dml/star_schema",
            "sql/dml/scd2",
            "sql/dml/cleanup",
            "sql/migration"
        ]
        
        for dir_path in sql_dirs:
            self.create_directory(self.root_dir / dir_path)
        
        # Move star schema files
        star_schema_source = self.root_dir / "dags" / "star-schema"
        star_schema_dest = self.root_dir / "sql" / "dml" / "star_schema"
        
        if star_schema_source.exists():
            for sql_file in star_schema_source.glob("*.sql"):
                self.copy_file(sql_file, star_schema_dest / sql_file.name)
        
        # Move SCD2 files from dags
        scd2_files = [
            "scd2_update.sql",
            "scd2_cleaned.sql",
            "full_load_cleaned.sql"
        ]
        
        for filename in scd2_files:
            source = self.root_dir / "dags" / filename
            dest = self.root_dir / "sql" / "dml" / "scd2" / filename
            self.copy_file(source, dest)
        
        # Move DDL files
        # Operational schemas
        init_scripts = self.root_dir / "init-scripts"
        if init_scripts.exists():
            for sql_file in init_scripts.glob("*.sql"):
                dest = self.root_dir / "sql" / "ddl" / "operational" / sql_file.name
                self.copy_file(sql_file, dest)
        
        # Analytical schemas
        analytical_scripts = self.root_dir / "init-scripts-analytical"
        if analytical_scripts.exists():
            for sql_file in analytical_scripts.glob("*.sql"):
                dest = self.root_dir / "sql" / "ddl" / "analytical" / sql_file.name
                self.copy_file(sql_file, dest)
    
    def reorganize_etl_scripts(self):
        """Separate ETL scripts from DAG definitions."""
        self.log("\n=== Reorganizing ETL Scripts ===")
        
        # Create airflow directory structure
        airflow_dirs = [
            "airflow/dags",
            "airflow/plugins",
            "airflow/config"
        ]
        
        for dir_path in airflow_dirs:
            self.create_directory(self.root_dir / dir_path)
        
        # Move pure DAG files to airflow/dags
        dag_files = [
            "star_schema_etl.py",
            "full_scd2_etl.py", 
            "incremental_scd2_etl.py",
            "populate_cleaned_dag.py",
            "etl_full_to_cleaned.py",
            "total_incremental_etl.py",
            "api_to_landing_dag.py"
        ]
        
        for filename in dag_files:
            source = self.root_dir / "dags" / filename
            dest = self.root_dir / "airflow" / "dags" / filename
            self.copy_file(source, dest)
        
        # Move ETL scripts to src/etl
        etl_scripts = [
            "incremental_load_python.py",
            "incremental_load_python_improved.py",
            "full_load_python.py",
            "populate_cleaned_python.py",
            "run_full_cleaned_load.py",
            "etl_prices_to_landing.py",
            "etl_co2_to_landing.py"
        ]
        
        extract_dir = self.root_dir / "src" / "etl" / "extract"
        self.create_directory(extract_dir)
        
        for filename in etl_scripts:
            source = self.root_dir / "dags" / filename
            dest = extract_dir / filename
            self.copy_file(source, dest)
    
    def reorganize_config_files(self):
        """Organize configuration files."""
        self.log("\n=== Reorganizing Configuration Files ===")
        
        # Create config directory
        config_dir = self.root_dir / "config"
        self.create_directory(config_dir)
        self.create_directory(config_dir / "environments")
        
        # Move configuration files
        config_files = [
            ("dags/constants.py", "config/constants.py"),
            ("dags/tables_config.py", "config/tables_config.py")
        ]
        
        for source_path, dest_path in config_files:
            source = self.root_dir / source_path
            dest = self.root_dir / dest_path
            self.copy_file(source, dest)
    
    def run_reorganization(self):
        """Run the complete reorganization process."""
        print(f"Starting folder structure reorganization...")
        print(f"Working directory: {self.root_dir}")
        print(f"Dry run mode: {self.dry_run}")
        print("")
        
        self.reorganize_sql_files()
        self.reorganize_etl_scripts()
        self.reorganize_config_files()

def main():
    parser = argparse.ArgumentParser(description="Reorganize GreenPals ETL project structure")
    parser.add_argument(
        "--execute", 
        action="store_true", 
        help="Execute the reorganization (default is dry run)"
    )
    
    args = parser.parse_args()
    
    reorganizer = StructureReorganizer(dry_run=not args.execute)
    reorganizer.run_reorganization()

if __name__ == "__main__":
    main() 