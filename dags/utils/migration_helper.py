"""
Migration Helper for GreenPals ETL Pipeline

This utility helps safely migrate from original scripts to improved versions,
allowing for easy testing and rollback if needed.
"""

import os
import shutil
from typing import List, Dict
from datetime import datetime

# Mapping of original files to improved versions
MIGRATION_MAP = {
    "incremental_load_python.py": "incremental_load_python_improved.py",
    # Add more mappings as we create improved versions
}

def create_backup(file_path: str) -> str:
    """
    Create a backup of the original file with timestamp.
    
    Args:
        file_path: Path to the file to backup
        
    Returns:
        Path to the backup file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.backup_{timestamp}"
    
    if os.path.exists(file_path):
        shutil.copy2(file_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
        return backup_path
    else:
        print(f"⚠️  Original file not found: {file_path}")
        return ""

def switch_to_improved(original_name: str) -> bool:
    """
    Switch from original script to improved version.
    
    Args:
        original_name: Name of the original script file
        
    Returns:
        True if switch successful, False otherwise
    """
    if original_name not in MIGRATION_MAP:
        print(f"❌ No improved version available for {original_name}")
        return False
    
    improved_name = MIGRATION_MAP[original_name]
    
    # Check if improved version exists
    if not os.path.exists(improved_name):
        print(f"❌ Improved version not found: {improved_name}")
        return False
    
    # Create backup of original
    backup_path = create_backup(original_name)
    if not backup_path:
        return False
    
    try:
        # Copy improved version to original name
        shutil.copy2(improved_name, original_name)
        print(f"✅ Switched to improved version: {original_name}")
        print(f"📝 Original backed up as: {backup_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error during switch: {e}")
        return False

def rollback_to_original(original_name: str, backup_path: str = None) -> bool:
    """
    Rollback to the original version from backup.
    
    Args:
        original_name: Name of the original script file
        backup_path: Optional specific backup path to restore from
        
    Returns:
        True if rollback successful, False otherwise
    """
    if backup_path and os.path.exists(backup_path):
        target_backup = backup_path
    else:
        # Find the latest backup
        backup_files = [f for f in os.listdir('.') if f.startswith(f"{original_name}.backup_")]
        if not backup_files:
            print(f"❌ No backup files found for {original_name}")
            return False
        
        # Get the latest backup
        backup_files.sort(reverse=True)
        target_backup = backup_files[0]
    
    try:
        shutil.copy2(target_backup, original_name)
        print(f"✅ Rolled back to original: {original_name}")
        print(f"📝 Restored from: {target_backup}")
        return True
        
    except Exception as e:
        print(f"❌ Error during rollback: {e}")
        return False

def test_improved_version(script_name: str) -> None:
    """
    Run a test of the improved version without replacing the original.
    
    Args:
        script_name: Name of the script to test
    """
    if script_name not in MIGRATION_MAP:
        print(f"❌ No improved version available for {script_name}")
        return
    
    improved_name = MIGRATION_MAP[script_name]
    
    if not os.path.exists(improved_name):
        print(f"❌ Improved version not found: {improved_name}")
        return
    
    print(f"🧪 Testing improved version: {improved_name}")
    print("You can run this manually to test before switching:")
    print(f"python {improved_name}")

def list_available_improvements() -> None:
    """List all available improvements."""
    print("📋 Available Improvements:")
    print("-" * 50)
    
    for original, improved in MIGRATION_MAP.items():
        status = "✅ Available" if os.path.exists(improved) else "❌ Not Found"
        print(f"{original} → {improved} [{status}]")

def interactive_migration() -> None:
    """Interactive migration helper."""
    print("🔧 GreenPals ETL Migration Helper")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("1. List available improvements")
        print("2. Test improved version")
        print("3. Switch to improved version")
        print("4. Rollback to original")
        print("5. Exit")
        
        choice = input("\nSelect an option (1-5): ").strip()
        
        if choice == "1":
            list_available_improvements()
            
        elif choice == "2":
            script_name = input("Enter script name to test: ").strip()
            test_improved_version(script_name)
            
        elif choice == "3":
            script_name = input("Enter script name to switch: ").strip()
            switch_to_improved(script_name)
            
        elif choice == "4":
            script_name = input("Enter script name to rollback: ").strip()
            rollback_to_original(script_name)
            
        elif choice == "5":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid option. Please try again.")

if __name__ == "__main__":
    interactive_migration() 