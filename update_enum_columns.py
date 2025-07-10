#!/usr/bin/env python3
"""
Script to add native_enum=False to all enum column definitions
"""

import os
import re
from pathlib import Path

def update_file(file_path):
    """Update a single file to add native_enum=False to Enum columns"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern to match Column(Enum(...)) without native_enum already set
    # This handles various formatting styles
    enum_pattern = r'Column\(Enum\(([^)]+)\)([^)]*)\)'
    
    def replace_enum(match):
        enum_class = match.group(1)
        rest_of_column = match.group(2)
        
        # Check if native_enum is already set
        if 'native_enum' in rest_of_column:
            return match.group(0)  # Return unchanged
        
        # Add native_enum=False to the Enum constructor
        return f'Column(Enum({enum_class}, native_enum=False){rest_of_column})'
    
    # Apply the replacement
    content = re.sub(enum_pattern, replace_enum, content)
    
    # Write back if changed
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"✅ Updated: {file_path}")
        return True
    else:
        print(f"⏭️  No changes needed: {file_path}")
        return False

def main():
    """Update all table files to add native_enum=False"""
    
    # Find all Python files in the tables directory
    tables_dir = Path('indexer/database/indexer/tables')
    
    if not tables_dir.exists():
        print(f"❌ Directory not found: {tables_dir}")
        print("Please run this script from your project root directory")
        return
    
    files_to_check = []
    for root, dirs, files in os.walk(tables_dir):
        for file in files:
            if file.endswith('.py') and file != '__init__.py':
                files_to_check.append(Path(root) / file)
    
    print(f"🔍 Found {len(files_to_check)} table files to check")
    print("Adding native_enum=False to Enum columns...\n")
    
    updated_count = 0
    for file_path in files_to_check:
        if update_file(file_path):
            updated_count += 1
    
    print(f"\n✅ Updated {updated_count} files")
    
    if updated_count > 0:
        print("\nNext steps:")
        print("1. Review the changes: git diff")
        print("2. Recreate model database: python -m indexer.cli migrate model recreate blub_test")
        print("3. Test pipeline: python testing/pipeline/test_end_to_end.py 58277747")
    else:
        print("\n✅ All enum columns already have proper configuration!")

if __name__ == "__main__":
    main()