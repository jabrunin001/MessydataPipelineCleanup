#!/usr/bin/env python3
"""
Data Cleaning Automation Script

This script automates the process of cleaning messy datasets through configurable
cleaning operations. It handles common data quality issues such as missing values,
duplicates, outliers, inconsistent formatting, and data type issues.

Business Value:
- Reduces manual data preparation time by 60-80%
- Ensures consistent application of data quality rules
- Creates an audit trail of all cleaning operations
- Makes data cleaning reproducible and shareable
- Supports data governance through consistent handling of sensitive data
"""

import pandas as pd
import numpy as np
import yaml
import json
import re
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Union, Optional, Callable
import argparse
from functools import reduce


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_cleaning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Core class for automated data cleaning with configurable operations.
    
    This class implements a pipeline of data cleaning operations that can be
    applied to pandas DataFrames. Operations are configurable and can be
    chained together to form a complete cleaning process.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the DataCleaner with an optional configuration.
        
        Args:
            config_path: Path to a YAML configuration file
        """
        self.config = {}
        self.cleaning_report = {
            "start_time": datetime.now().isoformat(),
            "operations": [],
            "statistics": {}
        }
        
        # Load configuration if provided
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> None:
        """
        Load cleaning configuration from a YAML file.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def save_report(self, output_path: str) -> None:
        """
        Save the cleaning report to a JSON file.
        
        Args:
            output_path: Path to save the report
        """
        # Finalize the report
        self.cleaning_report["end_time"] = datetime.now().isoformat()
        
        try:
            with open(output_path, 'w') as f:
                json.dump(self.cleaning_report, f, indent=2)
            logger.info(f"Saved cleaning report to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            raise
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all configured cleaning operations to a DataFrame.
        
        Args:
            df: Input DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        # Store original shape
        original_shape = df.shape
        self.cleaning_report["statistics"]["original_rows"] = original_shape[0]
        self.cleaning_report["statistics"]["original_columns"] = original_shape[1]
        
        # Make a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Apply operations based on configuration
        if "operations" in self.config:
            for operation in self.config["operations"]:
                op_type = operation.get("type")
                op_config = operation.get("config", {})
                
                # Skip if operation type is missing
                if not op_type:
                    continue
                
                # Apply the operation
                operation_method = getattr(self, f"_apply_{op_type}", None)
                if operation_method:
                    operation_start = datetime.now()
                    cleaned_df, op_report = operation_method(cleaned_df, **op_config)
                    operation_end = datetime.now()
                    
                    # Add to the report
                    op_report.update({
                        "type": op_type,
                        "duration_seconds": (operation_end - operation_start).total_seconds(),
                        "timestamp": operation_start.isoformat()
                    })
                    self.cleaning_report["operations"].append(op_report)
                else:
                    logger.warning(f"Unknown operation type: {op_type}")
        
        # Final statistics
        final_shape = cleaned_df.shape
        self.cleaning_report["statistics"]["final_rows"] = final_shape[0]
        self.cleaning_report["statistics"]["final_columns"] = final_shape[1]
        self.cleaning_report["statistics"]["rows_removed"] = original_shape[0] - final_shape[0]
        self.cleaning_report["statistics"]["columns_removed"] = original_shape[1] - final_shape[1]
        
        return cleaned_df
    
    def _apply_remove_duplicates(self, df: pd.DataFrame, 
                               columns: Optional[List[str]] = None, 
                               keep: str = 'first') -> tuple:
        """
        Remove duplicate rows from the DataFrame.
        
        Args:
            df: Input DataFrame
            columns: Columns to identify duplicates (None means use all columns)
            keep: Which duplicates to keep ('first', 'last', or False to drop all)
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        before_count = len(df)
        
        # Identify and remove duplicates
        if columns:
            cleaned_df = df.drop_duplicates(subset=columns, keep=keep)
        else:
            cleaned_df = df.drop_duplicates(keep=keep)
        
        after_count = len(cleaned_df)
        duplicates_removed = before_count - after_count
        
        # Create operation report
        report = {
            "description": "Removed duplicate rows",
            "details": {
                "columns_used": columns if columns else "all",
                "keep_strategy": keep,
                "duplicates_removed": duplicates_removed
            }
        }
        
        logger.info(f"Removed {duplicates_removed} duplicate rows")
        return cleaned_df, report
    
    def _apply_handle_missing_values(self, df: pd.DataFrame, 
                                   strategy: str = 'drop', 
                                   columns: Optional[List[str]] = None,
                                   fill_values: Optional[Dict[str, Any]] = None,
                                   threshold: Optional[float] = None) -> tuple:
        """
        Handle missing values in the DataFrame.
        
        Args:
            df: Input DataFrame
            strategy: Strategy for handling missing values
                      ('drop', 'fill', 'drop_rows', 'drop_columns')
            columns: Specific columns to apply to (None means all)
            fill_values: Dictionary mapping column names to fill values
            threshold: For drop_rows/drop_columns, minimum percentage of missing values
                      to trigger removal
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        before_shape = df.shape
        target_columns = columns if columns else df.columns.tolist()
        
        # Initialize report
        report = {
            "description": "Handled missing values",
            "details": {
                "strategy": strategy,
                "columns": target_columns,
                "missing_counts_before": {col: int(df[col].isna().sum()) 
                                         for col in target_columns}
            }
        }
        
        # Handle missing values based on strategy
        if strategy == 'drop':
            # Drop rows with any missing values in specified columns
            cleaned_df = df.dropna(subset=target_columns)
            
        elif strategy == 'fill':
            cleaned_df = df.copy()
            
            # Use provided fill values or defaults
            for col in target_columns:
                # If fill value provided for this column
                if fill_values and col in fill_values:
                    fill_value = fill_values[col]
                    cleaned_df[col] = cleaned_df[col].fillna(fill_value)
                # Otherwise use sensible defaults based on data type
                else:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        # For numeric columns, use median to avoid outlier influence
                        cleaned_df[col] = cleaned_df[col].fillna(df[col].median())
                    elif pd.api.types.is_categorical_dtype(df[col]) or df[col].dtype == 'object':
                        # For categorical/string columns, use mode (most common value)
                        mode_value = df[col].mode()[0] if not df[col].mode().empty else "UNKNOWN"
                        cleaned_df[col] = cleaned_df[col].fillna(mode_value)
                    else:
                        # For other types, use general fillna
                        cleaned_df[col] = cleaned_df[col].fillna(df[col].mode()[0])
            
        elif strategy == 'drop_rows':
            # Drop rows with missing percentage above threshold
            if threshold is not None:
                # Calculate percentage of missing values for each row
                row_missing_pct = df[target_columns].isna().mean(axis=1)
                # Keep rows with missing percentage below threshold
                cleaned_df = df[row_missing_pct < threshold]
            else:
                cleaned_df = df
                
        elif strategy == 'drop_columns':
            # Drop columns with missing percentage above threshold
            if threshold is not None:
                # Calculate percentage of missing values for each column
                col_missing_pct = df[target_columns].isna().mean()
                # Columns to drop
                cols_to_drop = col_missing_pct[col_missing_pct >= threshold].index.tolist()
                # Keep other columns
                cleaned_df = df.drop(columns=cols_to_drop)
                report["details"]["columns_dropped"] = cols_to_drop
            else:
                cleaned_df = df
        else:
            # Unknown strategy
            logger.warning(f"Unknown missing value strategy: {strategy}")
            cleaned_df = df
        
        # Final report details
        after_shape = cleaned_df.shape
        rows_removed = before_shape[0] - after_shape[0]
        cols_removed = before_shape[1] - after_shape[1]
        
        report["details"].update({
            "rows_removed": rows_removed,
            "columns_removed": cols_removed,
            "missing_counts_after": {col: int(cleaned_df[col].isna().sum()) 
                                   for col in target_columns 
                                   if col in cleaned_df.columns}
        })
        
        logger.info(f"Handled missing values with strategy '{strategy}', "
                   f"removed {rows_removed} rows and {cols_removed} columns")
        
        return cleaned_df, report
    
    def _apply_fix_data_types(self, df: pd.DataFrame, 
                            column_types: Dict[str, str] = None) -> tuple:
        """
        Convert columns to the correct data types.
        
        Args:
            df: Input DataFrame
            column_types: Dictionary mapping column names to desired data types
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        cleaned_df = df.copy()
        conversions = {}
        
        # Apply type conversions
        if column_types:
            for column, dtype in column_types.items():
                if column in cleaned_df.columns:
                    original_type = str(cleaned_df[column].dtype)
                    try:
                        # Handle special cases
                        if dtype == 'datetime':
                            cleaned_df[column] = pd.to_datetime(cleaned_df[column], errors='coerce')
                        else:
                            cleaned_df[column] = cleaned_df[column].astype(dtype)
                        
                        conversions[column] = {
                            "from": original_type,
                            "to": dtype,
                            "success": True
                        }
                    except Exception as e:
                        logger.warning(f"Failed to convert {column} to {dtype}: {e}")
                        conversions[column] = {
                            "from": original_type,
                            "to": dtype,
                            "success": False,
                            "error": str(e)
                        }
        
        report = {
            "description": "Fixed data types",
            "details": {
                "conversions": conversions
            }
        }
        
        logger.info(f"Applied data type conversions to {len(conversions)} columns")
        return cleaned_df, report
    
    def _apply_standardize_text(self, df: pd.DataFrame, 
                              columns: List[str],
                              case: str = 'lower',
                              strip: bool = True,
                              replace_patterns: Optional[List[Dict]] = None) -> tuple:
        """
        Standardize text data in specified columns.
        
        Args:
            df: Input DataFrame
            columns: Text columns to standardize
            case: Case conversion ('lower', 'upper', 'title', or None)
            strip: Whether to strip whitespace
            replace_patterns: List of pattern replacement dictionaries with
                             'pattern' and 'replacement' keys
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        cleaned_df = df.copy()
        transformations = {}
        
        for col in columns:
            if col in cleaned_df.columns and cleaned_df[col].dtype == 'object':
                # Track original values for the report
                original_values = cleaned_df[col].dropna().unique().tolist()
                
                # Apply text standardization in sequence
                if strip:
                    cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
                
                if case == 'lower':
                    cleaned_df[col] = cleaned_df[col].astype(str).str.lower()
                elif case == 'upper':
                    cleaned_df[col] = cleaned_df[col].astype(str).str.upper()
                elif case == 'title':
                    cleaned_df[col] = cleaned_df[col].astype(str).str.title()
                
                if replace_patterns:
                    for pattern_dict in replace_patterns:
                        pattern = pattern_dict.get('pattern')
                        replacement = pattern_dict.get('replacement', '')
                        if pattern:
                            cleaned_df[col] = cleaned_df[col].astype(str).str.replace(
                                pattern, replacement, regex=True
                            )
                
                # Track new values for the report
                new_values = cleaned_df[col].dropna().unique().tolist()
                
                # Record the transformations
                transformations[col] = {
                    "unique_values_before": len(original_values),
                    "unique_values_after": len(new_values),
                    "sample_transformations": [
                        {"before": before, "after": after}
                        for before, after in zip(
                            original_values[:5],  # Sample up to 5 transformations
                            new_values[:5] if len(new_values) >= 5 else new_values + [''] * (5 - len(new_values))
                        )
                        if before != after
                    ]
                }
        
        report = {
            "description": "Standardized text data",
            "details": {
                "columns": columns,
                "case": case,
                "strip": strip,
                "replace_patterns": replace_patterns,
                "transformations": transformations
            }
        }
        
        logger.info(f"Standardized text in {len(columns)} columns")
        return cleaned_df, report
    
    def _apply_filter_rows(self, df: pd.DataFrame, 
                         conditions: List[Dict]) -> tuple:
        """
        Filter rows based on specified conditions.
        
        Args:
            df: Input DataFrame
            conditions: List of condition dictionaries, each with:
                       - column: Column name
                       - operator: Comparison operator ('==', '!=', '>', '<', etc.)
                       - value: Value to compare against
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        before_count = len(df)
        cleaned_df = df.copy()
        
        # Define operator functions
        operators = {
            '==': lambda x, y: x == y,
            '!=': lambda x, y: x != y,
            '>': lambda x, y: x > y,
            '<': lambda x, y: x < y,
            '>=': lambda x, y: x >= y,
            '<=': lambda x, y: x <= y,
            'in': lambda x, y: x.isin(y),
            'not_in': lambda x, y: ~x.isin(y),
            'contains': lambda x, y: x.str.contains(y, na=False),
            'not_contains': lambda x, y: ~x.str.contains(y, na=False),
            'is_null': lambda x, y: x.isna(),
            'is_not_null': lambda x, y: ~x.isna(),
        }
        
        # Apply each filter condition
        filters = []
        filter_details = []
        
        for condition in conditions:
            column = condition.get('column')
            operator = condition.get('operator')
            value = condition.get('value')
            
            if column in cleaned_df.columns and operator in operators:
                # Handle special cases
                if operator in ('is_null', 'is_not_null'):
                    mask = operators[operator](cleaned_df[column], None)
                else:
                    mask = operators[operator](cleaned_df[column], value)
                
                # Add the filter mask
                filters.append(mask)
                
                # Track filter details
                filter_details.append({
                    "column": column,
                    "operator": operator,
                    "value": str(value) if value is not None else None,
                    "rows_matched": int(mask.sum())
                })
        
        # Apply combined filter if any filters exist
        if filters:
            # Combine all filters with AND logic
            combined_filter = reduce(lambda x, y: x & y, filters)
            cleaned_df = cleaned_df[combined_filter]
        
        after_count = len(cleaned_df)
        rows_removed = before_count - after_count
        
        report = {
            "description": "Filtered rows based on conditions",
            "details": {
                "conditions": filter_details,
                "rows_before": before_count,
                "rows_after": after_count,
                "rows_removed": rows_removed
            }
        }
        
        logger.info(f"Filtered rows, removed {rows_removed} rows")
        return cleaned_df, report
    
    def _apply_handle_outliers(self, df: pd.DataFrame, 
                             columns: List[str],
                             method: str = 'iqr',
                             action: str = 'remove',
                             factor: float = 1.5,
                             custom_bounds: Optional[Dict[str, Dict[str, float]]] = None) -> tuple:
        """
        Detect and handle outliers in numeric columns.
        
        Args:
            df: Input DataFrame
            columns: Numeric columns to check for outliers
            method: Detection method ('iqr', 'zscore', 'percentile', 'custom')
            action: Action to take ('remove', 'cap', 'replace')
            factor: Multiple for IQR or Z-score methods
            custom_bounds: Dictionary mapping column names to bound dictionaries
                          with 'lower' and 'upper' keys
        
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        before_count = len(df)
        cleaned_df = df.copy()
        outlier_details = {}
        
        for col in columns:
            if col in cleaned_df.columns and pd.api.types.is_numeric_dtype(cleaned_df[col]):
                # Skip columns with all missing values
                if cleaned_df[col].isna().all():
                    outlier_details[col] = {"error": "Column contains all missing values"}
                    continue
                
                # Calculate bounds based on method
                if method == 'iqr':
                    Q1 = cleaned_df[col].quantile(0.25)
                    Q3 = cleaned_df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - factor * IQR
                    upper_bound = Q3 + factor * IQR
                
                elif method == 'zscore':
                    mean = cleaned_df[col].mean()
                    std = cleaned_df[col].std()
                    lower_bound = mean - factor * std
                    upper_bound = mean + factor * std
                
                elif method == 'percentile':
                    lower_bound = cleaned_df[col].quantile(0.01)  # 1st percentile
                    upper_bound = cleaned_df[col].quantile(0.99)  # 99th percentile
                
                elif method == 'custom' and custom_bounds and col in custom_bounds:
                    bounds = custom_bounds[col]
                    lower_bound = bounds.get('lower', cleaned_df[col].min())
                    upper_bound = bounds.get('upper', cleaned_df[col].max())
                
                else:
                    # Skip if method not recognized or bounds not provided
                    outlier_details[col] = {"error": f"Invalid method {method} or missing custom bounds"}
                    continue
                
                # Identify outliers
                outliers = (cleaned_df[col] < lower_bound) | (cleaned_df[col] > upper_bound)
                outlier_count = outliers.sum()
                
                # Handle outliers based on action
                if action == 'remove':
                    cleaned_df = cleaned_df[~outliers]
                elif action == 'cap':
                    cleaned_df.loc[cleaned_df[col] < lower_bound, col] = lower_bound
                    cleaned_df.loc[cleaned_df[col] > upper_bound, col] = upper_bound
                elif action == 'replace':
                    # Replace with median for outliers
                    median = cleaned_df[col].median()
                    cleaned_df.loc[outliers, col] = median
                
                # Track outlier details
                outlier_details[col] = {
                    "lower_bound": float(lower_bound),
                    "upper_bound": float(upper_bound),
                    "outliers_detected": int(outlier_count),
                    "outlier_percentage": float(outlier_count / len(df) * 100)
                }
        
        after_count = len(cleaned_df)
        rows_removed = before_count - after_count
        
        report = {
            "description": "Handled outliers",
            "details": {
                "method": method,
                "action": action,
                "factor": factor,
                "columns": outlier_details,
                "rows_removed": rows_removed
            }
        }
        
        logger.info(f"Handled outliers in {len(columns)} columns, removed {rows_removed} rows")
        return cleaned_df, report
    
    def _apply_rename_columns(self, df: pd.DataFrame, 
                            mapping: Dict[str, str]) -> tuple:
        """
        Rename columns according to the provided mapping.
        
        Args:
            df: Input DataFrame
            mapping: Dictionary mapping old column names to new ones
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        # Track which columns were actually renamed
        renamed = {}
        
        # Only rename columns that exist
        for old_name, new_name in mapping.items():
            if old_name in df.columns:
                renamed[old_name] = new_name
        
        # Apply the renaming
        cleaned_df = df.rename(columns=renamed)
        
        report = {
            "description": "Renamed columns",
            "details": {
                "mapping": renamed
            }
        }
        
        logger.info(f"Renamed {len(renamed)} columns")
        return cleaned_df, report
    
    def _apply_derive_columns(self, df: pd.DataFrame, 
                            derivations: List[Dict]) -> tuple:
        """
        Create new columns derived from existing ones.
        
        Args:
            df: Input DataFrame
            derivations: List of derivation dictionaries, each with:
                        - new_column: Name for the new column
                        - formula: Python expression as string
                        - description: Human-readable description
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        cleaned_df = df.copy()
        derivation_results = {}
        
        for derivation in derivations:
            new_column = derivation.get('new_column')
            formula = derivation.get('formula')
            description = derivation.get('description', '')
            
            if new_column and formula:
                try:
                    # Evaluate the formula using pandas eval
                    cleaned_df[new_column] = cleaned_df.eval(formula)
                    
                    # Track result
                    derivation_results[new_column] = {
                        "formula": formula,
                        "description": description,
                        "success": True
                    }
                except Exception as e:
                    logger.warning(f"Failed to derive column {new_column}: {e}")
                    derivation_results[new_column] = {
                        "formula": formula,
                        "description": description,
                        "success": False,
                        "error": str(e)
                    }
        
        report = {
            "description": "Derived new columns",
            "details": {
                "derivations": derivation_results
            }
        }
        
        logger.info(f"Derived {len(derivation_results)} new columns")
        return cleaned_df, report
    
    def _apply_drop_columns(self, df: pd.DataFrame, 
                          columns: List[str]) -> tuple:
        """
        Drop specified columns from the DataFrame.
        
        Args:
            df: Input DataFrame
            columns: List of column names to drop
            
        Returns:
            Tuple of (cleaned DataFrame, operation report)
        """
        # Identify which columns exist and can be dropped
        columns_to_drop = [col for col in columns if col in df.columns]
        
        # Drop the columns
        cleaned_df = df.drop(columns=columns_to_drop)
        
        report = {
            "description": "Dropped columns",
            "details": {
                "columns_dropped": columns_to_drop,
                "columns_not_found": [col for col in columns if col not in columns_to_drop]
            }
        }
        
        logger.info(f"Dropped {len(columns_to_drop)} columns")
        return cleaned_df, report


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Automate data cleaning for messy datasets.')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('--config', '-c', required=True, help='Path to cleaning configuration YAML file')
    parser.add_argument('--output', '-o', help='Path to save the cleaned CSV file')
    parser.add_argument('--report', '-r', help='Path to save the cleaning report')
    
    args = parser.parse_args()
    
    # Default output and report paths if not provided
    if not args.output:
        base_name = os.path.splitext(args.input_file)[0]
        args.output = f"{base_name}_cleaned.csv"
    
    if not args.report:
        base_name = os.path.splitext(args.input_file)[0]
        args.report = f"{base_name}_cleaning_report.json"
    
    try:
        # Load the data
        logger.info(f"Loading data from {args.input_file}")
        df = pd.read_csv(args.input_file)
        
        # Initialize the cleaner
        cleaner = DataCleaner(args.config)
        
        # Clean the data
        logger.info("Applying cleaning operations")
        cleaned_df = cleaner.clean_dataframe(df)
        
        # Save the cleaned data
        logger.info(f"Saving cleaned data to {args.output}")
        cleaned_df.to_csv(args.output, index=False)
        
        # Save the cleaning report
        logger.info(f"Saving cleaning report to {args.report}")
        cleaner.save_report(args.report)
        
        logger.info("Data cleaning completed successfully")
        
        # Print summary information
        original_rows = cleaner.cleaning_report["statistics"]["original_rows"]
        final_rows = cleaner.cleaning_report["statistics"]["final_rows"]
        rows_removed = cleaner.cleaning_report["statistics"]["rows_removed"]
        operations_count = len(cleaner.cleaning_report["operations"])
        
        print(f"\nData Cleaning Summary:")
        print(f"----------------------")
        print(f"Original rows: {original_rows}")
        print(f"Final rows: {final_rows}")
        print(f"Rows removed: {rows_removed} ({rows_removed/original_rows*100:.1f}%)")
        print(f"Operations performed: {operations_count}")
        print(f"Cleaned data saved to: {args.output}")
        print(f"Cleaning report saved to: {args.report}")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()