# Data Cleaning Automation

A systematic approach to cleaning messy datasets through configurable operations.

## Business Value

Organizations face significant challenges with messy, inconsistent data:

- **Lost Productivity**: Data scientists and analysts spend 60-80% of their time cleaning data rather than analyzing it
- **Inconsistent Results**: Manual cleaning introduces inconsistencies between different team members
- **Missing Audit Trail**: Manual cleaning lacks documentation of what was changed and why
- **Repeated Work**: Similar cleaning operations are reimplemented for each new dataset
- **Data Governance Challenges**: Inconsistent handling of sensitive data across the organization

This solution addresses these problems through:

1. **Automation**: Reduces cleaning time from hours to minutes
2. **Reproducibility**: The same cleaning steps can be reapplied to updated data
3. **Transparency**: Detailed reports document every transformation applied
4. **Flexibility**: Configurable operations accommodate different data types and quality issues
5. **Consistency**: Standard processes ensure uniform handling of data quality problems

## How It Works

At its core, this project implements a pipeline of data cleaning operations:

```
Raw Data → Operation 1 → Operation 2 → ... → Operation N → Clean Data
                                                           ↓
                                                     Cleaning Report
```

Each operation addresses a specific data quality issue:

1. **Structural Issues**: Inconsistent column names, unnecessary columns
2. **Duplicate Data**: Redundant records
3. **Type Issues**: Incorrect data types
4. **Format Inconsistencies**: Inconsistent text formats, dates, numbers
5. **Missing Values**: Gaps in the data
6. **Outliers**: Extreme values that distort analysis
7. **Invalid Data**: Records that don't meet business rules
8. **Derived Information**: Calculated fields needed for analysis

## Project Structure

```
data-cleaning/
├── data_cleaner.py       # Main script with cleaning logic
├── config.yaml           # Configuration file defining cleaning operations
├── requirements.txt      # Python dependencies
├── README.md             # This file
├── data/                 # Directory for raw and cleaned data
└── docs/                 # Documentation
```

## Installation

1. Clone this repository:
   ```bash
   git clone 
   cd data-cleaning
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

```bash
python data_cleaner.py input_data.csv --config config.yaml --output cleaned_data.csv
```

### Additional Options

```bash
python data_cleaner.py input_data.csv --config config.yaml --output cleaned_data.csv --report cleaning_report.json
```

## Configuration

The cleaning process is driven by a YAML configuration file that defines a sequence of operations. Each operation has a type and specific parameters.

### Example Configuration

```yaml
operations:
  - type: remove_duplicates
    config:
      columns: ["customer_id", "transaction_date"]
      keep: "first"
  
  - type: handle_missing_values
    config:
      strategy: "fill"
      columns: ["income"]
      fill_values:
        income: 50000
```

### Available Operations

1. **remove_duplicates**: Remove duplicate rows
2. **handle_missing_values**: Handle NULL/NA values
3. **fix_data_types**: Convert columns to correct types
4. **standardize_text**: Normalize text data
5. **filter_rows**: Remove rows based on conditions
6. **handle_outliers**: Detect and process outliers
7. **rename_columns**: Standardize column names
8. **derive_columns**: Create calculated fields
9. **drop_columns**: Remove unnecessary columns

## Cleaning Report

After processing, the script generates a detailed JSON report documenting:

- Start and end times
- Original and final data dimensions
- Each operation applied
- Details of transformations made
- Statistics about data quality improvements

Example report excerpt:
```json
{
  "start_time": "2023-06-15T14:30:22.651232",
  "operations": [
    {
      "type": "remove_duplicates",
      "description": "Removed duplicate rows",
      "details": {
        "columns_used": ["customer_id", "transaction_date"],
        "duplicates_removed": 52
      }
    }
  ],
  "statistics": {
    "original_rows": 1452,
    "final_rows": 1322,
    "rows_removed": 130
  }
}
```

## Possible Future Use Cases

### Customer Data Integration

Problem: Customer data from multiple systems needs to be integrated with inconsistent formats.

Solution:
1. Standardize name formats (upper/lower case, whitespace)
2. Parse phone numbers into consistent format
3. Deduplicate based on email and phone
4. Standardize address information

### Financial Transaction Cleaning

Problem: Transaction data contains test records, invalid amounts, and duplicates.

Solution:
1. Remove test transactions (identified by specific account numbers)
2. Filter out zero-amount transactions
3. Remove transactions in the future
4. Handle outlier transaction amounts
5. Derive year/month/day fields for time-based analysis

### IoT Sensor Data Preparation

Problem: Sensor data contains equipment failures, calibration errors, and missing readings.

Solution:
1. Apply physical validity rules (e.g., temperature ranges)
2. Handle missing readings with interpolation
3. Remove calibration periods
4. Flag anomalous readings
5. Derive rolling averages for trend analysis

## Extending the System

Add new cleaning operations by:

1. Implementing a new method in the DataCleaner class following the pattern:
   ```python
   def _apply_your_operation(self, df, **params):
       # Transform the dataframe
       # Return (transformed_df, report_dict)
   ```

2. Use it in your configuration:
   ```yaml
   operations:
     - type: your_operation
       config:
         param1: value1
   ```

## Best Practices

1. **Start with exploration**: Before cleaning, understand your data's issues
2. **Test incrementally**: Add operations one at a time and verify results
3. **Preserve raw data**: Never modify the original files
4. **Document decisions**: Add comments to your config explaining why certain operations are needed
5. **Review the report**: Always check the cleaning report to understand what changes were made
6. **Version your configs**: Track changes to cleaning configurations in version control
