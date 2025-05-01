# Product_Deduplication

## Description
This Python application is designed to deduplicate product records from a dataset by identifying similar entries based on certain attributes such as product name, brand, color, and others. The application uses fuzzy matching to group similar records, consolidating them into a single, enriched entry per product. It processes product data in a Parquet format and outputs a deduplicated dataset.

## Features
- Deduplicate product records by comparing product attributes.
- Use fuzzy matching to identify similar entries with a customizable threshold.
- Consolidate duplicate entries into a single record with enriched details.
- Output the cleaned dataset in Parquet format.

## Technologies Used
- Python (pandas, numpy, rapidfuzz)

## How to Run
1. Clone the repository:
   ```bash
   git clone <repository_url>
   ```
2. Navigate to the project directory:
   ```bash
   cd <repository_name>
   ```
3. Install the required dependencies:
   ```bash
   pip install pandas numpy rapidfuzz

   ```
4. Run the script:
   ```bash
   python product_deduplication.py
   ```

## Usage
1. Load the dataset by specifying the input Parquet file.
2. The program will clean and process product attributes such as product_identifier, brand, product_name, and others.
3. The fuzzy matching function will group similar product entries.
4. The deduplicated data will be saved in a new Parquet file.

# Decision-Making and Reasoning
## Data Cleaning
The decision to clean the text data before processing was made to ensure that all product attributes are consistently formatted. This includes:
- Converting the values to lowercase to ensure case insensitivity during comparison.
- Stripping extra whitespace to avoid mismatches due to formatting inconsistencies.
- Handling empty, missing, or invalid data by returning a default value (empty string) to prevent errors.
## Fuzzy Matching
Initially, I used an algorithm that only considered identical values to detect duplicates. However, this approach proved limited as it couldn't handle minor discrepancies (e.g., different spellings, missing spaces). Therefore, we switched to a fuzzy matching algorithm using RapidFuzz:
- **Threshold**: A threshold of 85 was chosen for the fuzzy matching score to ensure a balance between precision and recall. This helps in correctly matching similar but not identical records.
- **Minimum Matches**: The minimum number of matching attributes was set to 5 to group only those products with significant similarity.
## Bug Fixes
During the development process, we identified and fixed critical bugs related to:
- Handling empty and missing values more gracefully.
- Adjusting the fuzzy matching logic to ensure better accuracy and avoid mismatches.
  
## Author
Developed by Dumitru Vlad-Mihai

