import pandas as pd

def get_info(file):
    file.info()

def clean_text(value):
    if pd.isna(value):
        return ""
    
    value = str(value).lower()
    value = value.replace("  ", " ")
    return value


opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")
opened_file.to_csv("veridion_product_deduplication_challenge.snappy.csv")


get_info(opened_file)
