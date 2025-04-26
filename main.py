import pandas as pd

def get_info(file):
    file.info()

def clean_text(value):
    if pd.isna(value):
        return ""
    
    value = str(value).lower()
    value = value.replace(" ", "")
    return value


opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")


print(clean_text(opened_file["unspsc"][0]))
#get_info(opened_file)
