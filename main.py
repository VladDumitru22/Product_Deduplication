import pandas as pd

def get_info(file):
    file.info()

opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")


get_info(opened_file)
