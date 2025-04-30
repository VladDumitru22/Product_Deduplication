import pandas as pd

opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")

def clean_text(value):
    if pd.isna(value):
        return ""
    else:
        value = str(value).lower()
        value = value.replace(" ", "")
        return value
    


def get_primary_keys(row):
    primary_key = {
        "product_indentifier": clean_text(row["product_identifier"]),
        "brand": clean_text(row["brand"]),
        "materials": clean_text(row["materials"]),
        "form": clean_text(row["form"]),
        "size": clean_text(row["size"]),
        "color": clean_text(row["color"]),
        "product_name": clean_text(row["product_name"])
    }
    return primary_key



primary_keys = []
for indexm, row in opened_file.iterrows():
    primary_key = get_primary_keys(row)
    primary_keys.append(primary_key)

opened_file["primary_key"] = primary_keys


def compare_keys(key1, key2):
    count = 0
    for key in key1:
        if key1[key] == key2[key]:
            count += 1
    if count >= 5:
        return True
    else:
        return False



print(clean_text(opened_file["unspsc"][0]))
get_info(opened_file)
