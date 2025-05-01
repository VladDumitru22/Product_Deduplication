import pandas as pd

opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")

def clean_text(value):
    if pd.isna(value):
        return ""
    else:
        value = str(value).lower()
        value = value.replace(" ", "")
        return value
    


def get_primary_key(row):
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
for index, row in opened_file.iterrows():
    primary_key = get_primary_key(row)
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

groups = []
used = set()

for i in range(len(opened_file)):
    if i in used:
        continue
    
    group = [i]
    for j in range(i + 1, len(opened_file)):
        if j not in used and compare_keys(opened_file.at[i, "signature"], opened_file.at[j, "signature"]):
            group.append(j)
            used.add(j)
    used.add(i)
    groups.append(group)

final_rows = []

for gorup in groups:
    temp_opened_file = opened_file.loc[group]
    row ={}

    for column in opened_file.columns:
        if column == "primary_key":
            continue
        
        values = []
        for value in temp_opened_file[column]:
            if value not in values and value is not None and value != "":
                values.append(str(value))
        if len(values) == 1:
            row[column] = values[0]
        elif len(value) > 1:
            row[column] = " || ".join(values)
        else:
            row[column] = ""
        
        final_rows.append(row)

final_file = pd.DataFrame(final_rows)
final_file = final_file.to_parquet("veridion_deduplicated.parquet", index=False)
