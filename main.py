import pandas as pd
import numpy as np
from rapidfuzz import fuzz

def sow_changes(initial_file, final_file):
    print(f"Rânduri inițiale: {len(initial_file)}")
    print(f"Rânduri după deduplicare: {len(final_file)}")
    print(f"Rânduri eliminate: {len(initial_file) - len(final_file)}")
    

opened_file = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")

def clean_text(value):
    if isinstance(value, (list, np.ndarray)):
        if len(value) == 0:
            return ""
        value = value[0]

    if pd.isna(value) or str(value).strip() == "":
        return ""
    
    return str(value).strip().lower()
    


def get_primary_key(row):
    primary_key = {
        "product_identifier": clean_text(row.get("product_identifier", "")),
        "brand": clean_text(row.get("brand", "")),
        "materials": clean_text(row.get("materials", "")),
        "form": clean_text(row.get("form", "")),
        "size": clean_text(row.get("size", "")),
        "color": clean_text(row.get("color", "")),
        "product_name": clean_text(row.get("product_name", ""))
    }
    return primary_key



opened_file["signature"] = opened_file.apply(get_primary_key, axis=1)


def fuzzy_match(key1, key2, threshold=85, min_matches=5):
    matches = 0
    for key in key1:
        if key1[key] and key2[key]:
            score = fuzz.token_sort_ratio(key1[key], key2[key])
            if score >= threshold:
                matches += 1
    return matches >= min_matches

groups = []
used = [False] * len(opened_file)

for i in range(len(opened_file)):
    if used[i]:
        continue
    
    group = [i]
    key_i = opened_file.at[i, "signature"]
    for j in range(i + 1, len(opened_file)):
        if not used[j] and fuzzy_match(key_i, opened_file.at[j, "signature"]):
            group.append(j)
            used[j] = True
    used[i] = True
    groups.append(group)

final_rows = []

for group in groups:
    temp_opened_file = opened_file.loc[group]
    row ={}

    for column in opened_file.columns:
        if column == "primary_key":
            continue
        
        values = temp_opened_file[column].dropna().astype(str).unique()
        values = [v.strip() for v in values if v.strip() and v != "[]"]
        row[column] = values[0] if len(values) == 1 else " || ".join(sorted(set(values)))
    final_rows.append(row)

final_file = pd.DataFrame(final_rows)
final_file.to_parquet("veridion_deduplicated.parquet", index=False)

sow_changes(opened_file, final_file)
