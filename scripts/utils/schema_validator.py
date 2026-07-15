# Schema Validation Helper

def validate_schema(df, required_cols, source_name):
    actual_cols = set(df.columns)
    missing = required_cols - actual_cols
    extra = actual_cols - required_cols

    if missing:
        raise Exception(f"SCHEMA VALIDATION FAILED for {source_name}: missing required columns {missing}")

    if extra:
        print(f"WARNING: {source_name} has unexpected new columns: {extra} — continuing anyway")

    return True
