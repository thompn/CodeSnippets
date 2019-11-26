def clean_pandas(df):
    df.columns = df.columns.str.strip()\
    .str.lower()\
    .str.replace(' ', '_')\
    .str.replace('(', '')\
    .str.replace(')', '')
