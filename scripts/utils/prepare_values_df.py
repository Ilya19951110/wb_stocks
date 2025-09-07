import pandas as pd


def prepare_values_for_sheets(df: pd.DataFrame):
    values = []

    for row in df.to_numpy():
        new_row = []
        for v in row:
            if pd.isna(v):
                new_row.append('')
            elif isinstance(v, (int, float)):
                new_row.append(v)
            elif isinstance(v, pd.Timestamp):
                new_row.append(v.strftime("%Y-%m-%d"))
            else:
                new_row.append(str(v))

        values.append(new_row)
    return values
