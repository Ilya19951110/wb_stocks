import pandas as pd
import numpy as np


def prepare_values_for_sheets(df: pd.DataFrame):
    values = []

    for row in df.to_numpy():
        new_row = []
        for v in row:
            if pd.isna(v):
                new_row.append('')
            elif isinstance(v, (int, float, np.integer, np.floating)):
                # если float выглядит как целое → int
                if isinstance(v, (float, np.floating)) and v.is_integer():
                    new_row.append(int(v))
                else:
                    new_row.append(v)
            elif isinstance(v, pd.Timestamp):
                new_row.append(v.strftime("%Y-%m-%d"))
            else:
                new_row.append(str(v))

        values.append(new_row)
    return values
