import re
import pandas as pd

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def convert_dict_keys_snake_case(d):
    return {camel_to_snake(k): v for k, v in d.items()}

def infer_col_types_from_df(df: pd.DataFrame) -> dict:
    """DataFrame으로부터 컬럼 타입(str, int, float)을 추론하여 dict로 반환"""
    type_mapping = {
        "int64": "int",
        "float64": "float",
        "object": "str",
        "string": "str",
        "bool": "bool"
    }

    col_types = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        col_types[col] = type_mapping.get(dtype, "str")  # 기본은 str
    return col_types

def auto_cast_dataframe(df: pd.DataFrame, col_types: dict) -> pd.DataFrame:
    """컬럼별 타입을 자동으로 변환 (int, float, str 등)"""
    for col, dtype in col_types.items():
        if col in df.columns:
            if dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
            elif dtype == "str":
                df[col] = df[col].astype(str)
            elif dtype == "bool":
                df[col] = df[col].astype(bool)
    return df