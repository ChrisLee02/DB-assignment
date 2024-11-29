import pandas as pd

# CSV 파일 불러오기
file_path = "data.csv"  # 파일 경로를 적절히 수정
data = pd.read_csv(file_path)

# d_id 기준으로 유니크한 값 추출 후 정렬
unique_d = data[["d_id", "d_title", "d_name"]].drop_duplicates().sort_values("d_id")

# u_id 기준으로 유니크한 값 추출 후 정렬
unique_u = data[["u_id", "u_name", "u_age"]].drop_duplicates().sort_values("u_id")

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
print(unique_d)
print(unique_u)
pd.reset_option("display.max_rows")
pd.reset_option("display.max_columns")
pd.reset_option("display.max_colwidth")
