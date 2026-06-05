import pandas as pd
import time
import re

path = r"spark\douban_movies.csv"

df = pd.read_csv(path)

def pick_col(columns, candidates):
    lower_map = {c.lower(): c for c in columns}
    for key in candidates:
        for low, real in lower_map.items():
            if key.lower() in low:
                return real
    return None

title_col = pick_col(df.columns, ["title", "name", "movie", "电影", "片名", "名称"])
rating_col = pick_col(df.columns, ["rating", "rate", "score", "评分", "分数"])
year_col = pick_col(df.columns, ["year", "date", "time", "年份", "上映", "年代"])
genre_col = pick_col(df.columns, ["genre", "type", "category", "类型", "类别"])

start = time.time()

work = df.drop_duplicates().copy()

if title_col is not None:
    work = work[work[title_col].notna() & (work[title_col].astype(str).str.strip() != "")]

if rating_col is not None:
    work["rating_num"] = pd.to_numeric(work[rating_col], errors="coerce")
    work = work[work["rating_num"].notna()]
else:
    work["rating_num"] = 0.0

if year_col is not None:
    work["year_num"] = work[year_col].astype(str).str.extract(r"(\d{4})")[0]
    work["year_num"] = pd.to_numeric(work["year_num"], errors="coerce")

if genre_col is not None:
    work["genre_clean"] = work[genre_col].fillna("Unknown").astype(str).str.strip()
    work.loc[work["genre_clean"] == "", "genre_clean"] = "Unknown"
else:
    work["genre_clean"] = "Unknown"

result = (
    work.groupby("genre_clean")
    .agg(movie_count=("genre_clean", "count"), avg_rating=("rating_num", "mean"))
    .sort_values("movie_count", ascending=False)
)

end = time.time()

print("===== Pandas 性能测试 =====")
print("原始行数:", len(df))
print("清洗后行数:", len(work))
print(result.head(20))
print(f"Pandas query time: {end - start:.4f} seconds")