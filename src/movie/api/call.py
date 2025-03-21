import os
import requests
import pandas as pd

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    "호출 URL 생성, url_param 이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    
    # TODO = url_param 처리
    for k, v in url_param.items():
        url = url + f"&{k}={v}"
        
    return url

def call_api(dt="20120101", url_param={}):
    url = gen_url(dt, url_param)
    data = requests.get(url)
    j = data.json()
    return j['boxOfficeResult']['dailyBoxOfficeList']

def list2df(data: list, dt: str, url_param={}):
    df = pd.DataFrame(data)
    df['dt'] = dt
    # df['multiMovieYn'] = 'Y'
    for k,v in url_param.items():
        df[k] = v
    
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange', 'audiInten', 'audiChange']

    # for col_name in num_cols:
    #    df[col_name] = pd.to_numeric(df[col_name])
    df[num_cols] = df[num_cols].apply(pd.to_numeric) 

    return df

def save_df(df, base_path, partitions=['dt']):
    import pandas as pd
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

def fill_na_with_column(origin_df, c_name):
    import pandas as pd
    df = origin_df.copy()
    for i, row in df.iterrows():
            if pd.isna(row[c_name]):
                same_movie_df = df[df["movieCd"] == row["movieCd"]]
                notna_idx = same_movie_df[c_name].dropna().first_valid_index()
                if notna_idx is not None:
                    df.at[i, c_name] = df.at[notna_idx, c_name]
    return df


def gen_unique(df: pd.DataFrame, drop_columns: list) -> pd.DataFrame:
    import pandas as pd
    df_drop = df.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare','salesChange'])
    unique_df = df_drop.drop_duplicates(subset=['movieCd'])
    return unique_df

def re_ranking(df: pd.DataFrame) -> pd.DataFrame:
    df["rnum"] = df["audiCnt"].rank(method="dense", ascending=False).astype(int)
    df["rank"] = df["audiCnt"].rank(method="min", ascending=False).astype(int)
    return df

def fill_unique_ranking(ds: str, read_base, save_base):
    import pandas as pd
    PATH = f"{read_base}/dt={ds}"
    
    df = pd.read_parquet(PATH)
    df1 = fill_na_with_column(df,'multiMovieYn')
    df2 = fill_na_with_column(df1,'repNationCd')

    drop_columns = ['salesShare','rnum','salesChange','rank','rankInten']
    unique_df = gen_unique(df = df2, drop_columns = drop_columns)
    
    rdf = re_ranking(unique_df)

    rdf['dt'] = ds
    save_path = save_df(rdf, save_base)
    return save_path

def load_meta_data(base_path):
    """
    기존 메타 데이터를 로드하는 함수
    """
    import pandas as pd
    meta_path = os.path.expanduser(f"{base_path}/meta/meta.parquet")
    return pd.read_parquet(meta_path) if os.path.exists(meta_path) else None

def save_meta_data(base_path, new_df):
    import pandas as pd
    meta_path = os.path.expanduser(f"{base_path}/meta/meta.parquet")
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)

    if os.path.exists(meta_path):
        existing_df = pd.read_parquet(meta_path)
        merged_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["movieCd"], keep="last")
    else:
        merged_df = new_df

    merged_df.to_parquet(meta_path)
    return meta_path

def fillna_meta(previous_df, current_df):
    """ 
    이전 데이터를 활용하여 현재 데이터의 NaN 값을 채움 
    """
    import pandas as pd
    if previous_df is None:
        return current_df  # 이전 데이터가 없으면 현재 데이터 그대로 반환

    prev_df = previous_df[["movieCd", "multiMovieYn", "repNationCd"]].drop_duplicates("movieCd")

    merged_df = current_df.copy()

    merged_df = merged_df.merge(
        prev_df,
        on="movieCd",
        how="left",
        suffixes=("", "_prev")
    )

    merged_df["multiMovieYn"] = merged_df["multiMovieYn"].fillna(merged_df["multiMovieYn_prev"])
    merged_df["repNationCd"] = merged_df["repNationCd"].fillna(merged_df["repNationCd_prev"])

    merged_df.drop(columns=["multiMovieYn_prev", "repNationCd_prev"], inplace=True)

    return merged_df
