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
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

def fill_na_with_column(origin_df, c_name):
    df = origin_df.copy()
    for i, row in df.iterrows():
            if pd.isna(row[c_name]):
                same_movie_df = df[df["movieCd"] == row["movieCd"]]
                notna_idx = same_movie_df[c_name].dropna().first_valid_index()
                if notna_idx is not None:
                    df.at[i, c_name] = df.at[notna_idx, c_name]
    return df


def gen_unique(df: pd.DataFrame, drop_columns: list) -> pd.DataFrame:
    df_drop = df.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare','salesChange'])
    unique_df = df_drop.drop_duplicates(subset=['movieCd'])
    return unique_df

def re_ranking(df: pd.DataFrame) -> pd.DataFrame:
    df["rnum"] = df["audiCnt"].rank(method="dense", ascending=False).astype(int)
    df["rank"] = df["audiCnt"].rank(method="min", ascending=False).astype(int)
    return df

def fill_unique_ranking(ds: str, read_base, save_base):
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

def fillna_meta(previous_df, current_df):
    if previous_df is None:
        return current_df  # 이전 데이터가 없으면 현재 데이터 그대로 반환

    merged_df = current_df.copy()

    # movieCd를 기준으로 병합 (left join)
    merged_df = merged_df.merge(
        previous_df,
        on="movieCd",
        how="left",
        suffixes=("", "_prev")
    )

    # multiMovieYn 결측치 채우기
    merged_df["multiMovieYn"] = merged_df["multiMovieYn"].fillna(merged_df["multiMovieYn_prev"])

    # repNationCd 결측치 채우기
    merged_df["repNationCd"] = merged_df["repNationCd"].fillna(merged_df["repNationCd_prev"])

    # 불필요한 _prev 컬럼 제거
    merged_df.drop(columns=["multiMovieYn_prev", "repNationCd_prev"], inplace=True)

    return merged_df

def load_meta_data(base_path):
    """
    기존 메타 데이터를 로드하는 함수
    """
    meta_path = os.path.join(base_path, "meta/meta.parquet")
    return pd.read_parquet(meta_path) if os.path.exists(meta_path) else None


def save_meta_data(base_path, df):
    """
    병합된 메타 데이터를 저장하는 함수
    """
    meta_path = os.path.join(base_path, "meta/meta.parquet")
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    df.to_parquet(meta_path)
    return meta_path

def process_meta_data(base_path, ds_nodash):
    """
    기존 메타데이터와 새로운 데이터를 병합하고 저장하는 함수.
    """
    previous_df = load_meta_data(base_path)

    # 새로운 데이터 로드
    current_path = os.path.join(base_path, f"dailyboxoffice/dt={ds_nodash}")
    if not os.path.exists(current_path):
        print(f"🚨 데이터 파일 없음: {current_path}")
        return None

    current_df = pd.read_parquet(current_path)

    # 이전 데이터와 현재 데이터 병합하여 결측치 채움
    merged_df = fillna_meta(previous_df, current_df)

    # 병합된 데이터를 메타 데이터로 저장
    save_path = save_meta_data(merged_df, base_path)

    print(f"✅ 메타 데이터 저장 완료: {save_path}")
    return merged_df