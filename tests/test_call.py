import pandas as pd
from movie.api.call import (gen_url, call_api, list2df, save_df 
                            ,fill_na_with_column , gen_unique, re_ranking,fillna_meta,
                            fill_unique_ranking)
import os

def test_gen_url_default():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    
def test_gen_url_defaults():
    r = gen_url(url_param={"multiMovieYn":"Y", "repNationCd":"K"})
    print(r)
    assert "multiMovieYn=Y" in r
    assert "repNationCd=K" in r

def test_call_api():
    r = call_api()
    assert isinstance(r, list)
    assert isinstance(r[0]['rnum'], str)
    assert len(r) == 10
    for e in r:
        assert isinstance(e,dict)
    
def test_list2df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    assert isinstance(df, pd.DataFrame)
    assert len(data) == len(df)
    assert set(data[0].keys()).issubset(set(df.columns)) 
    assert "dt" in df.columns, "df 컬럼이 있어야 함"
    assert (df["dt"] == ymd).all(), "모든 컬럼에 입력된 날짜 값이 존재 해야 함"
    
def test_save_df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    base_path = "~/temp/movie"
    r = save_df(df, base_path)
    assert r == f"{base_path}/dt={ymd}"
    print("save_path:", r)
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns

def test_list2df_check_num():
    """df의 특정 컬럼이 숫자로 변환되었는지 확인"""
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange', 'audiInten', 'audiChange']
    
    ymd = "20210101"
    data = call_api(dt=ymd)  
    df = list2df(data, ymd)
    
    from pandas.api.types import is_numeric_dtype
    
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')  # 숫자로 변환 (변환 불가한 값은 NaN)

    for c in num_cols:
        assert df[c].dtype in ['float64', 'int64'],f"{c}가 숫자가 아님" # 숫자로 변환되었는지 확인
        assert is_numeric_dtype(df[c]), f"{c}가 숫자가 아님"  # 숫자로 변환되었는지 확인
        
def test_save_df_url_param():
    ymd = "20210101"
    url_params = {"multiMovieYn": "Y"}
    # TO Airflow Dag
    data = call_api(dt=ymd, url_param=url_params)
    df = list2df(data, ymd, url_params)
    base_path = "~/temp/movie"
    partitions = ['dt'] + list(url_params.keys())
    save_path = save_df(df, base_path,partitions)
    
    assert save_path == f"{base_path}/dt={ymd}/multiMovieYn=Y"
    assert f"{base_path}/dt={ymd}" in save_path
    print("save_path", save_path)
    read_df = pd.read_parquet(save_path)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns
    
def test_list2df_url_param():
    ymd = "20210101"
    url_param = {"multiMovieYn": "Y"}
    data = call_api(dt=ymd, url_param=url_param)
    df = list2df(data, ymd, url_param)
    assert "multiMovieYn" in df.columns
    assert (df["multiMovieYn"] == "Y").all()
    assert (df["dt"] == ymd).all()
 
def test_merge_df():
    PATH = "~/data/movies/dailyboxoffice/dt=20240101"
    df = pd.read_parquet(PATH)
    assert len(df) == 50
    
    df1 = fill_na_with_column(df,'multiMovieYn')
    assert df1['multiMovieYn'].isna().sum() == 5
    
    df2 = fill_na_with_column(df1,'repNationCd')
    assert df2['repNationCd'].isna().sum() == 5
    
    drop_columns = ['salesShare','rnum','salesChange','rank','rankInten']
    unique_df = gen_unique(df = df2, drop_columns = drop_columns)
    assert len(unique_df) == 25
    
def test_merge_df_save():
    ds = "20240101"
    PATH = f"~/data/movies/dailyboxoffice/dt={ds}"
    
    df = pd.read_parquet(PATH)
    df1 = fill_na_with_column(df,'multiMovieYn')
    df2 = fill_na_with_column(df1,'repNationCd')
    drop_columns = ['salesShare','rnum','salesChange','rank','rankInten']
    unique_df = gen_unique(df = df2, drop_columns = drop_columns)
    
    # merge.data --> /home/wsl/data/movies/merge/dailyboxoffice/dt=20240101
    unique_df['dt'] = ds
    base_path = "/home/wsl/temp/data/movies/merge/dailyboxoffice"
    save_path = save_df(unique_df, base_path)
    assert save_path == f"{base_path}/dt={ds}"
    
    
def test_fill_unique_ranking():
    ds = "20240101"
    read_base = "/home/wsl/data/movies/dailyboxoffice"
    save_base = "/home/wsl/temp/data/movies/merge/dailyboxoffice"
    save_path = fill_unique_ranking(ds, read_base, save_base)
    assert save_path == f"{save_base}/dt={ds}"
    
def test_fillna_meta():
    previous_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "Y", "N"],
            "repNationCd": ["K", "F", None],
        }
    )

    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert not r_df.isnull().values.any(), "결과 데이터프레임에 NaN 또는 None 값이 있습니다!"
    
def test_fillna_meta_none_previous_df():
    previous_df = None

    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert r_df.equals(current_df), "r_df는 current_df와 동일해야 합니다!"