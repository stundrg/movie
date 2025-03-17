import pandas as pd
from movie.api.call import gen_url, call_api, list2df, save_df
import os

def test_gen_url_default():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    
def test_gen_url_defaults():
    r = gen_url(url_params={"multiMovieYn":"Y", "repNationCd":"K"})
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