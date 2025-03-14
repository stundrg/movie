import os
import requests

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY = os.getenv("MOVIE_KEY")

def gen_url(dt="20120101",url_params={}):
    "호출 URL 생성, url_params 이 입력되면 multiMocvieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    # TODO: url_params 처리
    
       
    for k, v in url_params.items():
        url = f"{url}&{k}={v}"
    # if url_params:
    #     param_str = "&".join(f"{key}={value}" for key, value in url_params.items())
    #     url = f"{url}&{param_str}"
        
    return url

def call_api(dt="20120101", url_params={}): 
    url = gen_url(dt, url_params)
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()['boxOfficeResult']['dailyBoxOfficeList']
    else:
        return []
    return data

