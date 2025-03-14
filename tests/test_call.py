from movie.api.call import gen_url, call_api
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
    