from movie.api.call import gen_url
import os

def test_gen_url_default():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    
def test_gen_url_defaults():
    r = gen_url(url_params={"multiMovieYn":"Y"})
    assert "multiMovieYn=Y" in r
