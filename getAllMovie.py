import jsonlines

movies = jsonlines.open("movies.json", "a")
newmovies = open("movie_1_15000.json", encoding='UTF-8')

for item in jsonlines.Reader(newmovies):
    if 'isMovie' not in item:
        movies.write(item)
movies.close()
newmovies.close()
