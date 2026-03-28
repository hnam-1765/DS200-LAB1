from common import exercise_2, load_movies, load_ratings


def main():
    movies = load_movies()
    ratings = load_ratings()
    for line in exercise_2(movies, ratings):
        print(line)


if __name__ == "__main__":
    main()
