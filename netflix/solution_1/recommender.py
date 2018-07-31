import time
import sys
import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf
from datetime import datetime

# calculates average rating from movieid/rating tuple
# ret: movieid[nr/rating, avg/rating]
def get_counts_and_averages(movidid_rating):
    nr_rating = len(movidid_rating[1])
    return movidid_rating[0], (nr_rating, float(sum(x for x in movidid_rating[1])) / nr_rating)


class NetflixRecommender:

    def __init__(self, sc):

        dataset = os.path.join('.', '../data/netflix')
        # das4 = os.path.join('.', '/data/scratch/vw/netflix')
        # latinum = os.path.join('.', '/data/vw/netflix-prize-dataset')
        self.sc = sc

        print("\n\n ** loading ratings... **\n\n")
        self.ratings = self.sc.textFile(os.path.join(dataset, 'training_set')).map(lambda line: line.split(
            ",")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()

        print("\n\n ** loading movie data **\n\n")
        self.movies = self.sc.textFile(os.path.join(dataset, 'movie_titles')).map(
            lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()
        self.movie_wtitle = self.movies.map(
            lambda x: (int(x[0]), x[2])).cache()

        print("\n\n ** loading complete ** \n\n")

        self.count_ratings_and_average()

        print("\n\n ** train model ** \n\n")

        self.train()

        print("\n\n ** results ** \n\n")

        user_id = 6
        number_recommendations = 15
        # the number of ratings a movie must have before it's concidered...
        rating_threshold = 15
        predicted = self.predict_n_ratings(user_id, number_recommendations, rating_threshold)
        print("recommended ratings for user id 6:\n%s" % "\n".join(map(str, predicted)))

    def count_ratings_and_average(self):
        movieids_ratings = self.ratings.map(
            lambda x: (x[0], x[2])).groupByKey()
        average_movieid_rating = movieids_ratings.map(get_counts_and_averages)
        self.movie_rating_count = average_movieid_rating.map(
            lambda x: (x[0], x[1][0]))

    def train(self):
        self.rank = 15
        self.lambda_ = 0.09
        self.numIterations = 10
        # train the model and write to self
        self.model = ALS.train(self.ratings, 10, seed=10L, iterations=self.numIterations, lambda_=self.lambda_)
        # self.model = ALS.train(self.ratings, 10, seed=10L, iterations=10, lambda_=0.1)
        print("Finished training at :")
        print(str(datetime.now()))


    def predict_n_ratings(self, user_id, n_count, threshold):
        unrated = self.movies.filter(lambda rating: not rating[1] == user_id).map(lambda x: (user_id, x[0]))

        # get predicted ratings
        ratings = self.predict_full(unrated).filter(
            lambda r: r[2] >= threshold).takeOrdered(n_count, key=lambda x: -x[1])

        return ratings

    def predict_full(self, user_movie):
        pre_model = self.model.predictAll(user_movie).map(
            lambda x: (x.product, x.rating))

        return pre_model.join(self.movies_wtitle).join(self.movie_rating_count).map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))


if __name__ == '__main__':
    conf = SparkConf().setAppName('netflix-recommender')
    NetflixRecommender(SparkContext(conf=conf))
    print("\n ** DONE. **")
