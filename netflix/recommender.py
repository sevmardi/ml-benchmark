import time, sys, os
from pyspark.mllib.recommendation import ALS
from pyspark.conf import SparkContext, SparkConf



# calculates average rating from movieid/rating tuple
# ret: movieid[nr/rating, avg/rating]
def get_counts_and_averages(movidid_rating):
    nr_rating = len(movidid_rating[1])
    return movidid_rating[0], (nr_rating, float(sum(x for x in movidid_rating[1])) / nr_rating)


class NetflixRecommender:

    def count_ratings_and_average(self):
        movieids_ratings = self.ratings.map(
            lambda x: (x[0], x[2])).groupByKey()
        average_movieid_rating = movieids_ratings.map(get_counts_and_averages)
        self.movie_rating_count = average_movieid_rating.map(
            lambda x: (x[0], x[1][0]))

    def train(self):
        # train the model and write to self
        # raise NotImplementedError("Not implemented just yet")
        self.model = ALS.train(self.ratings, 10, seed=10L, iterations=10, lambda_=0.1)

    def predict_n_ratings(self, user_id, n_count, threshold):
        # raise NotImplementedError("Not implemented just yet")
        unrated = self.movies.filter(lambda rating: not rating[
                                     1] == user_id).map(lambda x: (user_id, x[0]))

        # get predicted ratings
        ratings = self.predict_full(unrated).filter(
            lambda r: r[2] >= threshold).takeOrdered(n_count, key=lambda x: -x[1])

        return ratings

    def predict_full(self, user_movie):
        # raise NotImplementedError("Not implemented just yet")
        pre_model = self.model.predictAll(user_movie).map(
            lambda x: (x.product, x.rating))

        return pre_model.join(self.movies_wtitle).join(self.movie_rating_count).map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

    def __init__(self, sc):
        
        dataset = os.path.join('.', '~/Documents/netflix/download')
        self.sc = sc
        print("\n\n ** loading ratings... **\n\n")

        self.ratings = self.sc.textFile(os.path.join(dataset, 'nf_10000')).map(lambda line: line.split(
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
        #the number of ratings a movie must have before it's concidered...
        rating_threshold = 15
        predicted = self.predict_n_ratings(
            user_id, number_recommendations, rating_threshold)
        print("recommended ratings for user id 6:\n%s" %
              "\n".join(map(str, predicted)))


if __name__ == '__main__':
    conf = SparkConf().setAppName('netflix-recommender')
    NetflixRecommender(SparkContext(conf=conf))
    print("\n ** DONE. **")
