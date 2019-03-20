#-*-coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from metrics import correlation
from math import sqrt
from itertools import combinations

PRIOR_COUNT = 10
PRIOR_CORRELATION = 0

class MoviesSimilarities(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
             MRStep(mapper=self.group_by_user_rating,
                    reducer=self.count_ratings_users_freq),
             MRStep(mapper=self.pairwise_items,
                    reducer=self.calculate_similarity),
             MRStep(mapper=self.calculate_ranking,
                    reducer=self.top_similar_items)
        ]

    def group_by_user_rating(self, key, line):
        """
        Emit the user_id and group by their ratings (item and rating)

        17  70,3
        35  21,1
        49  19,2
        49  21,1
        49  70,4
        87  19,1
        87  21,2
        98  19,2

        """
        user_id, item_id, rating = line.split('|')
        #print(user_id, item_id, rating)
        
        yield  user_id, (item_id, float(rating))

    def count_ratings_users_freq(self, user_id, values):
        """
        For each user, emit a row containing their "postings"
        (item,rating pairs)
        Also emit user rating sum and count for use later steps.

        17    1,3,(70,3)
        35    1,1,(21,1)
        49    3,7,(19,2 21,1 70,4)
        87    2,3,(19,1 21,2)
        98    1,2,(19,2)
        """
        item_count = 0
        item_sum = 0
        final = []
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))

        yield user_id, (item_count, item_sum, final)

    def pairwise_items(self, user_id, values):
        '''
        The output drops the user from the key entirely, instead it emits
        the pair of items as the key:

        19,21  2,1
        19,70  2,4
        21,70  1,4
        19,21  1,2

        This mapper is the main performance bottleneck.  One improvement
        would be to create a java Combiner to aggregate the
        outputs by key before writing to hdfs, another would be to use
        a vector format and SequenceFiles instead of streaming text
        for the matrix data.
        '''
        item_count, item_sum, ratings = values
        #bottleneck at combinations
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1])

    def calculate_similarity(self, pair_key, lines):
        '''
        Sum components of each corating pair across all users who rated both
        item x and item y, then calculate pairwise pearson similarity and
        corating counts.  The similarities are normalized to the [0,1] scale
        because we do a numerical sort.

        19,21   0.4,2
        21,19   0.4,2
        19,70   0.6,1
        70,19   0.6,1
        21,70   0.1,1
        70,21   0.1,1
        '''
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1
        corr_sim = correlation(n, sum_xy, sum_x, sum_y, sum_xx, sum_yy)

        yield (item_xname, item_yname), (corr_sim, n)

    def calculate_ranking(self, item_keys, values):
        '''
        Emit items with similarity in key for ranking:

        19,0.4    70,1
        19,0.6    21,2
        21,0.6    19,2
        21,0.9    70,1
        70,0.4    19,1
        70,0.9    21,1
        '''
        corr_sim, n = values
        item_x, item_y = item_keys
        if int(n) > 0:
            yield (item_x, corr_sim), (item_y, n)

    def top_similar_items(self, key_sim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:

        De La Soul;A Tribe Called Quest;0.6;1
        De La Soul;2Pac;0.4;2
        '''
        item_x, corr_sim = key_sim
        for item_y, n in similar_ns:
            yield None, (item_x, item_y, corr_sim, n)

if __name__ == '__main__':
    MoviesSimilarities.run()
