from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageRatingUnder25(MRJob):

    def steps(self):    # Define the steps for the MapReduce job
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_join),
            MRStep(reducer=self.reducer_avg)
        ]

    def mapper(self, _, line):  # Process each line of input
        fields = line.strip().split('|')
        if len(fields) == 5:  # u.user
            user_id, age = fields[0], int(fields[1])
            if age < 25:    # Check if the user is under 25
                yield user_id, ("AGE", None)
        else:  # u.data
            fields = line.strip().split('\t')
            if len(fields) == 4:
                user_id, _, rating, _ = fields
                yield user_id, ("RATING", int(rating))

    def reducer_join(self, user_id, values):    # Combine age and ratings for each user
        has_young_age = False
        ratings = []
        for key, val in values:
            if key == "AGE":    # Check if the user is under 25
                has_young_age = True
            elif key == "RATING":
                ratings.append(val)
        
        if has_young_age:   # Only yield if the user is under 25
            for r in ratings:
                yield "AVG", (r, 1)  # (rating, 1 count)

    def reducer_avg(self, key, values):   # Calculate the average rating for users under 25
        total = count = 0
        for r, c in values:
            total += r
            count += c
        yield "Average rating by users under age 25", total / count if count != 0 else 0

if __name__ == '__main__':
    AverageRatingUnder25.run()