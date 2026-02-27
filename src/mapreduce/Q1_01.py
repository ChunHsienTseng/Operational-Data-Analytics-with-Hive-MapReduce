from mrjob.job import MRJob
from mrjob.step import MRStep

class CountMovieRatings(MRJob):

    def steps(self):    # Define the steps for the MapReduce job
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2)
        ]

    def mapper_1(self, _, line):  # Process each line of input
        parts = line.strip().split('\t')
        if len(parts) == 4:
            movie_id = parts[1]
            yield movie_id, 1

    def reducer_1(self, movie_id, values):   # Sum the counts for each movie_id
        yield None, (sum(values), movie_id)

    def reducer_2(self, _, values):  # Sort and limit the results
        for count, movie_id in sorted(values, reverse=True)[:5]: # sort by count in descending order and take top 5
            yield movie_id, count

if __name__ == '__main__':
    CountMovieRatings.run()