from mrjob.job import MRJob
from mrjob.step import MRStep

class CountUserRatings(MRJob):

    def steps(self):    # Define the steps for the MapReduce job
        # First step: map user IDs to counts
        # Second step: reduce to sum counts and sort by count
        # Third step: sort the results
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2)
        ]

    def mapper_1(self, _, values):  # Process each line of input
        parts = values.strip().split('\t')
        if len(parts) == 4:
            key = parts[0]  
            yield key, 1

    def reducer_1(self, key, values):   # Sum the counts for each key
        yield None, (sum(values), key)

    def reducer_2(self, _, values):  # Sort and limit the results
        for value, key in sorted(values, reverse=True):
            yield key, value

if __name__ == '__main__':
    CountUserRatings.run()