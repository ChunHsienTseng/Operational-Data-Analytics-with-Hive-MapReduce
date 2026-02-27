from mrjob.job import MRJob
from mrjob.step import MRStep

class JoinMovieTitles(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Case 1: u.item file (contains "|")
        fields = line.strip().replace('"', '').split('|')   # Remove quotes and split by '|'
        if len(fields) >= 2:    # Ensure there are enough fields
            movie_id = fields[0].strip()  # strip any quotes
            title = fields[1].strip()
            yield str(movie_id), ("TITLE", title)
        else:    # Case 2: out1.txt (output from phase 1)
            fields = line.strip().replace('"', '').split('\t')
            if len(fields) == 2:
                movie_id = fields[0].strip()
                count = fields[1].strip()
                try:
                    yield str(movie_id), ("COUNT", int(count))
                except ValueError:
                    pass  # Ignore bad lines

    def reducer(self, movie_id, values):    # Combine title and count
        title = None
        count = None
        for tag, v in values:
            # Decode bytes if necessary
            if tag == "TITLE":
                title = v
            elif tag == "COUNT":
                count = v
        if title and count:
            yield title, count

if __name__ == '__main__':
    JoinMovieTitles.run()