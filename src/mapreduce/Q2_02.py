from mrjob.job import MRJob
from mrjob.step import MRStep

class JoinUserOccupation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_aggregate)
        ]

    def mapper(self, _, line):
        # Case 1: u.user file (contains "|")
        fields = line.strip().replace('"', '').split('|')  # Remove quotes and split by '|'
        if len(fields) == 5:    # Ensure there are enough fields
            user_id = fields[0].strip()   # strip any quotes
            occupation = fields[3].strip()
            yield str(user_id), ("OCCUPATION", occupation)
        else:    # Case 2: out1.txt (output from phase 1)
            fields = line.strip().replace('"', '').split('\t')  # Remove quotes and split by '\t'
            if len(fields) == 2:
                user_id = fields[0].strip()
                count = fields[1].strip()
                try:
                    yield str(user_id), ("COUNT", int(count))
                except ValueError:
                    pass  # Ignore bad lines

    def reducer(self, user_id, values):     # Combine occupation and count
        occupation = None
        count = None
        for tag, v in values:
            # Decode bytes if necessary
            if tag == "OCCUPATION":
                occupation = v
            elif tag == "COUNT":
                count = v
        if occupation and count:
            yield occupation, count
    
    def reducer_aggregate(self, occupation, counts):
        yield occupation, sum(counts)

if __name__ == '__main__':
    JoinUserOccupation.run()