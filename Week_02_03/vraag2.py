# vraag 2: het aantal keer dat elk karakter voorkomt

from mrjob.job import MRJob

class MRWordCount(MRJob):
    def mapper(self, _, line):
        for c in line:
            if c.isalpha():
                yield (c.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()
