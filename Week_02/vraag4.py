# vraag 4: Het aantal woorden in de tekst

from mrjob.job import MRJob

class Vraag4(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield ("aantal woorden", 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    Vraag4.run()
