# vraag 3: * Het aantal woorden dat begint met elke letter

from mrjob.job import MRJob

class Vraag3(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            if len(word) > 0 and word[0].isalpha():
                yield (word[0], 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    Vraag3.run()
