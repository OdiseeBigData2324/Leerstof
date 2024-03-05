# vraag 1: gemiddelde woordlengte
from mrjob.job import MRJob

class Vraag1(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield ("average length", len(word)) # spatie kan gebruikt worden om een unieke key te hebben als je ook iets doet woord per woord

    def reducer(self, word, counts):
        # zelf itereren
        # Calculate the average word length
        #total_length = 0
        #total_count = 0
        #for value in values:
        #    total_length += value
        #    total_count += 1
        #average_length = total_length / total_count
        #yield (key, average_length)

        # dit gaat niet werken want counts is een generator object
        # wijst naar het eerste object, en dat heeft een functie next()
        # je kan niet terug naar het begin gaan, je kan er maar 1 keer over itereren
        #counts = list(counts)
        mean = sum(counts) / len(counts)
        print('mean', mean)
        
        yield (word, mean)

if __name__ == '__main__':
    Vraag1.run()
