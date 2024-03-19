from mrjob.job import MRJob
import csv
from io import StringIO

col_survived = 1
col_gender = 4
col_age = 5

class Structured(MRJob):
    def mapper(self, _, line):

        # skip de header rij
        if line == "PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked":
            return

        # converteer lijn naar rij van column values
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file))  # csv.reader returned een generator die over de lijnen itereert

        if cols[col_age] != '':
            yield ("leeftijd", float(cols[col_age]))

        if cols[col_survived] != '':
            yield('overleefd', float(cols[col_survived]))

        if cols[col_gender] != '':
            yield('perc_man', int(cols[col_gender] == 'male'))

        if cols[col_gender] != '' and cols[col_survived] != '':
            if cols[col_gender] == 'female':
                yield('vrouw_overleefd', float(cols[col_survived]))

    def reducer(self, word, counts):

        if word == 'leeftijd':
            counts = list(counts)
            yield(word, sum(counts)/len(counts))
        elif word == 'overleefd' or word =='perc_man' or word == 'vrouw_overleefd':
            counts = list(counts)
            yield(word, sum(counts)/len(counts) * 100)         

if __name__ == '__main__':
    Structured.run()
