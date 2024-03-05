
from mrjob.job import MRJob
import csv
from io import StringIO
import numpy as np
import logging

col_survived = 1
col_class = 2
col_name = 3
col_sex = 4
col_age = 5
col_embarked = -1
col_cabin = -2
col_fare = -3

class MR_Structured_Oef(MRJob):
    
    def mapper(self, _, line):
        
        logger = logging.getLogger(__name__)

        if line == "PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked":
            return

        #logger.info(f"Processed line: {line}")
    
        # line of text to list of columns
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file)) 

        if cols[col_class] != '':
            yield("klasse_" + cols[col_class], 1)
        else:
            yield("klasse_unknown", 1)

        if cols[col_name] != '':
            yield('lengte_naam', (len(cols[col_name]), cols[col_name]))

        if cols[col_fare] != '':
            yield('fare', float(cols[col_fare]))

        if cols[col_cabin] == '':
            yield("nulls_cabin", 1)
        
        if cols[col_embarked] != '':
            yield("embarkment point", cols[col_embarked])

        yield('aantal_passagiers', 1)
       
    def reducer(self, word, counts):

        if word == "lengte_naam":
            current_max = 0
            current_max_naam = ''
            for naam in counts:
                if naam[0] > current_max:
                    current_max = naam[0]
                    current_max_naam = naam[1]
            yield(word, current_max_naam)
        elif word == "fare":
            fares = list(counts)

            yield("min fare", min(fares))
            yield("max fare", max(fares))
            yield("mean fare", sum(fares)/len(fares))
            yield("std fare", np.std(fares))
        elif word == 'embarkment point':
            points = set(counts)
            yield('aantal unieke embarkment points', len(points))
        else:
            yield(word, sum(counts))

if __name__ == '__main__':
    MR_Structured_Oef.run()
