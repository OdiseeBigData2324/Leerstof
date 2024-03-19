# aantal keer shcaak
from mrjob.job import MRJob
import csv
from io import StringIO
import numpy as np

col_victory_status = 5
col_winner = 6
col_white_rating = 9
col_black_rating = 11
col_moves = 12

class Vraag4(MRJob):

    
    def mapper(self, _, line):

        if line == "id,rated,created_at,last_move_at,turns,victory_status,winner,increment_code,white_id,white_rating,black_id,black_rating,moves,opening_eco,opening_name,opening_ply":
            return

        # line of text to list of columns
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file))    

        aantal_keer_schaak = cols[col_moves].count('+')
        aantal_keer_mat = cols[col_moves].count('#')
        
        yield ("aantal schaak", aantal_keer_mat + aantal_keer_schaak)
        
    def reducer(self, word, counts):

        counts = list(counts)

        yield("min", min(counts))
        yield("max", max(counts))
        
        yield("mean", sum(counts) / len(counts))
        yield("std", np.std(counts))

if __name__ == '__main__':
    Vraag4.run()
