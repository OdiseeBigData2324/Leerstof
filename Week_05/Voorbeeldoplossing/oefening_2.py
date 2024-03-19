from mrjob.job import MRJob
import csv
from io import StringIO

col_winner = 6
col_white_rating = 9
col_black_rating = 11

class Vraag2(MRJob):

    def mapper(self, _, line):

        if line == "id,rated,created_at,last_move_at,turns,victory_status,winner,increment_code,white_id,white_rating,black_id,black_rating,moves,opening_eco,opening_name,opening_ply":
            return
        
        # line of text to list of columns
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file))  

        speler1 = float(cols[col_white_rating])
        speler2 = float(cols[col_black_rating])
        winner = cols[col_winner]

        yield("verschil", abs(speler1-speler2))

        if winner=="white" and speler1 >= speler2:
            yield("sterkste", 1)
        elif winner=="black" and speler2 >= speler1:
            yield("sterkste", 1)
        else:
            yield("sterkste", 0)

    def reducer(self, key, counts):
        # dit is per key maar beide keys kunnen op dezelfde manier verwerkt worden

        counts = list(counts)

        yield(key, sum(counts)/ len(counts))
        

# de main functie komt hieronder
if __name__ == "__main__":
    Vraag2.run()
