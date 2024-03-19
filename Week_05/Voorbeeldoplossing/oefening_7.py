
from mrjob.job import MRJob
import csv
from io import StringIO
import numpy as np
from collections import Counter

col_start = 2
col_last_move = 3
col_turns = 4
col_victory_status = 5
col_winner = 6
col_white_rating = 9
col_black_rating = 11
col_moves = 12
col_openingen = 14

class Vraag7(MRJob):

    
    def mapper(self, _, line):

        if line == "id,rated,created_at,last_move_at,turns,victory_status,winner,increment_code,white_id,white_rating,black_id,black_rating,moves,opening_eco,opening_name,opening_ply":
            return

        # line of text to list of columns
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file)) 

        last_move = float(cols[col_last_move])
        first_move = float(cols[col_start])
        yield("aantal minuten", (last_move - first_move) / 1000 / 60)

        aantal_zetten = int(cols[col_turns])
        if aantal_zetten > 0:
            yield("aantal zetten", aantal_zetten)

        if cols[col_winner] == 'white':
            yield("opening", cols[col_openingen])

    def reducer(self, key, counts):

        counts = list(counts)

        if key == "aantal minuten":
            yield('min', min(counts))
            yield('max', max(counts))
            yield('mean', sum(counts)/len(counts))
        elif key == 'opening':
            counts = Counter(counts)
            yield('beste strategie', counts.most_common(1)[0])
        elif key == 'aantal zetten':
            bin_limits = [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51]
            hist, bin_edges = np.histogram(counts, bins=bin_limits)

            for i in range(len(bin_edges) - 1):
                yield(f"bin_{bin_edges[i]}_{bin_edges[i+1]}", int(hist[i]))

if __name__ == '__main__':
    Vraag7.run()
