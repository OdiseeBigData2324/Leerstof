from mrjob.job import MRJob

class Vraag1(MRJob):

    def mapper(self, _, line):

        if line == "id,rated,created_at,last_move_at,turns,victory_status,winner,increment_code,white_id,white_rating,black_id,black_rating,moves,opening_eco,opening_name,opening_ply":
            return

        yield("row", 1)
        yield("columns", line.count(",") + 1)

    def reducer(self, key, counts):
        # dit is per key, je kan niet van de ene key aan data van een andere key

        if key == "row":
            yield("aantal rijen", sum(counts))
        elif key == "columns":
            yield("aantal kolommen", max(counts))
        

# de main functie komt hieronder
if __name__ == "__main__":
    Vraag1.run()
