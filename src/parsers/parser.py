import pandas as pd


class PokemonParser:
    def __init__(self):
        pass

    def parse(self, records: list) -> pd.DataFrame:
        df = pd.DataFrame(records)

        print("Parsed records into DataFrame, total records:", df.shape[0])

        return df