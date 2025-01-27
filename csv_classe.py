import pandas as pd


class CSVProcessor:
    def __init__(self,file_path: str):
        self.file_path = file_path
        self.df = None

    
    def carregar_csv(self):
        self.df = pd.read_csv(self.file_path)
        return self.df

    def filtrar_por(self,coluna, atributo):
        return self.df[self.df[coluna] == atributo]
    