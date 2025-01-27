import pandas as pd

df = pd.read_csv('./exemplo.csv')

df_filtrado = df[df['estado'] == 'sp']

print(df_filtrado)