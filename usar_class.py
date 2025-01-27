from csv_classe import CSVProcessor

arquivo_csv = './exemplo.csv'
filtro = 'estado'
limite = 'sp'

arquivo_CSV = CSVProcessor(arquivo_csv)
arquivo_CSV.carregar_csv()
print(arquivo_CSV.filtrar_por(filtro,limite))