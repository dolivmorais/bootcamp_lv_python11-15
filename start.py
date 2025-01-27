from fastapi import FastAPI, HTTPException
from faker import Faker
import pandas as pd
import random
import logging
import webbrowser

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
fake = Faker()

# Configuração do arquivo CSV
file_name = 'data/products.csv'

try:
    df = pd.read_csv(file_name)
    df['indice'] = range(1, len(df) + 1)
    df.set_index('indice', inplace=True)
    logging.info("Arquivo CSV carregado com sucesso!")
except FileNotFoundError:
    logging.error(f"Arquivo {file_name} não encontrado.")
    df = pd.DataFrame()  # Cria um DataFrame vazio para evitar falhas

lojapadraoonline = 11

@app.get("/")
async def hello_world():
    return {"message": "Jornada de Dados!"}


@app.get("/gerar_compra")
async def gerar_compra():
    if df.empty:
        raise HTTPException(status_code=500, detail="Arquivo CSV não carregado corretamente.")
    
    try:
        index = random.randint(1, len(df) - 1)
        produto = df.iloc[index]
        return {
            "client": fake.name(),
            "creditcard": fake.credit_card_provider(),
            "product": produto["Product Name"],
            "ean": int(produto["EAN"]),
            "price": round(float(produto["Price"]) * 1.2, 2),
            "clientPosition": fake.location_on_land(),
            "store": lojapadraoonline,
            "dateTime": fake.iso8601()
        }
    except Exception as e:
        logging.error(f"Erro ao gerar compra: {e}")
        raise HTTPException(status_code=500, detail="Erro ao gerar compra.")


@app.get("/gerar_compras/{numero_registro}")
async def gerar_compras(numero_registro: int):
    if numero_registro < 1:
        raise HTTPException(status_code=400, detail="O número de registros deve ser maior que 0.")

    if df.empty:
        raise HTTPException(status_code=500, detail="Arquivo CSV não carregado corretamente.")
    
    respostas = []
    for _ in range(numero_registro):
        try:
            index = random.randint(1, len(df) - 1)
            produto = df.iloc[index]
            compra = {
                "client": fake.name(),
                "creditcard": fake.credit_card_provider(),
                "product": produto["Product Name"],
                "ean": int(produto["EAN"]),
                "price": round(float(produto["Price"]) * 1.2, 2),
                "clientPosition": fake.location_on_land(),
                "store": lojapadraoonline,
                "dateTime": fake.iso8601()
            }
            respostas.append(compra)
        except IndexError as e:
            logging.warning(f"Erro de índice: {e}")
        except Exception as e:
            logging.error(f"Erro inesperado: {e}")
            respostas.append({
                "client": fake.name(),
                "creditcard": fake.credit_card_provider(),
                "product": "error",
                "ean": 0,
                "price": 0.0,
                "clientPosition": fake.location_on_land(),
                "store": lojapadraoonline,
                "dateTime": fake.iso8601()
            })

    return respostas

if __name__ == "__main__":
    import uvicorn
    # Abra o navegador na rota inicial
    webbrowser.open("http://127.0.0.1:8000")
    # Inicie o servidor
    uvicorn.run(app, host="127.0.0.1", port=8000)
