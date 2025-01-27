import requests
import pandas as pd
import os

# Базовий URL ендпоінта
BASE_URL = "https://api.bybit.com/v5/market/tickers"

# Конфігурація
category = "spot"  # Категорія ринку
symbol = "BTCUSDT"  # Торгова пара

# Параметри запиту
params = {
    "category": category,
    "symbol": symbol,
}

# Визначення шляху до папки data
current_dir = os.path.dirname(os.path.abspath(__file__))  # Поточна директорія файлу
data_dir = os.path.join(current_dir, '..', 'data')  # Перехід на рівень вище і до папки data
os.makedirs(data_dir, exist_ok=True)  # Створення папки, якщо вона не існує


# Виконання запиту
try:
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()  # Перевірка статусу відповіді
    data = response.json()  # Парсинг JSON

    if data["retCode"] == 0:
        tickers = data["result"]["list"]

        # Збереження важливих параметрів
        important_data = []
        for ticker in tickers:
            important_data.append({
                "symbol": ticker["symbol"],
                "bid1Price": float(ticker["bid1Price"]),
                "bid1Size": float(ticker["bid1Size"]),
                "ask1Price": float(ticker["ask1Price"]),
                "ask1Size": float(ticker["ask1Size"]),
                "lastPrice": float(ticker["lastPrice"]),
                "prevPrice24h": float(ticker["prevPrice24h"]),
                "price24hPcnt": float(ticker["price24hPcnt"]),
                "highPrice24h": float(ticker["highPrice24h"]),
                "lowPrice24h": float(ticker["lowPrice24h"]),
                "volume24h": float(ticker["volume24h"]),
            })

        # Перетворення даних у DataFrame
        df = pd.DataFrame(important_data)

        # Збереження у CSV
        csv_file = os.path.join(data_dir, "tickers_realtime.csv")
        df.to_csv(csv_file, index=False)
        print(f"Дані збережено у файл {csv_file}")

        # Виведення перших рядків
        print("Перші рядки даних:")
        print(df.head(10))
    else:
        print(f"Помилка у відповіді: {data['retMsg']}")
        print(f"Повна відповідь: {data}")
except requests.exceptions.RequestException as e:
    print(f"Помилка запиту: {e}")
