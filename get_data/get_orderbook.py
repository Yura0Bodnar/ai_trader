import requests
import time
import pandas as pd
import os

# Базовий URL ендпоінта
BASE_URL = "https://api.bybit.com/v5/market/orderbook"

# Конфігурація
symbol = "BTCUSDT"  # Торгова пара
category = "spot"   # Категорія ринку (спотовий)
limit = 200          # Глибина ордербуку (від 1 до 200 для spot)
interval_minutes = 15  # Інтервал між запитами (15 хвилин)
total_hours = 7 * 24  # Загальна кількість годин (1 тиждень)
iterations = int(total_hours * 60 / interval_minutes)  # Кількість ітерацій

# Поточний час
current_time = int(time.time() * 1000)  # Поточний час у мілісекундах

# Змінна для збору результатів
all_orderbooks = []

# Визначення шляху до папки data
current_dir = os.path.dirname(os.path.abspath(__file__))  # Поточна директорія файлу
data_dir = os.path.join(current_dir, '..', 'data')  # Перехід на рівень вище і до папки data
os.makedirs(data_dir, exist_ok=True)  # Створення папки, якщо вона не існує


# Цикл для виконання запитів
for i in range(iterations):
    # Розрахунок часу запиту
    timestamp = current_time - i * int(interval_minutes * 60 * 1000)

    # Параметри запиту
    params = {
        "category": category,
        "symbol": symbol,
        "limit": limit,
    }

    try:
        # Виконання запиту
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Перевірка статусу відповіді
        data = response.json()  # Парсинг JSON

        if data["retCode"] == 0:
            result = data["result"]
            symbol = result["s"]  # Торгова пара
            asks = result["a"]    # Аски (продавці)
            bids = result["b"]    # Біди (покупці)
            timestamp = result["ts"]  # Час у мілісекундах

            # Формування даних
            for ask in asks:
                all_orderbooks.append([symbol, "ask", float(ask[0]), float(ask[1]), timestamp])
            for bid in bids:
                all_orderbooks.append([symbol, "bid", float(bid[0]), float(bid[1]), timestamp])

            print(f"Ітерація {i + 1}: отримано {len(asks) + len(bids)} записів")
        else:
            print(f"Помилка у відповіді: {data['retMsg']}")
            break
    except requests.exceptions.RequestException as e:
        print(f"Помилка запиту: {e}")
        break

# Створення DataFrame
columns = ["symbol", "type", "price", "quantity", "timestamp"]
table = pd.DataFrame(all_orderbooks, columns=columns)

# Конвертація часу з мілісекунд у читаємий формат
table["timestamp"] = pd.to_numeric(table["timestamp"])  # Явне перетворення в int
table["timestamp"] = pd.to_datetime(table["timestamp"], unit="ms")

# Збереження у CSV
csv_file = os.path.join(data_dir, "orderbook_history.csv")
table.to_csv(csv_file, index=False)
print(f"Дані збережено у файл {csv_file}")

# Виведення перших 50 рядків
print("Перші 10 рядків:")
print(table.head(10))
