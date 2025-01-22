import requests
import time
import pandas as pd

# Базовий URL ендпоінта
BASE_URL = "https://api.bybit.com/v5/market/kline"

# Конфігурація
symbol = "BTCUSDT"  # Торгова пара
interval = 15  # Інтервал у хвилинах
limit = 200  # Максимальна кількість свічок
total_hours = 7 * 24  # Загальна кількість годин (1 тиждень)
hours_per_request = limit * interval / 60  # Кількість годин за один запит
iterations = int(total_hours / hours_per_request)  # Кількість ітерацій

# Поточний час
current_time = int(time.time() * 1000)  # Поточний час у мілісекундах

# Змінна для збору результатів
all_candles = []

# Цикл для виконання запитів
for i in range(iterations):
    # Розрахунок start і end
    end_time = current_time - i * int(hours_per_request * 3600 * 1000)
    start_time = end_time - int(hours_per_request * 3600 * 1000)

    # Параметри запиту
    params = {
        "category": "spot",
        "symbol": symbol,
        "interval": str(interval),
        "start": start_time,
        "end": end_time,
        "limit": limit,
    }

    try:
        # Виконання запиту
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Перевірка статусу відповіді
        data = response.json()  # Парсинг JSON

        if data["retCode"] == 0:
            candles = data["result"]["list"]
            all_candles.extend(candles)
            print(f"Ітерація {i + 1}: отримано {len(candles)} свічок")
        else:
            print(f"Помилка у відповіді: {data['retMsg']}")
            break
    except requests.exceptions.RequestException as e:
        print(f"Помилка запиту: {e}")
        break

# Перетворення даних у DataFrame
columns = ["startTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume", "turnover"]
table = pd.DataFrame(all_candles, columns=columns)

# Конвертація часу з мілісекунд у читаємий формат
table["startTime"] = pd.to_numeric(table["startTime"])  # Явне перетворення в int
table["startTime"] = pd.to_datetime(table["startTime"], unit="ms")


# Збереження у CSV
csv_file = "candles.csv"
table.to_csv(csv_file, index=False)
print(f"Дані збережено у файл {csv_file}")

# Виведення перших 50 рядків
print("Перші 50 рядків:")
print(table.head(10))
