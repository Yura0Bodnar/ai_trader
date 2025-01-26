import requests
import time
import pandas as pd

# Базовий URL ендпоінта
BASE_URL = "https://api.bybit.com/v5/market/funding/history"

# Конфігурація
symbol = "BTCUSDT"  # Торгова пара
category = "linear"  # Тип ринку (лінійний контракт)
interval_hours = 8   # Інтервал у годинах між запитами (враховуємо частоту ставок фінансування)
total_hours = 7 * 24  # Загальна кількість годин (1 тиждень)
iterations = int(total_hours / interval_hours)  # Кількість ітерацій

# Поточний час
current_time = int(time.time() * 1000)  # Поточний час у мілісекундах

# Змінна для збору результатів
all_funding_rates = []

# Цикл для виконання запитів
for i in range(iterations):
    # Розрахунок start і end
    end_time = current_time - i * int(interval_hours * 3600 * 1000)
    start_time = end_time - int(interval_hours * 3600 * 1000)

    # Параметри запиту
    params = {
        "category": category,
        "symbol": symbol,
        "start": start_time,
        "end": end_time,
        "limit": 200,  # Максимальна кількість записів
    }

    try:
        # Виконання запиту
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Перевірка статусу відповіді
        data = response.json()  # Парсинг JSON

        if data["retCode"] == 0:
            funding_data = data["result"]["list"]
            for record in funding_data:
                funding_rate = float(record["fundingRate"])
                timestamp = int(record["fundingRateTimestamp"])
                all_funding_rates.append([symbol, funding_rate, timestamp])

            print(f"Ітерація {i + 1}: отримано {len(funding_data)} записів")
        else:
            print(f"Помилка у відповіді: {data['retMsg']}")
            break
    except requests.exceptions.RequestException as e:
        print(f"Помилка запиту: {e}")
        break

# Створення DataFrame
columns = ["symbol", "funding_rate", "timestamp"]
table = pd.DataFrame(all_funding_rates, columns=columns)

# Конвертація часу з мілісекунд у читаємий формат
table["timestamp"] = pd.to_numeric(table["timestamp"])  # Явне перетворення в int
table["timestamp"] = pd.to_datetime(table["timestamp"], unit="ms")

# Збереження у CSV
csv_file = "funding_rate_history.csv"
table.to_csv(csv_file, index=False)
print(f"Дані збережено у файл {csv_file}")

# Виведення перших 10 рядків
print("Перші 10 рядків:")
print(table.head(10))
