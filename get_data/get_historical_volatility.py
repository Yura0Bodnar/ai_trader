import requests
import pandas as pd
import os

# Базовий URL ендпоінта
BASE_URL = "https://api.bybit.com/v5/market/historical-volatility"

# Конфігурація
base_coin = "BTC"  # Базова криптовалюта
period = 30  # Волатильність за місяць
category = "option"  # Категорія (для опціонів)
iterations = 12  # дані за останній рік
all_volatility_data = []  # Змінна для збереження результатів

# Початковий запит без startTime і endTime
params = {
    "category": category,
    "baseCoin": base_coin,
    "period": period,
}

# Визначення шляху до папки data
current_dir = os.path.dirname(os.path.abspath(__file__))  # Поточна директорія файлу
data_dir = os.path.join(current_dir, '..', 'data')  # Перехід на рівень вище і до папки data
os.makedirs(data_dir, exist_ok=True)  # Створення папки, якщо вона не існує


try:
    # Перший запит
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()  # Перевірка статусу відповіді
    data = response.json()  # Парсинг JSON

    if data["retCode"] == 0 and data["result"]:
        results = data["result"]
        for item in results:
            period_value = item["period"]
            value = float(item["value"])
            timestamp = int(item["time"])
            all_volatility_data.append([base_coin, period_value, value, timestamp])

        # Ініціалізація `endTime` для наступної ітерації
        last_time = int(data["result"][-1]["time"])
        print(f"Перший запит успішний. Отримано час: {last_time}")
    else:
        print(f"Помилка у відповіді: {data['retMsg']}")
        print(f"Повна відповідь: {data}")
except requests.exceptions.RequestException as e:
    print(f"Помилка запиту: {e}")

# Цикл для наступних запитів із `startTime` і `endTime`
for i in range(iterations):
    # Розрахунок нового startTime і endTime
    end_time = last_time
    start_time = end_time - (30 * 24 * 60 * 60 * 1000)  # Відняти 30 днів у мілісекундах

    # Параметри запиту
    params = {
        "category": category,
        "baseCoin": base_coin,
        "period": period,
        "startTime": start_time,
        "endTime": end_time,
    }

    print(f"\nЗапит #{i + 2}: Параметри -> {params}")

    # Визначення шляху до папки data
    current_dir = os.path.dirname(os.path.abspath(__file__))  # Поточна директорія файлу
    data_dir = os.path.join(current_dir, '..', 'data')  # Перехід на рівень вище і до папки data
    os.makedirs(data_dir, exist_ok=True)  # Створення папки, якщо вона не існує

    try:
        # Виконання запиту
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Перевірка статусу відповіді
        data = response.json()  # Парсинг JSON

        if data["retCode"] == 0 and data["result"]:
            results = data["result"]
            for item in results:
                period_value = item["period"]
                value = float(item["value"])
                timestamp = int(item["time"])
                all_volatility_data.append([base_coin, period_value, value, timestamp])

            # Оновлення `endTime` для наступної ітерації
            last_time = int(data["result"][-1]["time"])
            print(f"Дані за інтервал #{i + 2} отримано успішно. Новий час: {last_time}")
        else:
            print(f"Помилка у відповіді: {data['retMsg']}")
            print(f"Повна відповідь: {data}")
            break
    except requests.exceptions.RequestException as e:
        print(f"Помилка запиту: {e}")
        break

# Створення DataFrame
if all_volatility_data:
    columns = ["base_coin", "period", "volatility", "timestamp"]
    volatility_table = pd.DataFrame(all_volatility_data, columns=columns)

    # Конвертація часу з мілісекунд у читаємий формат
    volatility_table["timestamp"] = pd.to_datetime(volatility_table["timestamp"], unit="ms")

    # Збереження у CSV
    csv_file = os.path.join(data_dir, "historical_volatility.csv")
    volatility_table.to_csv(csv_file, index=False)
    print(f"Дані збережено у файл {csv_file}")

    # Виведення перших 10 рядків
    print("Перші 10 рядків:")
    print(volatility_table.head(10))
else:
    print("Дані недоступні для вказаних параметрів.")
