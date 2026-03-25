import requests
import json
import time
import random

# Налаштування
COORDINATOR_URL = "http://localhost:8000"
SHARD2_FOLLOWER_URL = "http://localhost:9003"
HEADERS = {"Content-Type": "application/json"}

# Генеруємо унікальний ключ
unique_key = f"user_merge_{random.randint(1000, 9999)}"

print(f"\n--- 1. Створюємо початковий кошик (List) для ключа {unique_key} ---")
initial_data = {
    "table": "cart",
    "key": unique_key,
    "value": ["apple"]
}

try:
    resp = requests.post(f"{COORDINATOR_URL}/create", json=initial_data)
    print(f"Create Response: {resp.status_code}")

    time.sleep(1)

    # Перевірка
    resp = requests.get(f"{COORDINATOR_URL}/read/cart/{unique_key}")
    # Ось тут ми просто беремо 'response', який повертає координатор
    current_data = resp.json().get("response")
    print(f"Кошик зараз: {current_data} (Type: {type(current_data)})")

    print("\n--- 2. Симулюємо конфлікт (Merge Strategy) ---")
    conflict_event = {
        "op": "update",
        "table": "cart",
        "key": unique_key,
        "value": ["banana"],
        "timestamp": "2099-01-01T12:00:00.000000",
        "node_id": "mobile_app_offline",
        "vector_clock": {"mobile_app_offline": 1}
    }

    resp = requests.post(f"{SHARD2_FOLLOWER_URL}/replicate", json=conflict_event)
    print(f"Replicate Response: {resp.status_code}")

    time.sleep(2)

    print("\n--- 3. Перевіряємо результат злиття ---")
    resp = requests.get(f"{COORDINATOR_URL}/read/cart/{unique_key}")
    final_data = resp.json().get("response")

    print(f"Фінальний кошик: {final_data}")

    if isinstance(final_data, list) and "apple" in final_data and "banana" in final_data:
        print("\n✅ [SUCCESS] УСПІХ: Списки об'єдналися! (Custom Merge працює)")
    else:
        print(f"\n❌ [FAIL] ФЕЙЛ: Результат: {final_data}")

except Exception as e:
    print(f"Error: {e}")