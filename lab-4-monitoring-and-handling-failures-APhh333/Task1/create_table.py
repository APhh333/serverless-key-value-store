import httpx
import asyncio
import time
import sys

COORDINATOR_URL = "http://127.0.0.1:8000"
TABLE_NAME = "usertable"
TEST_KEY = "manual_test_key_123"

async def manual_test_create_and_read():
    """
    Виконує ручний запис (POST /create) та читання (GET /read),
    щоб перевірити, чи Координатор повертає 500 помилку.
    """
    data = {
        "table": TABLE_NAME,
        "key": TEST_KEY,
        "value": {"field0": "initial_value", "timestamp_sent": time.time()}
    }

    # --- 1. СТВОРЕННЯ (Запис) ---
    print(f"--- 1. Attempting to POST /create for key: {TEST_KEY} ---")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            create_resp = await client.post(f"{COORDINATOR_URL}/create", json=data)

        if create_resp.status_code == 200:
            version = create_resp.json().get("version", "N/A")
            print(f"✅ Success (CREATE): Key created. Version: {version}")
        else:
            print(f"❌ Failure (CREATE): Status {create_resp.status_code}. Response body: {create_resp.text}")
            sys.exit(1)
            
    except httpx.RequestError as e:
        print(f"❌ Connection Error (CREATE): Could not reach Coordinator. {e}")
        sys.exit(1)

    # --- 2. ЧИТАННЯ (Quorum Read) ---
    print(f"\n--- 2. Attempting to GET /read for key: {TEST_KEY} (Quorum Read) ---")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            read_resp = await client.get(f"{COORDINATOR_URL}/read/{TABLE_NAME}/{TEST_KEY}")

        if read_resp.status_code == 200:
            data = read_resp.json()
            print(f"✅ Success (READ): Status 200.")
            print(f"   Version: {data.get('version')}")
            print(f"   Data: {data.get('response')}")
        else:
            # Якщо тут 500, то це саме та проблема, яку ми шукаємо
            print(f"❌ Failure (READ): Status {read_resp.status_code}. Response body: {read_resp.text}")
            print("\n!!! КРИТИЧНО: Якщо це 500, негайно перевірте логи контейнера 'coordinator' !!!")
            sys.exit(1)

    except httpx.RequestError as e:
        print(f"❌ Connection Error (READ): Could not reach Coordinator. {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(manual_test_create_and_read())