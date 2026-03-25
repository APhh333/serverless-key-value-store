from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import os, httpx, asyncio, datetime, socket

app = FastAPI(title="Shard Node with Replication Log")

# --- Конфіг ---
COORDINATOR_URL = os.getenv("COORDINATOR_URL")
SHARD_NAME = os.getenv("SHARD_NAME")
REPLICA_ROLE = os.getenv("REPLICA_ROLE", "leader")
SHARD_GROUP = os.getenv("SHARD_GROUP")
FOLLOWER_URLS = os.getenv("FOLLOWER_URLS", "").split(",") if os.getenv("FOLLOWER_URLS") else []
LEADER_URL = os.getenv("LEADER_URL")

NODE_URL = f"http://{SHARD_NAME}:8000"

# --- STORAGE + LOG ---
STORAGE: Dict[str, Dict[str, Any]] = {}
EVENT_LOG: List[Dict[str, Any]] = []

class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    # 'value' - це дані, які надсилає користувач (напр. {"name": "Alice"})
    value: dict 

# --- ВНУТРІШНЯ ЛОГІКА РЕПЛІКАЦІЇ ---

def apply_event(event: Dict[str, Any]):
    """
    Уніфікована функція для застосування події до STORAGE.
    (ОНОВЛЕНО для версіонування)
    """
    op = event.get("op")
    table, key, val = event.get("table"), event.get("key"), event.get("value")
    
    # Використовуємо timestamp події як нашу "версію"
    version = event.get("timestamp", datetime.datetime.utcnow().isoformat())
    
    STORAGE.setdefault(table, {})
    
    if op in ("create", "update"):
        # ЗБЕРІГАЄМО ДАНІ РАЗОМ З ВЕРСІЄЮ
        STORAGE[table][key] = {"value": val, "version": version}
        print(f"[{SHARD_NAME}] Applied event (op: {op}, key: {key}, version: {version}). Total log size: {len(EVENT_LOG)}")
    elif op == "delete":
        STORAGE[table].pop(key, None)
        print(f"[{SHARD_NAME}] Applied event (op: {op}, key: {key}). Total log size: {len(EVENT_LOG)}")
    
    # Додаємо подію до логу ТІЛЬКИ якщо ми її не маємо
    # (проста дедуплікація)
    if event not in EVENT_LOG:
         EVENT_LOG.append(event)


async def sync_with_leader():
    """
    (Тільки для фоловерів)
    Звертається до лідера, щоб отримати пропущені події.
    """
    if REPLICA_ROLE != "follower" or not LEADER_URL:
        return

    current_offset = len(EVENT_LOG)
    print(f"[{SHARD_NAME}] Starting sync with leader {LEADER_URL}. Current offset: {current_offset}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{LEADER_URL}/sync", params={"offset": current_offset}, timeout=30.0)
            
            if resp.status_code != 200:
                print(f"[ERROR] Failed to sync with leader. Status: {resp.status_code}")
                return

            missing_events = resp.json().get("events", [])
            if not missing_events:
                print(f"[{SHARD_NAME}] Already up to date.")
                return

            print(f"[{SHARD_NAME}] Received {len(missing_events)} missing events from leader.")
            for event in missing_events:
                apply_event(event)
            
            print(f"[{SHARD_NAME}] Sync complete. New log size: {len(EVENT_LOG)}")

    except Exception as e:
        print(f"[ERROR] Sync with leader failed: {e}")


# --- ЗАПУСК ТА РЕЄСТРАЦІЯ ---
@app.on_event("startup")
async def register_and_sync():
    """
    1. Реєструє shard при запуску (включно з роллю і групою).
    2. (Для фоловерів) Запускає синхронізацію з лідером.
    """
    payload = {
        "name": SHARD_NAME,
        "url": f"http://{SHARD_NAME}:8000", 
        "group": SHARD_GROUP,
        "role": REPLICA_ROLE
    }

    print(f"🧭 Registering shard with coordinator: {payload}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{COORDINATOR_URL}/register_shard", json=payload, timeout=10.0)
            print(f"✅ Coordinator responded: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"❌ Failed to register shard: {e}")
    
    await sync_with_leader()


def _composite(k: str, sk: Optional[str]) -> str:
    return f"{k}:{sk}" if sk else k

async def fanout_to_followers(event: dict):
    async with httpx.AsyncClient() as client:
        valid_urls = [url for url in FOLLOWER_URLS if url]
        print(f"[Leader] Fanning out event to {valid_urls}")

        for url in valid_urls:
            try:
                await client.post(f"{url}/replicate", json=event, timeout=5)
            except Exception as e:
                print(f"[WARN] fan-out to {url} failed: {e}")

# --- CRUD (ЗАПИС) ---
# (Функції запису оновлено, щоб створювати timestamp)
@app.post("/create")
async def create(data: KeyValue):
    key = _composite(data.key, data.sort_key)
    
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower.")

    event = {
        "op": "create", "table": data.table, "key": key,
        "value": data.value, 
        "timestamp": datetime.datetime.utcnow().isoformat() # ВАЖЛИВО: Генеруємо timestamp тут
    }
    apply_event(event) 
    
    asyncio.create_task(fanout_to_followers(event))
    return {"message": "created", "key": key, "version": event["timestamp"]}

@app.put("/update")
async def update(data: KeyValue):
    key = _composite(data.key, data.sort_key)

    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower.")

    if data.table not in STORAGE or key not in STORAGE[data.table]:
        raise HTTPException(404, "Key not found")
    
    event = {
        "op": "update", "table": data.table, "key": key,
        "value": data.value, 
        "timestamp": datetime.datetime.utcnow().isoformat() # ВАЖЛИВО: Генеруємо timestamp тут
    }
    apply_event(event)

    asyncio.create_task(fanout_to_followers(event))
    return {"message": "updated", "key": key, "version": event["timestamp"]}

@app.delete("/delete/{table}/{key}")
async def delete(table: str, key: str, sort_key: Optional[str] = Query(None)):
    comp = _composite(key, sort_key)
    
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower.")

    if table not in STORAGE or comp not in STORAGE[table]:
        raise HTTPException(404, "not found")
    
    event = {"op": "delete", "table": table, "key": comp,
             "timestamp": datetime.datetime.utcnow().isoformat()}
    apply_event(event)
    
    asyncio.create_task(fanout_to_followers(event))
    return {"message": "deleted", "key": comp}

# --- CRUD (ЧИТАННЯ) ---
@app.get("/read/{table}/{key}")
def read(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """
    (ОНОВЛЕНО) Повертає повний об'єкт {value, version}
    """
    comp = _composite(key, sort_key)
    if table not in STORAGE or comp not in STORAGE[table]:
        raise HTTPException(404, "not found")
    
    print(f"[{SHARD_NAME}] Serving read for key {comp}")
    
    # Повертаємо весь об'єкт, що зберігається (включно з 'value' та 'version')
    return {"key": comp, "data": STORAGE[table][comp], "served_by": SHARD_NAME}

@app.get("/exists/{table}/{key}")
def exists(table: str, key: str, sort_key: Optional[str] = Query(None)):
    comp = _composite(key, sort_key)
    print(f"[{SHARD_NAME}] Serving exists for key {comp}")
    return {"exists": comp in STORAGE.get(table, {}), "served_by": SHARD_NAME}


# --- ЕНДПОІНТИ РЕПЛІКАЦІЇ ---

@app.post("/replicate")
def replicate(event: Dict[str, Any]):
    if REPLICA_ROLE == "leader":
        print("[ERROR] Leader node received a replication event.")
        raise HTTPException(500, "Leader cannot replicate from itself")

    # Перевірка на дублікати
    event_timestamp = event.get("timestamp")
    for e in EVENT_LOG:
        if e.get("timestamp") == event_timestamp and e.get("key") == event.get("key"):
            print(f"[{SHARD_NAME}] Ignoring duplicate event: {event.get('key')}")
            return {"replicated": False, "message": "Duplicate event"}

    apply_event(event)
    return {"replicated": True, "op": event.get("op"), "node": SHARD_NAME}


@app.get("/sync")
def sync(offset: int = 0):
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower.")

    if offset < 0:
        offset = 0
        
    missing_events = EVENT_LOG[offset:]
    
    print(f"[Leader] Follower requested sync from offset {offset}. Sending {len(missing_events)} events.")
    
    return {"events": missing_events, "current_leader_offset": len(EVENT_LOG)}