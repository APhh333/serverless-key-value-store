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

# Формуємо URL — використовуємо ім’я контейнера (воно збігається з SHARD_NAME)
NODE_URL = f"http://{SHARD_NAME}:8000"

# --- STORAGE + LOG ---
STORAGE: Dict[str, Dict[str, Any]] = {}
EVENT_LOG: List[Dict[str, Any]] = []  # simple in-memory log for replication

class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: dict

@app.on_event("startup")
async def register_with_coordinator():
    """Реєструє shard при запуску (включно з роллю і групою)."""
    payload = {
        "name": SHARD_NAME,
        "url": f"http://{SHARD_NAME}:8000",
        "group": os.getenv("SHARD_GROUP"),
        "role": os.getenv("REPLICA_ROLE")
    }

    print(f"🧭 Registering shard with coordinator: {payload}")  # Debug log

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{COORDINATOR_URL}/register_shard", json=payload, timeout=10.0)
            print(f"✅ Coordinator responded: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"❌ Failed to register shard: {e}")


def _composite(k: str, sk: Optional[str]) -> str:
    """Формує compound key."""
    return f"{k}:{sk}" if sk else k

async def fanout_to_followers(event: dict):
    """Асинхронна реплікація події на всі FOLLOWER_URLS."""
    async with httpx.AsyncClient() as client:
        for url in FOLLOWER_URLS:
            try:
                await client.post(f"{url}/replicate", json=event, timeout=5)
            except Exception as e:
                print(f"[WARN] fan-out to {url} failed: {e}")

# --- CRUD + LOGGING ---
@app.post("/create")
async def create(data: KeyValue):
    """CREATE (лідера та фоловерів, з журналом подій)."""
    key = _composite(data.key, data.sort_key)
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower. Writes must go to leader at {LEADER_URL}")
    STORAGE.setdefault(data.table, {})[key] = data.value
    event = {
        "op": "create", "table": data.table, "key": key,
        "value": data.value, "timestamp": datetime.datetime.utcnow().isoformat()
    }
    EVENT_LOG.append(event)
    if REPLICA_ROLE == "leader":
        asyncio.create_task(fanout_to_followers(event))
    return {"message": "created", "key": key}
    

@app.put("/update")
async def update(data: KeyValue):
    """UPDATE: додає до журналу та розсилає фоловерам."""
    key = _composite(data.key, data.sort_key)
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower. Writes must go to leader at {LEADER_URL}")
    if data.table not in STORAGE or key not in STORAGE[data.table]:
        raise HTTPException(404, "Key not found")
    STORAGE[data.table][key] = data.value
    event = {
        "op": "update", "table": data.table, "key": key,
        "value": data.value, "timestamp": datetime.datetime.utcnow().isoformat()
    }
    EVENT_LOG.append(event)
    if REPLICA_ROLE == "leader":
        asyncio.create_task(fanout_to_followers(event))
    return {"message": "updated", "key": key}

@app.delete("/delete/{table}/{key}")
async def delete(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """DELETE (з фан-аутом події на follower)."""
    comp = _composite(key, sort_key)
    if REPLICA_ROLE != "leader":
         raise HTTPException(403, f"Forbidden: This node is a follower. Writes must go to leader at {LEADER_URL}")
    if table not in STORAGE or comp not in STORAGE[table]:
        raise HTTPException(404, "not found")
    del STORAGE[table][comp]
    event = {"op": "delete", "table": table, "key": comp,
             "timestamp": datetime.datetime.utcnow().isoformat()}
    EVENT_LOG.append(event)
    if REPLICA_ROLE == "leader":
        asyncio.create_task(fanout_to_followers(event))
    return {"message": "deleted", "key": comp}

@app.get("/read/{table}/{key}")
def read(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """READ: локальне читання (для load-balancing)."""
    comp = _composite(key, sort_key)
    if table not in STORAGE or comp not in STORAGE[table]:
        raise HTTPException(404, "not found")
    return {"key": comp, "value": STORAGE[table][comp]}

@app.get("/exists/{table}/{key}")
def exists(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """EXISTS: перевіряє наявність ключа."""
    comp = _composite(key, sort_key)
    return {"exists": comp in STORAGE.get(table, {})}

@app.post("/replicate")
def replicate(event: Dict[str, Any]):
    """Фоловери отримують події від лідера."""
    op = event.get("op")
    table, key, val = event.get("table"), event.get("key"), event.get("value")
    STORAGE.setdefault(table, {})
    if op in ("create", "update"):
        STORAGE[table][key] = val
    elif op == "delete":
        STORAGE[table].pop(key, None)
    EVENT_LOG.append(event)
    return {"replicated": True, "op": op}
