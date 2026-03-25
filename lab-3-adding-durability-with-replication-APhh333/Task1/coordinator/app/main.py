#TEST Deploy
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import httpx
from consistent_hash import ConsistentHashRing

app = FastAPI(title="Coordinator with Sharding and Compound Keys")

# --- Дані ---
SHARDS: Dict[str, str] = {}  # name -> URL
RING = ConsistentHashRing()
TABLES: Dict[str, Dict[str, Any]] = {}  # таблиці

# --- Моделі ---
class ShardRegistration(BaseModel):
    name: str
    url: str

class TableDef(BaseModel):
    name: str
    partition_key_name: str
    sort_key_name: Optional[str] = None

class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: Optional[dict] = None


# --- API ---
@app.post("/register_table")
def register_table(defn: TableDef):
    """Реєстрація схеми таблиці"""
    if defn.name in TABLES:
        raise HTTPException(status_code=400, detail="Table already exists")
    TABLES[defn.name] = defn.dict()
    return {"message": "Table registered", "table": defn.name}


@app.get("/tables")
def list_tables():
    """Переглянути всі таблиці"""
    return {"tables": list(TABLES.keys())}


@app.post("/register_shard")
def register_shard(data: ShardRegistration):
    """Реєстрація нового shard"""
    if data.name in SHARDS:
        raise HTTPException(status_code=400, detail="Shard already exists")
    SHARDS[data.name] = data.url
    RING.add_node(data.name)
    return {"message": f"Shard {data.name} registered", "url": data.url}


@app.get("/shards")
def list_shards():
    """Переглянути всі зареєстровані shards"""
    return {"shards": SHARDS}


def get_shard_for_key(key: str):
    """Знаходимо shard для конкретного ключа"""
    shard_name = RING.get_node(key)
    if shard_name is None:
        raise HTTPException(status_code=500, detail="No shards available")
    return shard_name, SHARDS[shard_name]


# --- CRUD маршрутизація ---
@app.post("/create")
async def create_record(data: KeyValue):
    """CREATE з маршрутизацією по shard (підтримує compound key)"""
    composite_key = f"{data.key}:{data.sort_key}" if data.sort_key else data.key
    shard_name, shard_url = get_shard_for_key(composite_key)
    payload = data.dict()
    payload["composite_key"] = composite_key
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{shard_url}/create", json=payload)
    return {"target_shard": shard_name, "response": resp.json()}


@app.get("/read/{table}/{key}")
async def read_record(table: str, key: str, sort_key: Optional[str] = None):
    """READ — читає з shard"""
    composite_key = f"{key}:{sort_key}" if sort_key else key
    shard_name, shard_url = get_shard_for_key(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{shard_url}/read/{table}/{key}", params={"sort_key": sort_key})
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard": shard_name, "response": resp.json()}


@app.get("/exists/{table}/{key}")
async def exists_record(table: str, key: str, sort_key: Optional[str] = None):
    """EXISTS — перевірка наявності ключа"""
    composite_key = f"{key}:{sort_key}" if sort_key else key
    shard_name, shard_url = get_shard_for_key(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{shard_url}/exists/{table}/{key}", params={"sort_key": sort_key})
    return {"target_shard": shard_name, "response": resp.json()}

@app.put("/update")
async def update_record(data: KeyValue):
    """UPDATE — оновлює запис у відповідному shard"""
    # Використовуємо той самий ключ для маршрутизації
    composite_key = f"{data.key}:{data.sort_key}" if data.sort_key else data.key
    shard_name, shard_url = get_shard_for_key(composite_key)
    
    # Передаємо повні дані, включаючи composite_key
    payload = data.dict()
    payload["composite_key"] = composite_key # Додаємо ключ для зручності шарда
    
    async with httpx.AsyncClient() as client:
        resp = await client.put(f"{shard_url}/update", json=payload)
    
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard": shard_name, "response": resp.json()}

@app.delete("/delete/{table}/{key}")
async def delete_record(table: str, key: str, sort_key: Optional[str] = None):
    """DELETE — видаляє ключ у відповідному shard"""
    composite_key = f"{key}:{sort_key}" if sort_key else key
    shard_name, shard_url = get_shard_for_key(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.delete(f"{shard_url}/delete/{table}/{key}", params={"sort_key": sort_key})
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard": shard_name, "response": resp.json()}