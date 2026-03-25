from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import httpx
import random
from consistent_hash import ConsistentHashRing

app = FastAPI(title="Coordinator with Replica Set Awareness")

# --- Дані ---
# Нова структура для зберігання груп реплік
# "shard1-group" -> {"leader": "url", "followers": ["url1", "url2"]}
SHARD_GROUPS: Dict[str, Dict[str, Any]] = {}

# Кільце тепер буде зберігати назви ГРУП (напр. "shard1-group")
RING = ConsistentHashRing()
TABLES: Dict[str, Dict[str, Any]] = {}  # Схеми таблиць

# --- Моделі ---
class ShardRegistration(BaseModel):
    """Модель, яку шард надсилає при реєстрації (з Lab 3)"""
    name: str
    url: str
    group: str  # Напр. "shard1-group"
    role: str   # "leader" або "follower"

class TableDef(BaseModel):
    name: str
    partition_key_name: str
    sort_key_name: Optional[str] = None

class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: Optional[dict] = None

# --- API Реєстрації ---

@app.post("/register_shard")
def register_shard(data: ShardRegistration):
    """
    Реєстрація нового shard (лідера або фоловера)
    Оновлено для Lab 3.
    """
    print(f"Received registration: {data.dict()}") # Debug log

    # Ініціалізуємо групу, якщо її немає
    SHARD_GROUPS.setdefault(data.group, {"leader": None, "followers": []})

    if data.role == "leader":
        SHARD_GROUPS[data.group]["leader"] = data.url
        # Додаємо ГРУПУ в хеш-кільце (якщо ще не там)
        # Це гарантує, що всі ключі цієї групи потраплять до неї
        
        # Проста перевірка, щоб не додавати вузол двічі
        # (в реальному житті потрібен кращий механізм)
        current_nodes = {RING.ring[k] for k in RING.sorted_keys}
        if data.group not in current_nodes:
             RING.add_node(data.group)
             print(f"Added group {data.group} to hash ring.")

    elif data.role == "follower":
        if data.url not in SHARD_GROUPS[data.group]["followers"]:
            SHARD_GROUPS[data.group]["followers"].append(data.url)
    
    print(f"Current state of SHARD_GROUPS: {SHARD_GROUPS}")
    return {"message": f"Shard {data.name} ({data.role}) registered to {data.group}"}


@app.get("/shards")
def list_shards():
    """Переглянути всі зареєстровані групи реплік"""
    return {"shard_groups": SHARD_GROUPS}

@app.post("/register_table")
def register_table(defn: TableDef):
    if defn.name in TABLES:
        raise HTTPException(status_code=400, detail="Table already exists")
    TABLES[defn.name] = defn.dict()
    return {"message": "Table registered", "table": defn.name}

# --- Логіка Маршрутизації (Оновлено) ---

def get_shard_group_for_key(key: str) -> Dict[str, Any]:
    """Знаходить групу реплік для ключа."""
    group_name = RING.get_node(key)
    if not group_name or group_name not in SHARD_GROUPS:
        print(f"Error: No group found for key '{key}'. Ring returned '{group_name}'. Ring state: {RING.ring}")
        raise HTTPException(status_code=500, detail="No shard group available for key")
    return SHARD_GROUPS[group_name]

def get_write_node(key: str) -> str:
    """Для ЗАПИСУ (Write) - завжди повертає ЛІДЕРА групи."""
    group = get_shard_group_for_key(key)
    if not group.get("leader"):
        raise HTTPException(status_code=503, detail="No leader available for shard group")
    return group["leader"]

def get_read_node(key: str) -> str:
    """Для ЧИТАННЯ (Read) - повертає БУДЬ-ЯКУ репліку (Лідер + Фоловери)."""
    group = get_shard_group_for_key(key)
    
    all_replicas = []
    if group.get("leader"):
        all_replicas.append(group["leader"])
    all_replicas.extend(group.get("followers", []))

    if not all_replicas:
        raise HTTPException(status_code=503, detail="No replicas available for shard group")
    
    # Балансування навантаження: обираємо випадкову репліку
    return random.choice(all_replicas)

def _composite(key: str, sort_key: Optional[str]) -> str:
    """Формує compound key."""
    return f"{key}:{sort_key}" if sort_key else key


# --- CRUD маршрутизація (Оновлено) ---

@app.post("/create")
async def create_record(data: KeyValue):
    """CREATE: Завжди йде на ЛІДЕРА."""
    composite_key = _composite(data.key, data.sort_key)
    # Використовуємо get_write_node
    shard_url = get_write_node(composite_key)
    
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{shard_url}/create", json=data.dict())
    
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json()}


@app.get("/read/{table}/{key}")
async def read_record(table: str, key: str, sort_key: Optional[str] = None):
    """READ: Йде на ВИПАДКОВУ репліку (балансування)."""
    composite_key = _composite(key, sort_key)
    # Використовуємо get_read_node
    shard_url = get_read_node(composite_key)
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{shard_url}/read/{table}/{key}", params={"sort_key": sort_key})
    
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json()}


@app.get("/exists/{table}/{key}")
async def exists_record(table: str, key: str, sort_key: Optional[str] = None):
    """EXISTS: Йде на ВИПАДКОВУ репліку (балансування)."""
    composite_key = _composite(key, sort_key)
    # Використовуємо get_read_node
    shard_url = get_read_node(composite_key)

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{shard_url}/exists/{table}/{key}", params={"sort_key": sort_key})
    
    return {"target_shard_url": shard_url, "response": resp.json()}

@app.put("/update")
async def update_record(data: KeyValue):
    """UPDATE: Завжди йде на ЛІДЕРА."""
    composite_key = _composite(data.key, data.sort_key)
    # Використовуємо get_write_node
    shard_url = get_write_node(composite_key)
    
    async with httpx.AsyncClient() as client:
        resp = await client.put(f"{shard_url}/update", json=data.dict())
    
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json()}

@app.delete("/delete/{table}/{key}")
async def delete_record(table: str, key: str, sort_key: Optional[str] = None):
    """DELETE: Завжди йде на ЛІДЕРА."""
    composite_key = _composite(key, sort_key)
    # Використовуємо get_write_node
    shard_url = get_write_node(composite_key)
    
    async with httpx.AsyncClient() as client:
        resp = await client.delete(f"{shard_url}/delete/{table}/{key}", params={"sort_key": sort_key})
    
    if resp.status_code not in (200, 204): # 204 typically means No Content (success)
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    
    return {"target_shard_url": shard_url, "response": resp.json() if resp.status_code == 200 else "deleted"}