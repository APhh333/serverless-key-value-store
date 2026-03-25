from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import httpx
# 'random' нам більше не потрібен для читання
import asyncio # Потрібен для паралельних запитів
from consistent_hash import ConsistentHashRing

app = FastAPI(title="Coordinator with Quorum Reads (R+W > N)")

# --- Дані ---
SHARD_GROUPS: Dict[str, Dict[str, Any]] = {}
RING = ConsistentHashRing()
TABLES: Dict[str, Dict[str, Any]] = {}

# --- Моделі ---
class ShardRegistration(BaseModel):
    name: str
    url: str
    group: str
    role: str

class TableDef(BaseModel):
    name: str
    partition_key_name: str
    sort_key_name: Optional[str] = None

class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: Optional[dict] = None

# --- API Реєстрації (без змін) ---
@app.post("/register_shard")
def register_shard(data: ShardRegistration):
    print(f"Received registration: {data.dict()}")
    SHARD_GROUPS.setdefault(data.group, {"leader": None, "followers": []})

    if data.role == "leader":
        SHARD_GROUPS[data.group]["leader"] = data.url
        current_nodes = {RING.ring[k] for k in RING.sorted_keys if k in RING.ring}
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
    return {"shard_groups": SHARD_GROUPS}

@app.post("/register_table")
def register_table(defn: TableDef):
    if defn.name in TABLES:
        raise HTTPException(status_code=400, detail="Table already exists")
    TABLES[defn.name] = defn.dict()
    return {"message": "Table registered", "table": defn.name}

# --- Логіка Маршрутизації (Оновлено) ---

def get_shard_group_for_key(key: str) -> Dict[str, Any]:
    group_name = RING.get_node(key)
    if not group_name or group_name not in SHARD_GROUPS:
        print(f"Error: No group found for key '{key}'. Ring returned '{group_name}'.")
        raise HTTPException(status_code=500, detail="No shard group available for key")
    return SHARD_GROUPS[group_name]

def get_write_node(key: str) -> str:
    """Для ЗАПИСУ (W=1) - завжди повертає ЛІДЕРА групи."""
    group = get_shard_group_for_key(key)
    if not group.get("leader"):
        raise HTTPException(status_code=503, detail="No leader available for shard group")
    return group["leader"]

def get_all_read_nodes(key: str) -> List[str]:
    """(НОВА ФУНКЦІЯ) Для ЧИТАННЯ (R=N=3) - повертає ВСІ репліки."""
    group = get_shard_group_for_key(key)
    
    all_replicas = []
    if group.get("leader"):
        all_replicas.append(group["leader"])
    all_replicas.extend(group.get("followers", []))

    if not all_replicas:
        raise HTTPException(status_code=503, detail="No replicas available for shard group")
    
    return all_replicas

def _composite(key: str, sort_key: Optional[str]) -> str:
    return f"{key}:{sort_key}" if sort_key else key


# --- CRUD маршрутизація (Оновлено) ---

# (ЗАПИС - без змін, вони все ще йдуть тільки до лідера)
@app.post("/create")
async def create_record(data: KeyValue):
    composite_key = _composite(data.key, data.sort_key)
    shard_url = get_write_node(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{shard_url}/create", json=data.dict())
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json()}

@app.put("/update")
async def update_record(data: KeyValue):
    composite_key = _composite(data.key, data.sort_key)
    shard_url = get_write_node(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.put(f"{shard_url}/update", json=data.dict())
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json()}

@app.delete("/delete/{table}/{key}")
async def delete_record(table: str, key: str, sort_key: Optional[str] = None):
    composite_key = _composite(key, sort_key)
    shard_url = get_write_node(composite_key)
    async with httpx.AsyncClient() as client:
        resp = await client.delete(f"{shard_url}/delete/{table}/{key}", params={"sort_key": sort_key})
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"target_shard_url": shard_url, "response": resp.json() if resp.status_code == 200 else "deleted"}


# --- (ЧИТАННЯ - Повністю переписано для Quorum) ---

@app.get("/read/{table}/{key}")
async def read_record(table: str, key: str, sort_key: Optional[str] = None):
    """
    READ: (ОНОВЛЕНО) Читає з R реплік (Quorum Read) і повертає найновішу версію.
    """
    composite_key = _composite(key, sort_key)
    
    # 1. Отримуємо ВСІ репліки (R=3)
    replica_urls = get_all_read_nodes(composite_key)
    R = len(replica_urls)

    print(f"[Quorum Read] Querying R={R} nodes for key '{composite_key}': {replica_urls}")

    # 2. Паралельно опитуємо всі репліки
    async with httpx.AsyncClient() as client:
        tasks = []
        for url in replica_urls:
            read_url = f"{url}/read/{table}/{key}"
            params = {"sort_key": sort_key} if sort_key else {}
            tasks.append(client.get(read_url, params=params, timeout=5.0))
        
        # return_exceptions=True, щоб одна помилка не зупинила всі
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # 3. Обробляємо відповіді та знаходимо найновішу
    successful_responses = []
    failed_nodes = 0
    
    for resp in responses:
        if isinstance(resp, httpx.Response) and resp.status_code == 200:
            successful_responses.append(resp.json())
        elif isinstance(resp, Exception):
            failed_nodes += 1
            print(f"[Quorum Read] Node failed: {resp}")
        # Ігноруємо 404, це означає, що вузол не має ключа (або відстав)
        elif isinstance(resp, httpx.Response) and resp.status_code == 404:
            pass 
            
    if not successful_responses:
        raise HTTPException(404, f"Key not found on any replica. {failed_nodes}/{R} nodes failed.")

    # 4. Знаходимо найновішу версію (найбільший timestamp)
    # Формат відповіді шарда: {"key": ..., "data": {"value": ..., "version": ...}, "served_by": ...}
    newest_response = None
    newest_version = "" # ISO-8601 timestamps можна порівнювати як рядки

    for resp_data in successful_responses:
        # data.get("data", {}) щоб уникнути помилки, якщо 'data' відсутня
        version = resp_data.get("data", {}).get("version", "")
        
        if version > newest_version:
            newest_version = version
            newest_response = resp_data
            
    if newest_response is None:
         raise HTTPException(404, "Key not found (all nodes returned 404 or errors)")

    # 5. Повертаємо відповідь від вузла, що мав найновішу версію
    return {
        "message": "Quorum read successful",
        "R_queried": R,
        "successful_nodes": len(successful_responses),
        "newest_data_from": newest_response.get("served_by"),
        "version": newest_version,
        "response": newest_response.get("data", {}).get("value") # Повертаємо чисті дані
    }


@app.get("/exists/{table}/{key}")
async def exists_record(table: str, key: str, sort_key: Optional[str] = None):
    """
    EXISTS: (ОНОВЛЕНО) Використовує Quorum Read.
    Повертає 'exists: true' якщо *будь-яка* репліка повернула 200 OK.
    """
    composite_key = _composite(key, sort_key)
    
    replica_urls = get_all_read_nodes(composite_key)
    
    async with httpx.AsyncClient() as client:
        tasks = [client.get(f"{url}/exists/{table}/{key}", params={"sort_key": sort_key} if sort_key else {}, timeout=5.0) for url in replica_urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Нам потрібен лише ОДИН вузол, який каже "exists: true"
    for resp in responses:
        if isinstance(resp, httpx.Response) and resp.status_code == 200:
            if resp.json().get("exists") == True:
                return {"exists": True, "message": "Quorum 'exists' successful"}
    
    # Якщо ніхто не сказав "true"
    return {"exists": False}