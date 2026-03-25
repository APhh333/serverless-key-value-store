from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, Optional
import os
import httpx

app = FastAPI(title="Shard Node with Compound Key Support")

# --- Локальне сховище ---
STORAGE: Dict[str, Dict[str, Any]] = {}

# --- Змінні середовища ---
COORDINATOR_URL = os.getenv("COORDINATOR_URL")
SHARD_NAME = os.getenv("SHARD_NAME")


class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: dict


@app.on_event("startup")
async def register_with_coordinator():
    """Реєструє shard при запуску"""
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{COORDINATOR_URL}/register_shard",
            json={"name": SHARD_NAME, "url": f"http://{SHARD_NAME}:8000"},
        )


@app.post("/create")
def create(data: KeyValue):
    """CREATE запис у локальному shard"""
    if data.table not in STORAGE:
        STORAGE[data.table] = {}
    composite_key = f"{data.key}:{data.sort_key}" if data.sort_key else data.key
    STORAGE[data.table][composite_key] = data.value
    return {"message": "Created", "table": data.table, "key": composite_key}


@app.get("/read/{table}/{key}")
def read(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """READ — читання запису"""
    if table not in STORAGE:
        raise HTTPException(status_code=404, detail="Table not found")
    composite_key = f"{key}:{sort_key}" if sort_key else key
    if composite_key not in STORAGE[table]:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": composite_key, "value": STORAGE[table][composite_key]}


@app.get("/exists/{table}/{key}")
def exists(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """EXISTS — перевіряє, чи є запис"""
    if table not in STORAGE:
        return {"exists": False}
    composite_key = f"{key}:{sort_key}" if sort_key else key
    return {"exists": composite_key in STORAGE[table]}

@app.put("/update")
def update(data: KeyValue):
    """UPDATE — оновлює запис у локальному сховищі"""
    if data.table not in STORAGE:
        raise HTTPException(status_code=404, detail="Table not found")
        
    # Використовуємо ключ, який передав координатор
    # (або обчислюємо його тут, якщо не передавати payload["composite_key"])
    composite_key = f"{data.key}:{data.sort_key}" if data.sort_key else data.key

    if composite_key not in STORAGE[data.table]:
        raise HTTPException(status_code=404, detail="Key not found for update")

    # Оновлюємо значення
    STORAGE[data.table][composite_key] = data.value
    return {"message": "Updated", "table": data.table, "key": composite_key, "new_value": data.value}

@app.delete("/delete/{table}/{key}")
def delete(table: str, key: str, sort_key: Optional[str] = Query(None)):
    """DELETE — видаляє запис"""
    if table not in STORAGE:
        raise HTTPException(status_code=404, detail="Table not found")
    composite_key = f"{key}:{sort_key}" if sort_key else key
    if composite_key not in STORAGE[table]:
        raise HTTPException(status_code=404, detail="Key not found")
    del STORAGE[table][composite_key]
    return {"message": "Deleted", "key": composite_key}