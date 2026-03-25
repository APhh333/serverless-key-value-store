from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, Any

app = FastAPI(title="Sharding API", version="1.0")

# Зберігаємо таблиці в пам’яті
TABLES: Dict[str, Dict[str, Any]] = {}


# ----- 1a. Реєстрація таблиці -----
class TableDefinition(BaseModel):
    name: str
    primary_key: str
    sort_key: Optional[str] = None  # опціонально


@app.post("/register_table")
def register_table(definition: TableDefinition):
    if definition.name in TABLES:
        raise HTTPException(status_code=400, detail="Table already exists")
    TABLES[definition.name] = {}
    return {"message": f"Table '{definition.name}' registered successfully"}


# ----- 1b. CRUD -----
class Record(BaseModel):
    key: str
    sort_key: Optional[str] = None
    value: dict


@app.post("/tables/{table_name}/create")
def create_record(table_name: str, record: Record):
    table = TABLES.get(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")
    composite_key = f"{record.key}:{record.sort_key}" if record.sort_key else record.key
    if composite_key in table:
        raise HTTPException(status_code=400, detail="Record already exists")
    table[composite_key] = record.value
    return {"message": "Record created", "key": composite_key}


@app.get("/tables/{table_name}/read/{key}")
def read_record(table_name: str, key: str):
    table = TABLES.get(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")
    record = table.get(key)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return {"key": key, "value": record}


@app.delete("/tables/{table_name}/delete/{key}")
def delete_record(table_name: str, key: str):
    table = TABLES.get(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")
    if key not in table:
        raise HTTPException(status_code=404, detail="Record not found")
    del table[key]
    return {"message": f"Record {key} deleted"}


@app.put("/tables/{table_name}/update/{key}")
def update_record(table_name: str, key: str, record: Record):
    table = TABLES.get(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")
    if key not in table:
        raise HTTPException(status_code=404, detail="Record not found")
    table[key] = record.value
    return {"message": "Record updated", "key": key}
