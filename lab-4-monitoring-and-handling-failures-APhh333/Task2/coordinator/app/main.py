from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import httpx
import asyncio
from consistent_hash import ConsistentHashRing
import os
from ddtrace import patch_all, tracer
from ddtrace.vendor.dogstatsd import DogStatsd
import datetime
import logging
import json
import time

patch_all()

# Ініціалізація DogStatsD для кастомних метрик
statlogger = DogStatsd(host=os.getenv("DD_AGENT_HOST", "datadog"), port=8125)

# Настройка структурированного логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def structured_log(operation: str, key: str = None, status: str = "success",
                   error: str = None, duration_ms: float = None, **kwargs):
    """Структурированное логирование с trace IDs"""
    span = tracer.current_span()
    log_data = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "level": "ERROR" if error else "INFO",
        "service": "coordinator",
        "operation": operation,
        "trace_id": span.trace_id if span else None,
        "key": key,
        "status": status,
        "duration_ms": duration_ms,
        **kwargs
    }

    if error:
        log_data["error"] = error
        logger.error(json.dumps(log_data))
    else:
        logger.info(json.dumps(log_data))


app = FastAPI(title="Coordinator with Quorum Reads (R+W > N) and Monitoring")

# --- Дані ---
SHARD_GROUPS: Dict[str, Dict[str, Any]] = {}
RING = ConsistentHashRing()
TABLES: Dict[str, Dict[str, Any]] = {}

# Глобальные счетчики для метрик
REQUEST_COUNTERS = {
    "write_success": 0,
    "write_error": 0,
    "read_success": 0,
    "read_error": 0,
    "exists_success": 0,
    "exists_error": 0
}


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
    value: Any  # <--- ОСЬ ТУТ ВАЖЛИВА ЗМІНА (dict -> Any)


# --- API Реєстрації ---
@app.post("/register_shard")
@tracer.wrap(service='coordinator', resource='register_shard')
def register_shard(data: ShardRegistration):
    start_time = time.time()
    try:
        print(f"Received registration: {data.dict()}")
        SHARD_GROUPS.setdefault(data.group, {"leader": None, "followers": []})

        if data.role == "leader":
            SHARD_GROUPS[data.group]["leader"] = data.url
            current_nodes = {RING.ring[k] for k in RING.sorted_keys if k in RING.ring}
            if data.group not in current_nodes:
                RING.add_node(data.group)
                print(f"Added group {data.group} to hash ring.")
                statlogger.increment('coordinator.shard.group_added', tags=[f'group:{data.group}'])
        elif data.role == "follower":
            if data.url not in SHARD_GROUPS[data.group]["followers"]:
                SHARD_GROUPS[data.group]["followers"].append(data.url)

        statlogger.gauge('coordinator.shard.total_nodes',
                         sum(1 for group in SHARD_GROUPS.values() for role in group.values() if role),
                         tags=[f'group:{data.group}'])
        print(f"Current state of SHARD_GROUPS: {SHARD_GROUPS}")

        duration_ms = (time.time() - start_time) * 1000
        structured_log("register_shard", data.name, "success", duration_ms=duration_ms, group=data.group,
                       role=data.role)

        return {"message": f"Shard {data.name} ({data.role}) registered to {data.group}"}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        structured_log("register_shard", data.name, "error", str(e), duration_ms, group=data.group, role=data.role)
        raise HTTPException(500, f"Registration failed: {e}")


@app.get("/shards")
@tracer.wrap(service='coordinator', resource='list_shards')
def list_shards():
    start_time = time.time()
    try:
        result = {"shard_groups": SHARD_GROUPS}
        duration_ms = (time.time() - start_time) * 1000
        structured_log("list_shards", status="success", duration_ms=duration_ms)
        return result
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        structured_log("list_shards", status="error", error=str(e), duration_ms=duration_ms)
        raise HTTPException(500, f"Failed to list shards: {e}")


@app.post("/register_table")
@tracer.wrap(service='coordinator', resource='register_table')
def register_table(defn: TableDef):
    start_time = time.time()
    try:
        if defn.name in TABLES:
            structured_log("register_table", defn.name, "error", "Table already exists",
                           (time.time() - start_time) * 1000)
            raise HTTPException(status_code=400, detail="Table already exists")

        TABLES[defn.name] = defn.dict()
        duration_ms = (time.time() - start_time) * 1000
        structured_log("register_table", defn.name, "success", duration_ms=duration_ms)

        return {"message": "Table registered", "table": defn.name}
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        structured_log("register_table", defn.name, "error", str(e), duration_ms)
        raise HTTPException(500, f"Table registration failed: {e}")


# --- Логіка Маршрутизації ---

def get_shard_group_for_key(key: str) -> Dict[str, Any]:
    group_name = RING.get_node(key)
    if not group_name or group_name not in SHARD_GROUPS:
        print(f"Error: No group found for key '{key}'. Ring returned '{group_name}'.")
        statlogger.increment('coordinator.errors.routing', tags=['type:no_group'])
        raise HTTPException(status_code=500, detail="No shard group available for key")
    return SHARD_GROUPS[group_name]


def get_write_node(key: str) -> str:
    """Для ЗАПИСУ (W=1) - завжди повертає ЛІДЕРА групи."""
    group = get_shard_group_for_key(key)
    if not group.get("leader"):
        statlogger.increment('coordinator.errors.write_failure', tags=['type:no_leader'])
        raise HTTPException(status_code=503, detail="No leader available for shard group")
    return group["leader"]


def get_all_read_nodes(key: str) -> List[str]:
    """Для ЧИТАННЯ (R=N=3) - повертає ВСІ репліки."""
    group = get_shard_group_for_key(key)

    all_replicas = []
    if group.get("leader"):
        all_replicas.append(group["leader"])
    all_replicas.extend(group.get("followers", []))

    if not all_replicas:
        statlogger.increment('coordinator.errors.read_failure', tags=['type:no_replicas'])
        raise HTTPException(status_code=503, detail="No replicas available for shard group")

    return all_replicas


def _composite(key: str, sort_key: Optional[str]) -> str:
    return f"{key}:{sort_key}" if sort_key else key


# --- Prometheus метрики ---
@app.get("/metrics")
def prometheus_metrics():
    """Prometheus-совместимые метрики"""
    metrics = []

    # Throughput
    metrics.append(f"coordinator_throughput_writes_total {REQUEST_COUNTERS['write_success']}")
    metrics.append(f"coordinator_throughput_reads_total {REQUEST_COUNTERS['read_success']}")

    # Error rate
    total_writes = REQUEST_COUNTERS['write_success'] + REQUEST_COUNTERS['write_error']
    total_reads = REQUEST_COUNTERS['read_success'] + REQUEST_COUNTERS['read_error']

    write_error_rate = REQUEST_COUNTERS['write_error'] / total_writes if total_writes > 0 else 0
    read_error_rate = REQUEST_COUNTERS['read_error'] / total_reads if total_reads > 0 else 0

    metrics.append(f"coordinator_error_rate_write {write_error_rate}")
    metrics.append(f"coordinator_error_rate_read {read_error_rate}")

    # Health metrics
    healthy_shards = sum(1 for group in SHARD_GROUPS.values() if group.get("leader"))
    metrics.append(f"coordinator_healthy_shards {healthy_shards}")
    metrics.append(f"coordinator_total_shard_groups {len(SHARD_GROUPS)}")

    return Response("\n".join(metrics), mimetype="text/plain")


# --- Health check ---
@app.get("/health")
def health_check():
    """Ендпоінт для перевірки здоров'я координатора"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "service": "coordinator",
        "shard_groups": len(SHARD_GROUPS),
        "healthy_groups": sum(1 for group in SHARD_GROUPS.values() if group.get("leader")),
        "ring_nodes": len(RING.ring) // RING.replicas
    }

    # Отправляем метрику здоровья
    health_value = 1 if health_status["healthy_groups"] > 0 else 0
    statlogger.gauge('coordinator.health.status', health_value)

    return health_status


# --- CRUD маршрутизація ---

@app.post("/create")
@tracer.wrap(service='coordinator', resource='create')
async def create_record(data: KeyValue):
    start_time = time.time()
    composite_key = _composite(data.key, data.sort_key)

    try:
        shard_url = get_write_node(composite_key)
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{shard_url}/create", json=data.dict(), timeout=30.0)

        duration_ms = (time.time() - start_time) * 1000

        if resp.status_code != 200:
            structured_log("create", composite_key, "error", f"HTTP {resp.status_code}", duration_ms)
            REQUEST_COUNTERS["write_error"] += 1
            statlogger.increment('coordinator.errors.write', tags=['status:not_200', 'op:create'])
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        # Успешная запись
        REQUEST_COUNTERS["write_success"] += 1
        statlogger.histogram('coordinator.latency.write', duration_ms, tags=['op:create'])
        statlogger.increment('coordinator.throughput.writes', tags=['op:create'])
        structured_log("create", composite_key, "success", duration_ms=duration_ms, shard_url=shard_url)

        return {"target_shard_url": shard_url, "response": resp.json()}

    except HTTPException as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("create", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:http_exception', 'op:create'])
        raise e
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("create", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:internal_failure', 'op:create'])
        raise HTTPException(500, f"Write failure: {e}")


@app.put("/update")
@tracer.wrap(service='coordinator', resource='update')
async def update_record(data: KeyValue):
    start_time = time.time()
    composite_key = _composite(data.key, data.sort_key)

    try:
        shard_url = get_write_node(composite_key)
        async with httpx.AsyncClient() as client:
            resp = await client.put(f"{shard_url}/update", json=data.dict(), timeout=30.0)

        duration_ms = (time.time() - start_time) * 1000

        if resp.status_code != 200:
            structured_log("update", composite_key, "error", f"HTTP {resp.status_code}", duration_ms)
            REQUEST_COUNTERS["write_error"] += 1
            statlogger.increment('coordinator.errors.write', tags=['status:not_200', 'op:update'])
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        REQUEST_COUNTERS["write_success"] += 1
        statlogger.histogram('coordinator.latency.write', duration_ms, tags=['op:update'])
        statlogger.increment('coordinator.throughput.writes', tags=['op:update'])
        structured_log("update", composite_key, "success", duration_ms=duration_ms, shard_url=shard_url)

        return {"target_shard_url": shard_url, "response": resp.json()}

    except HTTPException as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("update", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:http_exception', 'op:update'])
        raise e
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("update", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:internal_failure', 'op:update'])
        raise HTTPException(500, f"Write failure: {e}")


@app.delete("/delete/{table}/{key}")
@tracer.wrap(service='coordinator', resource='delete')
async def delete_record(table: str, key: str, sort_key: Optional[str] = None):
    start_time = time.time()
    composite_key = _composite(key, sort_key)

    try:
        shard_url = get_write_node(composite_key)
        async with httpx.AsyncClient() as client:
            resp = await client.delete(f"{shard_url}/delete/{table}/{key}",
                                       params={"sort_key": sort_key}, timeout=30.0)

        duration_ms = (time.time() - start_time) * 1000

        if resp.status_code not in (200, 204):
            structured_log("delete", composite_key, "error", f"HTTP {resp.status_code}", duration_ms)
            REQUEST_COUNTERS["write_error"] += 1
            statlogger.increment('coordinator.errors.write', tags=['status:not_200', 'op:delete'])
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        REQUEST_COUNTERS["write_success"] += 1
        statlogger.histogram('coordinator.latency.write', duration_ms, tags=['op:delete'])
        statlogger.increment('coordinator.throughput.writes', tags=['op:delete'])
        structured_log("delete", composite_key, "success", duration_ms=duration_ms, shard_url=shard_url)

        return {"target_shard_url": shard_url, "response": resp.json() if resp.status_code == 200 else "deleted"}

    except HTTPException as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("delete", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:http_exception', 'op:delete'])
        raise e
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["write_error"] += 1
        structured_log("delete", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.write', tags=['status:internal_failure', 'op:delete'])
        raise HTTPException(500, f"Write failure: {e}")


# --- ЧИТАННЯ з Quorum ---

@app.get("/read/{table}/{key}")
@tracer.wrap(service='coordinator', resource='read')
async def read_record(table: str, key: str, sort_key: Optional[str] = None):
    """
    READ: Читає з R реплік (Quorum Read) і повертає найновішу версію.
    """
    start_time = time.time()
    composite_key = _composite(key, sort_key)

    try:
        # 1. Отримуємо ВСІ репліки (R=N)
        replica_urls = get_all_read_nodes(composite_key)
        R = len(replica_urls)

        # 2. Паралельно опитуємо всі репліки
        async with httpx.AsyncClient() as client:
            tasks = []
            for url in replica_urls:
                read_url = f"{url}/read/{table}/{key}"
                params = {"sort_key": sort_key} if sort_key else {}
                tasks.append(client.get(read_url, params=params, timeout=5.0))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        # 3. Обробляємо відповіді та знаходимо найновішу
        successful_responses = []
        failed_nodes = 0

        for i, resp in enumerate(responses):
            if isinstance(resp, httpx.Response) and resp.status_code == 200:
                successful_responses.append(resp.json())
            else:
                failed_nodes += 1
                if isinstance(resp, Exception):
                    statlogger.increment('coordinator.read.node_failures', tags=['type:exception'])
                elif isinstance(resp, httpx.Response) and resp.status_code not in (404, 200):
                    statlogger.increment('coordinator.read.node_failures', tags=[f'status:{resp.status_code}'])

        # Метрика успешности quorum
        success_ratio = len(successful_responses) / R if R > 0 else 0
        statlogger.gauge('coordinator.read.quorum_success_ratio', success_ratio)

        if not successful_responses:
            REQUEST_COUNTERS["read_error"] += 1
            structured_log("read", composite_key, "error", "Key not found on any replica",
                           (time.time() - start_time) * 1000)
            statlogger.increment('coordinator.errors.read', tags=['status:not_found_on_any_replica'])
            raise HTTPException(404, f"Key not found on any replica.")

        # 4. Знаходимо найновішу версію (Last-Write-Wins)
        newest_response = None
        newest_version = ""
        newest_timestamp = ""

        for resp_data in successful_responses:
            data = resp_data.get("data", {})
            version = data.get("version", "")
            timestamp = data.get("timestamp", "")

            # Сравниваем по timestamp для LWW
            if timestamp > newest_timestamp:
                newest_timestamp = timestamp
                newest_version = version
                newest_response = resp_data

        if newest_response is None:
            REQUEST_COUNTERS["read_error"] += 1
            structured_log("read", composite_key, "error", "No valid data from replicas",
                           (time.time() - start_time) * 1000)
            statlogger.increment('coordinator.errors.read', tags=['status:no_valid_data'])
            raise HTTPException(404, "Key not found (all nodes returned 404 or errors)")

        # 5. Успешное чтение
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["read_success"] += 1

        statlogger.histogram('coordinator.latency.read', duration_ms)
        statlogger.increment('coordinator.throughput.reads', tags=['op:read'])
        statlogger.gauge('coordinator.read.replicas_responded', len(successful_responses))

        structured_log("read", composite_key, "success", duration_ms=duration_ms,
                       replicas_queried=R, replicas_responded=len(successful_responses))

        return {
            "message": "Quorum read successful",
            "R_queried": R,
            "successful_nodes": len(successful_responses),
            "failed_nodes": failed_nodes,
            "newest_data_from": newest_response.get("served_by"),
            "version": newest_version,
            "response": newest_response.get("data", {}).get("value")
        }

    except HTTPException as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["read_error"] += 1
        structured_log("read", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.read', tags=['status:http_exception'])
        raise e
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["read_error"] += 1
        structured_log("read", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.read', tags=['status:internal_failure'])
        raise HTTPException(500, f"Read failure: {e}")


@app.get("/exists/{table}/{key}")
@tracer.wrap(service='coordinator', resource='exists')
async def exists_record(table: str, key: str, sort_key: Optional[str] = None):
    """
    EXISTS: Використовує Quorum Read.
    Повертає 'exists: true' якщо *будь-яка* репліка повернула 200 OK.
    """
    start_time = time.time()
    composite_key = _composite(key, sort_key)

    try:
        replica_urls = get_all_read_nodes(composite_key)

        async with httpx.AsyncClient() as client:
            tasks = [client.get(f"{url}/exists/{table}/{key}",
                                params={"sort_key": sort_key} if sort_key else {},
                                timeout=5.0) for url in replica_urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        # Нам потрібен лише ОДИН вузол, який каже "exists: true"
        found = False
        responding_nodes = 0

        for resp in responses:
            if isinstance(resp, httpx.Response):
                responding_nodes += 1
                if resp.status_code == 200:
                    if resp.json().get("exists") == True:
                        found = True
                        break
            elif isinstance(resp, Exception):
                statlogger.increment('coordinator.read.node_failures', tags=['type:exception'])
            elif isinstance(resp, httpx.Response) and resp.status_code not in (404, 200):
                statlogger.increment('coordinator.read.node_failures', tags=[f'status:{resp.status_code}'])

        duration_ms = (time.time() - start_time) * 1000

        if found:
            REQUEST_COUNTERS["exists_success"] += 1
            structured_log("exists", composite_key, "success", duration_ms=duration_ms, found=True)
        else:
            REQUEST_COUNTERS["exists_error"] += 1
            structured_log("exists", composite_key, "success", duration_ms=duration_ms, found=False)

        statlogger.histogram('coordinator.latency.exists', duration_ms)
        statlogger.increment('coordinator.throughput.reads', tags=['op:exists'])
        statlogger.gauge('coordinator.exists.responding_nodes', responding_nodes)

        return {"exists": found, "message": "Quorum 'exists' check successful"}

    except HTTPException as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["exists_error"] += 1
        structured_log("exists", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.read', tags=['status:http_exception'])
        raise e
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        REQUEST_COUNTERS["exists_error"] += 1
        structured_log("exists", composite_key, "error", str(e), duration_ms)
        statlogger.increment('coordinator.errors.read', tags=['status:internal_failure'])
        raise HTTPException(500, f"Exists failure: {e}")