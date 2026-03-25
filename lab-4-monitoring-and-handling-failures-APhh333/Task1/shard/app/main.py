from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import os, httpx, asyncio, datetime, socket, time
from ddtrace import patch_all, tracer
from ddtrace.vendor.dogstatsd import DogStatsd
import logging
import json
from fastapi import Response

patch_all()

# Ініціалізація DogStatsD для кастомних метрик
statlogger = DogStatsd(host=os.getenv("DD_AGENT_HOST", "datadog"), port=8125)

# Настройка структурированного логирования для шардов
logging.basicConfig(level=logging.INFO)
shard_logger = logging.getLogger("shard")


def shard_structured_log(operation: str, key: str = None, status: str = "success",
                         error: str = None, duration_ms: float = None, **kwargs):
    """Структурированное логирование для шардов"""
    span = tracer.current_span()
    log_data = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "level": "ERROR" if error else "INFO",
        "service": "shard-service",
        "shard": SHARD_NAME,
        "role": REPLICA_ROLE,
        "group": SHARD_GROUP,
        "operation": operation,
        "trace_id": span.trace_id if span else None,
        "key": key,
        "status": status,
        "duration_ms": duration_ms,
        **kwargs
    }

    if error:
        log_data["error"] = error
        shard_logger.error(json.dumps(log_data))
    else:
        shard_logger.info(json.dumps(log_data))


app = FastAPI(title="Shard Node with Replication Log and Monitoring")

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

# Счетчики для метрик
SHARD_COUNTERS = {
    "write_success": 0,
    "write_error": 0,
    "read_success": 0,
    "read_error": 0,
    "replication_received": 0,
    "replication_errors": 0,
    "sync_requests": 0
}


class KeyValue(BaseModel):
    table: str
    key: str
    sort_key: Optional[str] = None
    value: dict


# --- Vector Clock для конфликт-резолюции ---
class VectorClock:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.versions: Dict[str, int] = {}

    def increment(self):
        self.versions[self.node_id] = self.versions.get(self.node_id, 0) + 1

    def merge(self, other: 'VectorClock') -> bool:
        """Объединяет векторные часы, возвращает True если есть конфликт"""
        has_conflict = False

        for node_id, counter in other.versions.items():
            if node_id in self.versions:
                if counter > self.versions[node_id]:
                    self.versions[node_id] = counter
                elif counter == self.versions[node_id]:
                    # Такая же версия - возможен конфликт
                    has_conflict = True
            else:
                self.versions[node_id] = counter

        return has_conflict

    def to_dict(self):
        return self.versions.copy()

    @classmethod
    def from_dict(cls, node_id: str, data: dict):
        vc = cls(node_id)
        vc.versions = data.copy()
        return vc


# --- ВНУТРІШНЯ ЛОГІКА РЕПЛІКАЦІЇ ---

def get_lag_seconds() -> float:
    """Вираховує затримку реплікації у секундах."""
    if not EVENT_LOG:
        return 0.0

    last_event_ts_str = EVENT_LOG[-1].get("timestamp")
    if last_event_ts_str:
        try:
            last_event_time = datetime.datetime.fromisoformat(last_event_ts_str.replace('Z', '+00:00'))
            current_time = datetime.datetime.now(datetime.timezone.utc)
            lag = (current_time - last_event_time).total_seconds()
            return max(0.0, lag)
        except ValueError:
            return 0.0
    return 0.0


def send_lag_metric(lag_s: float):
    """Уніфікована функція для відправки метрики Lag."""
    statlogger.gauge('shard.replication.lag_seconds', lag_s, tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])


def apply_event_with_conflict_resolution(event: Dict[str, Any]):
    """Применяет событие с разрешением конфликтов через Vector Clocks"""
    op = event.get("op")
    table = event.get("table")
    key = event.get("key")
    value = event.get("value")
    event_timestamp = event.get("timestamp")
    event_node = event.get("node_id", SHARD_NAME)

    start_time = time.time()

    try:
        # Создаем векторные часы для события
        event_vc = VectorClock(event_node)
        if "vector_clock" in event:
            event_vc = VectorClock.from_dict(event_node, event["vector_clock"])
        event_vc.increment()

        STORAGE.setdefault(table, {})

        if key in STORAGE[table]:
            current_data = STORAGE[table][key]
            current_vc = VectorClock.from_dict(SHARD_NAME, current_data.get("vector_clock", {}))

            # Проверяем конфликт
            has_conflict = current_vc.merge(event_vc)

            if has_conflict:
                # Разрешаем конфликт по LWW (последняя запись wins)
                current_timestamp = current_data.get("timestamp", "")
                if event_timestamp > current_timestamp:
                    # Новая версия побеждает
                    STORAGE[table][key] = {
                        "value": value,
                        "version": event_timestamp,
                        "vector_clock": event_vc.to_dict(),
                        "timestamp": event_timestamp
                    }
                    statlogger.increment('shard.conflicts.resolved',
                                         tags=[f'shard:{SHARD_NAME}', 'strategy:LWW', f'group:{SHARD_GROUP}'])
                    shard_structured_log("conflict_resolution", key, "resolved",
                                         strategy="LWW", duration_ms=(time.time() - start_time) * 1000)
            else:
                # Нет конфликта, обновляем данные
                STORAGE[table][key] = {
                    "value": value,
                    "version": event_timestamp,
                    "vector_clock": event_vc.to_dict(),
                    "timestamp": event_timestamp
                }
        else:
            # Новая запись
            STORAGE[table][key] = {
                "value": value,
                "version": event_timestamp,
                "vector_clock": event_vc.to_dict(),
                "timestamp": event_timestamp
            }

        EVENT_LOG.append(event)
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("apply_event", key, "success", duration_ms=duration_ms, event_op=op)

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("apply_event", key, "error", str(e), duration_ms, event_op=op)
        raise


async def sync_with_leader():
    """
    (Тільки для фоловерів)
    Звертається до лідера, щоб отримати пропущені події.
    """
    if REPLICA_ROLE != "follower" or not LEADER_URL:
        return

    current_offset = len(EVENT_LOG)
    start_time = time.time()

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{LEADER_URL}/sync", params={"offset": current_offset}, timeout=30.0)

            if resp.status_code != 200:
                shard_structured_log("sync", status="error", error=f"HTTP {resp.status_code}",
                                     duration_ms=(time.time() - start_time) * 1000)
                statlogger.increment('shard.sync.errors', tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])
                return

            missing_events = resp.json().get("events", [])
            events_count = len(missing_events)

            statlogger.gauge('shard.replication.lag_size', events_count,
                             tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])

            if missing_events:
                for event in missing_events:
                    apply_event_with_conflict_resolution(event)

                duration_ms = (time.time() - start_time) * 1000
                shard_structured_log("sync", status="success", duration_ms=duration_ms,
                                     events_synced=events_count)
                statlogger.increment('shard.sync.events_received', events_count,
                                     tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("sync", status="error", error=str(e), duration_ms=duration_ms)
        statlogger.increment('shard.sync.errors', tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])


# --- Prometheus метрики ---
@app.get("/metrics")
def prometheus_metrics():
    """Prometheus-совместимые метрики для шарда"""
    metrics = []

    # Throughput
    metrics.append(f"shard_throughput_writes_total{'{'}{SHARD_NAME}{'}'} {SHARD_COUNTERS['write_success']}")
    metrics.append(f"shard_throughput_reads_total{'{'}{SHARD_NAME}{'}'} {SHARD_COUNTERS['read_success']}")

    # Error rate
    total_writes = SHARD_COUNTERS['write_success'] + SHARD_COUNTERS['write_error']
    total_reads = SHARD_COUNTERS['read_success'] + SHARD_COUNTERS['read_error']

    write_error_rate = SHARD_COUNTERS['write_error'] / total_writes if total_writes > 0 else 0
    read_error_rate = SHARD_COUNTERS['read_error'] / total_reads if total_reads > 0 else 0

    metrics.append(f"shard_error_rate_write{'{'}{SHARD_NAME}{'}'} {write_error_rate}")
    metrics.append(f"shard_error_rate_read{'{'}{SHARD_NAME}{'}'} {read_error_rate}")

    # Storage metrics
    total_keys = sum(len(table) for table in STORAGE.values())
    metrics.append(f"shard_storage_keys_total{'{'}{SHARD_NAME}{'}'} {total_keys}")
    metrics.append(f"shard_event_log_size{'{'}{SHARD_NAME}{'}'} {len(EVENT_LOG)}")

    # Replication metrics
    metrics.append(f"shard_replication_received_total{'{'}{SHARD_NAME}{'}'} {SHARD_COUNTERS['replication_received']}")
    metrics.append(f"shard_sync_requests_total{'{'}{SHARD_NAME}{'}'} {SHARD_COUNTERS['sync_requests']}")

    return Response("\n".join(metrics), mimetype="text/plain")


# --- Health check ---
@app.get("/health")
def health_check():
    """Ендпоінт для перевірки здоров'я вузла."""
    health_data = {
        "status": "ok",
        "role": REPLICA_ROLE,
        "shard": SHARD_NAME,
        "group": SHARD_GROUP,
        "log_size": len(EVENT_LOG),
        "storage_keys": sum(len(table) for table in STORAGE.values()),
        "lag_seconds": get_lag_seconds() if REPLICA_ROLE == "follower" else 0,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    # Отправляем метрику здоровья
    health_value = 1
    statlogger.gauge('shard.health.status', health_value,
                     tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}', f'role:{REPLICA_ROLE}'])

    return health_data


async def self_health_check() -> bool:
    """Проверка здоровья текущего шарда"""
    try:
        # Проверяем что можем писать/читать из своего же хранилища
        test_key = f"health_check_{SHARD_NAME}_{int(time.time())}"
        STORAGE.setdefault("__health", {})
        STORAGE["__health"][test_key] = {
            "value": "test",
            "version": datetime.datetime.utcnow().isoformat(),
            "vector_clock": {SHARD_NAME: 1},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        test_value = STORAGE["__health"][test_key]
        del STORAGE["__health"][test_key]
        return True
    except Exception as e:
        shard_structured_log("health_check", status="error", error=str(e))
        return False


# --- ЗАПУСК ТА РЕЄСТРАЦІЯ ---
@app.on_event("startup")
async def register_and_sync():
    """
    1. Реєструє shard при запуску (включно з роллю і групою).
    2. Запускає єдиний цикл метрик та синхронізації.
    """
    payload = {
        "name": SHARD_NAME,
        "url": f"http://{SHARD_NAME}:8000",
        "group": SHARD_GROUP,
        "role": REPLICA_ROLE
    }

    print(f"🧭 Registering shard with coordinator: {payload}")
    statlogger.event('Shard Startup', f'Shard {SHARD_NAME} starting up with role {REPLICA_ROLE}', alert_type='info')

    # --- ДОДАНО: Цикл повторних спроб реєстрації ---
    registered = False
    for attempt in range(10):  # 10 спроб
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"{COORDINATOR_URL}/register_shard", json=payload, timeout=10.0)

                if resp.status_code == 200:
                    shard_structured_log("registration", status="success",
                                         coordinator_response=resp.status_code)
                    print(f"✅ Coordinator responded: {resp.status_code} - {resp.text}")
                    registered = True
                    break  # Вихід з циклу при успіху
                else:
                    print(f"⚠️ Attempt {attempt + 1}: Registration returned {resp.status_code}. Retrying...")

        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}: Failed to connect to coordinator ({e}). Retrying in 2s...")

        await asyncio.sleep(2)  # Чекаємо 2 секунди перед наступною спробою

    if not registered:
        shard_structured_log("registration", status="fatal", error="Could not register after multiple attempts")
        print("❌ FATAL: Could not register shard. Continuing without registration (Metrics only).")

    # Запуск єдиного циклу для метрик та синхронізації
    asyncio.create_task(periodic_metrics_and_sync())


async def periodic_metrics_and_sync():
    """Об'єднує синхронізацію та відправку метрик у фоновому режимі."""
    while True:
        await asyncio.sleep(10)  # Кожні 10 секунд

        # 1. СИНХРОНІЗАЦІЯ (Тільки для фоловерів)
        if REPLICA_ROLE == "follower":
            await sync_with_leader()

        # 2. ВІДПРАВКА МЕТРИКИ LAG (Для всіх)
        send_lag_metric(get_lag_seconds())

        # 3. Метрики здоровья хранилища
        total_keys = sum(len(table) for table in STORAGE.values())
        statlogger.gauge('shard.storage.keys_total', total_keys,
                         tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])

        # 4. Метрики размера лога
        statlogger.gauge('shard.event_log.size', len(EVENT_LOG),
                         tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])

        # 5. Health check метрика
        health_status = 1 if await self_health_check() else 0
        statlogger.gauge('shard.health.status', health_status,
                         tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}', f'role:{REPLICA_ROLE}'])


def _composite(k: str, sk: Optional[str]) -> str:
    return f"{k}:{sk}" if sk else k


async def fanout_to_followers(event: dict):
    start_time = time.time()
    async with httpx.AsyncClient() as client:
        valid_urls = [url for url in FOLLOWER_URLS if url]

        tasks = []
        for url in valid_urls:
            tasks.append(client.post(f"{url}/replicate", json=event, timeout=5))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        replication_time_ms = (time.time() - start_time) * 1000
        statlogger.histogram('shard.replication.fanout_latency', replication_time_ms,
                             tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])

        successful_replications = 0
        for url, resp in zip(valid_urls, responses):
            if isinstance(resp, Exception) or (isinstance(resp, httpx.Response) and resp.status_code != 200):
                shard_structured_log("fanout", status="error", error=str(resp),
                                     duration_ms=replication_time_ms, target=url)
                statlogger.increment('shard.replication.fanout_errors',
                                     tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])
            else:
                successful_replications += 1

        statlogger.gauge('shard.replication.fanout_success', successful_replications,
                         tags=[f'shard:{SHARD_NAME}', f'group:{SHARD_GROUP}'])


# --- CRUD (ЗАПИС) ---
@app.post("/create")
@tracer.wrap(service='shard-service', resource='create')
async def create(data: KeyValue):
    start_time = time.time()
    key = _composite(data.key, data.sort_key)

    if REPLICA_ROLE != "leader":
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("create", key, "error", "Forbidden: This node is a follower", duration_ms)
        SHARD_COUNTERS["write_error"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:create'])
        raise HTTPException(403, f"Forbidden: This node is a follower.")

    try:
        event = {
            "op": "create",
            "table": data.table,
            "key": key,
            "value": data.value,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "node_id": SHARD_NAME,
            "vector_clock": {SHARD_NAME: 1}
        }
        apply_event_with_conflict_resolution(event)

        asyncio.create_task(fanout_to_followers(event))

        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_success"] += 1
        statlogger.histogram('shard.latency.write', duration_ms, tags=[f'shard:{SHARD_NAME}', 'op:create'])
        statlogger.increment('shard.throughput.writes',
                             tags=[f'shard:{SHARD_NAME}', 'op:create', f'group:{SHARD_GROUP}'])
        shard_structured_log("create", key, "success", duration_ms=duration_ms)

        return {"message": "created", "key": key, "version": event["timestamp"]}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_error"] += 1
        shard_structured_log("create", key, "error", str(e), duration_ms)
        statlogger.increment('shard.errors.5xx', tags=[f'shard:{SHARD_NAME}', 'op:create'])
        raise HTTPException(500, f"Internal error during create: {e}")


@app.put("/update")
@tracer.wrap(service='shard-service', resource='update')
async def update(data: KeyValue):
    start_time = time.time()
    key = _composite(data.key, data.sort_key)

    if REPLICA_ROLE != "leader":
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("update", key, "error", "Forbidden: This node is a follower", duration_ms)
        SHARD_COUNTERS["write_error"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:update'])
        raise HTTPException(403, f"Forbidden: This node is a follower.")

    if data.table not in STORAGE or key not in STORAGE[data.table]:
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("update", key, "error", "Key not found", duration_ms)
        SHARD_COUNTERS["write_error"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:update'])
        raise HTTPException(404, "Key not found")

    try:
        # Получаем текущие векторные часы
        current_data = STORAGE[data.table][key]
        current_vc = VectorClock.from_dict(SHARD_NAME, current_data.get("vector_clock", {}))
        current_vc.increment()

        event = {
            "op": "update",
            "table": data.table,
            "key": key,
            "value": data.value,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "node_id": SHARD_NAME,
            "vector_clock": current_vc.to_dict()
        }
        apply_event_with_conflict_resolution(event)

        asyncio.create_task(fanout_to_followers(event))

        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_success"] += 1
        statlogger.histogram('shard.latency.write', duration_ms, tags=[f'shard:{SHARD_NAME}', 'op:update'])
        statlogger.increment('shard.throughput.writes',
                             tags=[f'shard:{SHARD_NAME}', 'op:update', f'group:{SHARD_GROUP}'])
        shard_structured_log("update", key, "success", duration_ms=duration_ms)

        return {"message": "updated", "key": key, "version": event["timestamp"]}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_error"] += 1
        shard_structured_log("update", key, "error", str(e), duration_ms)
        statlogger.increment('shard.errors.5xx', tags=[f'shard:{SHARD_NAME}', 'op:update'])
        raise HTTPException(500, f"Internal error during update: {e}")


@app.delete("/delete/{table}/{key}")
@tracer.wrap(service='shard-service', resource='delete')
async def delete(table: str, key: str, sort_key: Optional[str] = Query(None)):
    start_time = time.time()
    comp = _composite(key, sort_key)

    if REPLICA_ROLE != "leader":
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("delete", comp, "error", "Forbidden: This node is a follower", duration_ms)
        SHARD_COUNTERS["write_error"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:delete'])
        raise HTTPException(403, f"Forbidden: This node is a follower.")

    if table not in STORAGE or comp not in STORAGE[table]:
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("delete", comp, "error", "Key not found", duration_ms)
        SHARD_COUNTERS["write_error"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:delete'])
        raise HTTPException(404, "not found")

    try:
        event = {
            "op": "delete",
            "table": table,
            "key": comp,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "node_id": SHARD_NAME
        }
        apply_event_with_conflict_resolution(event)

        asyncio.create_task(fanout_to_followers(event))

        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_success"] += 1
        statlogger.histogram('shard.latency.write', duration_ms, tags=[f'shard:{SHARD_NAME}', 'op:delete'])
        statlogger.increment('shard.throughput.writes',
                             tags=[f'shard:{SHARD_NAME}', 'op:delete', f'group:{SHARD_GROUP}'])
        shard_structured_log("delete", comp, "success", duration_ms=duration_ms)

        return {"message": "deleted", "key": comp}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["write_error"] += 1
        shard_structured_log("delete", comp, "error", str(e), duration_ms)
        statlogger.increment('shard.errors.5xx', tags=[f'shard:{SHARD_NAME}', 'op:delete'])
        raise HTTPException(500, f"Internal error during delete: {e}")


# --- CRUD (ЧИТАННЯ) ---
@app.get("/read/{table}/{key}")
@tracer.wrap(service='shard-service', resource='read')
def read(table: str, key: str, sort_key: Optional[str] = Query(None)):
    start_time = time.time()
    comp = _composite(key, sort_key)

    try:
        if table not in STORAGE or comp not in STORAGE[table]:
            duration_ms = (time.time() - start_time) * 1000
            SHARD_COUNTERS["read_error"] += 1
            shard_structured_log("read", comp, "error", "Key not found", duration_ms)
            statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:read'])
            raise HTTPException(404, "not found")

        data = STORAGE[table][comp]
        duration_ms = (time.time() - start_time) * 1000

        SHARD_COUNTERS["read_success"] += 1
        statlogger.histogram('shard.latency.read', duration_ms, tags=[f'shard:{SHARD_NAME}', 'op:read'])
        statlogger.increment('shard.throughput.reads', tags=[f'shard:{SHARD_NAME}', 'op:read', f'group:{SHARD_GROUP}'])
        shard_structured_log("read", comp, "success", duration_ms=duration_ms)

        return {"key": comp, "data": data, "served_by": SHARD_NAME}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["read_error"] += 1
        shard_structured_log("read", comp, "error", str(e), duration_ms)
        statlogger.increment('shard.errors.5xx', tags=[f'shard:{SHARD_NAME}', 'op:read'])
        raise HTTPException(500, f"Internal error during read: {e}")


@app.get("/exists/{table}/{key}")
@tracer.wrap(service='shard-service', resource='exists')
def exists(table: str, key: str, sort_key: Optional[str] = Query(None)):
    start_time = time.time()
    comp = _composite(key, sort_key)

    try:
        exists = comp in STORAGE.get(table, {})
        duration_ms = (time.time() - start_time) * 1000

        if exists:
            SHARD_COUNTERS["read_success"] += 1
        else:
            SHARD_COUNTERS["read_error"] += 1
            statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:exists'])

        statlogger.histogram('shard.latency.read', duration_ms, tags=[f'shard:{SHARD_NAME}', 'op:exists'])
        statlogger.increment('shard.throughput.reads',
                             tags=[f'shard:{SHARD_NAME}', 'op:exists', f'group:{SHARD_GROUP}'])
        shard_structured_log("exists", comp, "success", duration_ms=duration_ms, exists=exists)

        return {"exists": exists, "served_by": SHARD_NAME}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["read_error"] += 1
        shard_structured_log("exists", comp, "error", str(e), duration_ms)
        statlogger.increment('shard.errors.5xx', tags=[f'shard:{SHARD_NAME}', 'op:exists'])
        raise HTTPException(500, f"Internal error during exists: {e}")


# --- ЕНДПОІНТИ РЕПЛІКАЦІЇ ---
@app.post("/replicate")
@tracer.wrap(service='shard-service', resource='replicate')
def replicate(event: Dict[str, Any]):
    start_time = time.time()

    if REPLICA_ROLE == "leader":
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("replicate", status="error", error="Leader cannot replicate from itself",
                             duration_ms=duration_ms)
        raise HTTPException(500, "Leader cannot replicate from itself")

    try:
        apply_event_with_conflict_resolution(event)

        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["replication_received"] += 1
        statlogger.increment('shard.replication.received', tags=[f'shard:{SHARD_NAME}'])
        shard_structured_log("replicate", event.get("key"), "success", duration_ms=duration_ms,
                             event_op=event.get("op"))

        return {"replicated": True, "op": event.get("op"), "node": SHARD_NAME}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["replication_errors"] += 1
        shard_structured_log("replicate", event.get("key"), "error", str(e), duration_ms, event_op=event.get("op"))
        statlogger.increment('shard.replication.errors', tags=[f'shard:{SHARD_NAME}'])
        raise HTTPException(500, f"Replication failed: {e}")


@app.get("/sync")
@tracer.wrap(service='shard-service', resource='sync')
def sync(offset: int = 0):
    start_time = time.time()

    if REPLICA_ROLE != "leader":
        duration_ms = (time.time() - start_time) * 1000
        shard_structured_log("sync", status="error", error="Forbidden: This node is a follower",
                             duration_ms=duration_ms)
        SHARD_COUNTERS["sync_requests"] += 1
        statlogger.increment('shard.errors.4xx', tags=[f'shard:{SHARD_NAME}', 'op:sync'])
        raise HTTPException(403, f"Forbidden: This node is a follower.")

    try:
        if offset < 0:
            offset = 0

        missing_events = EVENT_LOG[offset:]
        duration_ms = (time.time() - start_time) * 1000

        SHARD_COUNTERS["sync_requests"] += 1
        statlogger.increment('shard.sync.requests', tags=[f'shard:{SHARD_NAME}'])
        statlogger.gauge('shard.sync.events_sent', len(missing_events), tags=[f'shard:{SHARD_NAME}'])
        shard_structured_log("sync", status="success", duration_ms=duration_ms, events_sent=len(missing_events))

        return {"events": missing_events, "current_leader_offset": len(EVENT_LOG)}
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        SHARD_COUNTERS["sync_requests"] += 1
        shard_structured_log("sync", status="error", error=str(e), duration_ms=duration_ms)
        statlogger.increment('shard.sync.errors', tags=[f'shard:{SHARD_NAME}'])
        raise HTTPException(500, f"Sync failed: {e}")