"""
Configuración compartida del Proyecto 2a.

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839
Profesor: José Saúl de Lira Miramontes

Este módulo centraliza:
  - URI de conexión (variable de entorno MONGO_URI o valor por defecto local)
  - Nombres de base de datos y colecciones
  - Fábricas de clientes síncrono y asíncrono
  - Creación de índices recomendados para la colección de trabajo

Diseño de colecciones (para NO dañar el sample original):
  - Lectura del sample oficial : sample_mflix.movies (solo lectura)
  - Escritura/benchmarks       : proyecto2a.movies_carga (se puede borrar)

Si sample_mflix no existe en el servidor, las apps trabajan sobre
proyecto2a.movies_carga (que se llena con el generador incluido).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass  # python-dotenv es opcional; se usan variables de entorno directas.

# ---------------------------------------------------------------------------
# Parámetros de conexión (editables con variables de entorno o archivo .env)
# ---------------------------------------------------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/")

# Sample oficial (solo lectura en consultas de demostración).
DB_SAMPLE = os.environ.get("DB_SAMPLE", "sample_mflix")
COLL_SAMPLE = os.environ.get("COLL_SAMPLE", "movies")

# Colección de trabajo (carga masiva, CRUD y benchmarks). Es seguro borrarla.
DB_WORK = os.environ.get("DB_WORK", "proyecto2a")
COLL_WORK = os.environ.get("COLL_WORK", "movies_carga")

# Concurrencia por defecto de la app asíncrona.
DEFAULT_CONCURRENCY = int(os.environ.get("ASYNC_CONCURRENCY", "10"))
DEFAULT_BATCH_SIZE = int(os.environ.get("BULK_BATCH_SIZE", "1000"))


# ---------------------------------------------------------------------------
# Fábricas de clientes
# ---------------------------------------------------------------------------
def get_sync_client():
    """Crea un MongoClient síncrono (PyMongo clásico)."""
    from pymongo import MongoClient

    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_async_client():
    """Crea un AsyncMongoClient (PyMongo Async). Requiere pymongo >= 4.10."""
    from pymongo import AsyncMongoClient

    return AsyncMongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_sync_collections(client=None):
    """Devuelve (coleccion_sample, coleccion_trabajo) síncronas."""
    own = client is None
    client = client or get_sync_client()
    sample = client[DB_SAMPLE][COLL_SAMPLE]
    work = client[DB_WORK][COLL_WORK]
    return sample, work, (client if own else None)


async def get_async_collections(client=None):
    """Devuelve (coleccion_sample, coleccion_trabajo) asíncronas."""
    own = client is None
    client = client or get_async_client()
    sample = client[DB_SAMPLE][COLL_SAMPLE]
    work = client[DB_WORK][COLL_WORK]
    return sample, work, (client if own else None)


# ---------------------------------------------------------------------------
# Índices recomendados para la colección de trabajo
# ---------------------------------------------------------------------------
INDEXES = [
    ([("title", 1)], {}),
    ([("year", 1)], {}),
    ([("genres", 1)], {}),
    ([("imdb.rating", -1)], {}),
    ([("directors", 1)], {}),
]


def ensure_sync_indexes(collection):
    """Crea índices en la colección de trabajo (versión síncrona)."""
    for keys, kwargs in INDEXES:
        try:
            collection.create_index(keys, **kwargs)
        except Exception as exc:  # pragma: no cover - depende del servidor
            print(f"[aviso] no se pudo crear índice {keys}: {exc}")


async def ensure_async_indexes(collection):
    """Crea índices en la colección de trabajo (versión asíncrona)."""
    for keys, kwargs in INDEXES:
        try:
            await collection.create_index(keys, **kwargs)
        except Exception as exc:  # pragma: no cover - depende del servidor
            print(f"[aviso] no se pudo crear índice {keys}: {exc}")


def elegir_coleccion_lectura(sample_coll, work_coll, preferir_sample=True):
    """Elige colección para consultas de lectura (síncrona).

    Usa sample_mflix.movies si existe y tiene documentos; si no,
    usa la colección de trabajo.
    """
    if preferir_sample:
        try:
            if sample_coll.estimated_document_count() > 0:
                return sample_coll, f"{DB_SAMPLE}.{COLL_SAMPLE}"
        except Exception:
            pass
    return work_coll, f"{DB_WORK}.{COLL_WORK}"


async def elegir_coleccion_lectura_async(sample_coll, work_coll, preferir_sample=True):
    """Versión asíncrona de elegir_coleccion_lectura."""
    if preferir_sample:
        try:
            if await sample_coll.estimated_document_count() > 0:
                return sample_coll, f"{DB_SAMPLE}.{COLL_SAMPLE}"
        except Exception:
            pass
    return work_coll, f"{DB_WORK}.{COLL_WORK}"
