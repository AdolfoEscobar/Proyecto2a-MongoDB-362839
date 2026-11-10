"""
Verifica la conexión a MongoDB y reporta el estado de las colecciones.

Uso:
    python -m scripts.verificar_conexion
    python -m scripts.verificar_conexion --uri "mongodb+srv://..."
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


def main():
    parser = argparse.ArgumentParser(description="Verificar conexión MongoDB")
    parser.add_argument("--uri", default=None)
    args = parser.parse_args()
    if args.uri:
        config.MONGO_URI = args.uri

    from pymongo import MongoClient

    print(f"URI: {config.MONGO_URI.split('@')[-1]}")  # sin credenciales
    try:
        client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
        info = client.server_info()
        print(f"OK: conectado a MongoDB {info.get('version')}")
    except Exception as exc:
        print(f"FALLO: {exc}")
        print("\nLevanta MongoDB local o define MONGO_URI en .env (ver .env.example).")
        return 1

    sample = client[config.DB_SAMPLE][config.COLL_SAMPLE]
    work = client[config.DB_WORK][config.COLL_WORK]
    try:
        n_sample = sample.estimated_document_count()
    except Exception:
        n_sample = -1
    try:
        n_work = work.estimated_document_count()
    except Exception:
        n_work = -1
    print(f"  {config.DB_SAMPLE}.{config.COLL_SAMPLE}: {n_sample} docs "
          f"{'(sample oficial)' if n_sample > 0 else '(vacío/ausente: se usará trabajo)'}")
    print(f"  {config.DB_WORK}.{config.COLL_WORK}: {n_work} docs (trabajo)")
    if n_sample <= 0:
        print("\nTip: para usar el sample oficial de Atlas, en tu clúster elige")
        print("'Load Sample Dataset'. Si no, el generador incluido llena la")
        print("colección de trabajo con documentos compatibles.")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
