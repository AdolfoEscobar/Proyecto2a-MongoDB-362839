"""
Importa un archivo movies.json estilo sample_mflix a la colección de trabajo.

Sirve si descargaste el dataset de ejemplo (arreglo JSON o JSON-lines)
y quieres cargarlo tal cual en lugar de usar el generador.

Uso:
    python -m scripts.cargar_sample_mflix --archivo movies.json --lote 1000
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from dataset.generador_peliculas import en_lotes
from sync_app.app_sync import SyncMoviesDB


def leer_documentos(ruta):
    with open(ruta, encoding="utf-8") as f:
        texto = f.read().strip()
    if texto.startswith("["):
        docs = json.loads(texto)
    else:  # JSON-lines: un documento por línea
        docs = [json.loads(linea) for linea in texto.splitlines() if linea.strip()]

    # Limpia _id con formato Extended JSON {"$oid": ...} si viene así.
    from bson import ObjectId

    for doc in docs:
        oid = doc.get("_id")
        if isinstance(oid, dict) and "$oid" in oid:
            try:
                doc["_id"] = ObjectId(oid["$oid"])
            except Exception:
                del doc["_id"]
    return docs


def main():
    parser = argparse.ArgumentParser(description="Importar movies.json")
    parser.add_argument("--archivo", required=True)
    parser.add_argument("--lote", type=int, default=1000)
    parser.add_argument("--uri", default=None)
    args = parser.parse_args()

    docs = leer_documentos(args.archivo)
    print(f"Documentos leídos: {len(docs)}")
    db = SyncMoviesDB(uri=args.uri)
    try:
        db.ping()
    except Exception as exc:
        print(f"No hay conexión a MongoDB: {exc}")
        return 1
    total = 0
    for lote in en_lotes(docs, args.lote):
        res = db.work.insert_many(lote, ordered=False)
        total += len(res.inserted_ids)
        print(f"  ... {total}/{len(docs)}")
    print(f"Importados {total} documentos a {config.DB_WORK}.{config.COLL_WORK}")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
