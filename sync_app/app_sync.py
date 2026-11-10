"""
Aplicación SÍNCRONA de administración de películas (PyMongo clásico).

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839
Profesor: José Saúl de Lira Miramontes

Funcionalidades (las mismas que la app asíncrona):
  1. Carga masiva de datos (insert_many y bulk_write por lotes)
  2. CRUD síncrono completo
  3. Consultas síncronas (filtros, top-N, texto, agregaciones, paginación)

Uso:
    # Menú interactivo
    python -m sync_app.app_sync

    # Demostración automática (carga + CRUD + consultas)
    python -m sync_app.app_sync --demo --n 2000

    # Solo carga masiva
    python -m sync_app.app_sync --carga --n 10000 --lote 1000 --metodo bulk
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId
from pymongo import DeleteOne, InsertOne, MongoClient, UpdateOne

import config
from dataset.generador_peliculas import en_lotes, generar_peliculas


class SyncMoviesDB:
    """Administración síncrona de la colección de películas."""

    def __init__(self, client=None, uri=None):
        self.client = (client if client is not None else MongoClient(
            uri or config.MONGO_URI, serverSelectionTimeoutMS=5000
        ))
        self.sample = self.client[config.DB_SAMPLE][config.COLL_SAMPLE]
        self.work = self.client[config.DB_WORK][config.COLL_WORK]

    # -- utilidades ------------------------------------------------------
    def close(self):
        self.client.close()

    def ping(self):
        """Verifica conexión con el servidor. Lanza excepción si falla."""
        return self.client.admin.command("ping")

    def colleccion_lectura(self):
        """Colección para consultas (sample si existe, si no trabajo)."""
        return config.elegir_coleccion_lectura(self.sample, self.work)

    # -- 1. CARGA MASIVA -------------------------------------------------
    def carga_masiva_insert_many(self, docs, tam_lote=1000):
        """Carga masiva con insert_many por lotes. Devuelve dict con métricas."""
        insertados = 0
        inicio = time.perf_counter()
        for lote in en_lotes(docs, tam_lote):
            resultado = self.work.insert_many(lote, ordered=False)
            insertados += len(resultado.inserted_ids)
        fin = time.perf_counter()
        return {
            "metodo": "insert_many",
            "insertados": insertados,
            "lotes": (len(docs) + tam_lote - 1) // tam_lote,
            "segundos": fin - inicio,
            "docs_por_seg": insertados / (fin - inicio) if fin > inicio else 0,
        }

    def carga_masiva_bulk_write(self, docs, tam_lote=1000):
        """Carga masiva con bulk_write (operaciones InsertOne) por lotes."""
        insertados = 0
        inicio = time.perf_counter()
        for lote in en_lotes(docs, tam_lote):
            ops = [InsertOne(doc) for doc in lote]
            resultado = self.work.bulk_write(ops, ordered=False)
            insertados += resultado.inserted_count
        fin = time.perf_counter()
        return {
            "metodo": "bulk_write",
            "insertados": insertados,
            "lotes": (len(docs) + tam_lote - 1) // tam_lote,
            "segundos": fin - inicio,
            "docs_por_seg": insertados / (fin - inicio) if fin > inicio else 0,
        }

    def bulk_actualizar_ratings(self, pares_id_rating, tam_lote=1000):
        """Actualización masiva con bulk_write (operaciones UpdateOne).

        pares_id_rating: lista de tuplas (ObjectId/str, nuevo_rating).
        """
        modificados = 0
        for lote in en_lotes(list(pares_id_rating), tam_lote):
            ops = [
                UpdateOne({"_id": ObjectId(oid) if isinstance(oid, str) else oid},
                          {"$set": {"imdb.rating": rating}})
                for oid, rating in lote
            ]
            resultado = self.work.bulk_write(ops, ordered=False)
            modificados += resultado.modified_count
        return modificados

    def bulk_eliminar(self, ids, tam_lote=1000):
        """Eliminación masiva con bulk_write (operaciones DeleteOne)."""
        eliminados = 0
        for lote in en_lotes(list(ids), tam_lote):
            ops = [DeleteOne({"_id": ObjectId(i) if isinstance(i, str) else i})
                   for i in lote]
            resultado = self.work.bulk_write(ops, ordered=False)
            eliminados += resultado.deleted_count
        return eliminados

    # -- 2. CRUD ---------------------------------------------------------
    def crear_pelicula(self, doc):
        """Inserta una película. Devuelve el _id generado."""
        return self.work.insert_one(doc).inserted_id

    def obtener_por_id(self, oid):
        """Busca una película por _id (str u ObjectId)."""
        if isinstance(oid, str):
            oid = ObjectId(oid)
        return self.work.find_one({"_id": oid})

    def obtener_por_titulo(self, titulo):
        """Busca una película por título exacto."""
        return self.work.find_one({"title": titulo})

    def listar(self, filtro=None, pagina=1, por_pagina=10, orden=None):
        """Lista paginada. Devuelve (docs, total)."""
        filtro = filtro or {}
        total = self.work.count_documents(filtro)
        cursor = self.work.find(filtro).skip((pagina - 1) * por_pagina).limit(por_pagina)
        if orden:
            cursor = cursor.sort(orden)
        return list(cursor), total

    def actualizar_pelicula(self, oid, cambios):
        """Actualiza una película por _id. Devuelve nº de modificados (0/1)."""
        if isinstance(oid, str):
            oid = ObjectId(oid)
        return self.work.update_one({"_id": oid}, {"$set": cambios}).modified_count

    def actualizar_muchos(self, filtro, cambios):
        """Actualiza todas las películas que cumplan el filtro."""
        return self.work.update_many(filtro, {"$set": cambios}).modified_count

    def eliminar_pelicula(self, oid):
        """Elimina una película por _id. Devuelve nº de eliminados (0/1)."""
        if isinstance(oid, str):
            oid = ObjectId(oid)
        return self.work.delete_one({"_id": oid}).deleted_count

    def eliminar_muchos(self, filtro):
        """Elimina todas las películas que cumplan el filtro."""
        return self.work.delete_many(filtro).deleted_count

    def contar(self, filtro=None):
        return self.work.count_documents(filtro or {})

    def limpiar_trabajo(self):
        """Borra la colección de trabajo (NO toca sample_mflix)."""
        return self.work.delete_many({}).deleted_count

    # -- 3. CONSULTAS ----------------------------------------------------
    def consulta_por_genero(self, genero, limite=10):
        coll, _ = self.colleccion_lectura()
        return list(coll.find({"genres": genero},
                              {"title": 1, "year": 1, "imdb.rating": 1}
                              ).limit(limite))

    def consulta_por_rango_anios(self, anio_ini, anio_fin, limite=10):
        coll, _ = self.colleccion_lectura()
        return list(coll.find({"year": {"$gte": anio_ini, "$lte": anio_fin}},
                              {"title": 1, "year": 1, "genres": 1}
                              ).sort("year", 1).limit(limite))

    def consulta_top_rating(self, n=10, min_votos=1000):
        """Top-N películas por imdb.rating con mínimo de votos."""
        coll, _ = self.colleccion_lectura()
        return list(coll.find({"imdb.votes": {"$gte": min_votos}},
                              {"title": 1, "year": 1, "imdb": 1}
                              ).sort("imdb.rating", -1).limit(n))

    def consulta_buscar_titulo(self, texto, limite=10):
        """Búsqueda de títulos que contengan el texto (insensible a mayúsculas)."""
        coll, _ = self.colleccion_lectura()
        return list(coll.find({"title": {"$regex": texto, "$options": "i"}},
                              {"title": 1, "year": 1, "genres": 1}
                              ).limit(limite))

    def consulta_agregacion_por_genero(self, limite=10):
        """Nº de películas y rating promedio por género (ordenado por conteo)."""
        coll, _ = self.colleccion_lectura()
        pipeline = [
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres",
                        "total": {"$sum": 1},
                        "rating_promedio": {"$avg": "$imdb.rating"}}},
            {"$sort": {"total": -1}},
            {"$limit": limite},
        ]
        return list(coll.aggregate(pipeline))

    def consulta_promedio_por_anio(self, anio_ini=2000, anio_fin=2024):
        """Rating promedio y conteo por año en un rango."""
        coll, _ = self.colleccion_lectura()
        pipeline = [
            {"$match": {"year": {"$gte": anio_ini, "$lte": anio_fin}}},
            {"$group": {"_id": "$year",
                        "total": {"$sum": 1},
                        "rating_promedio": {"$avg": "$imdb.rating"}}},
            {"$sort": {"_id": 1}},
        ]
        return list(coll.aggregate(pipeline))

    def consulta_top_directores(self, n=10):
        """Directores con más películas."""
        coll, _ = self.colleccion_lectura()
        pipeline = [
            {"$unwind": "$directors"},
            {"$group": {"_id": "$directors", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": n},
        ]
        return list(coll.aggregate(pipeline))


# ---------------------------------------------------------------------------
# Demostración automática y menú interactivo
# ---------------------------------------------------------------------------
def demo(db, n=2000, tam_lote=1000, metodo="bulk"):
    print(f"\n=== DEMO SÍNCRONA ({n} docs, lote={tam_lote}, método={metodo}) ===")
    print("[1/4] Generando documentos...", flush=True)
    docs = generar_peliculas(n)

    print("[2/4] Carga masiva...", flush=True)
    if metodo == "bulk":
        stats = db.carga_masiva_bulk_write(docs, tam_lote)
    else:
        stats = db.carga_masiva_insert_many(docs, tam_lote)
    print(f"      Insertados: {stats['insertados']} en {stats['segundos']:.2f}s "
          f"({stats['docs_por_seg']:.0f} docs/s)")

    print("[3/4] CRUD: crear -> leer -> actualizar -> eliminar...", flush=True)
    nueva = generar_peliculas(1, semilla=999)[0]
    nueva["title"] = "Película de prueba SYNC 362839"
    oid = db.crear_pelicula(nueva)
    print(f"      creado: {oid}")
    print(f"      leído: {db.obtener_por_id(oid)['title']}")
    db.actualizar_pelicula(oid, {"imdb.rating": 9.9})
    print(f"      actualizado rating -> {db.obtener_por_id(oid)['imdb']['rating']}")
    db.eliminar_pelicula(oid)
    print(f"      eliminado (quedan {db.contar()} docs)")

    print("[4/4] Consultas...", flush=True)
    generos = db.consulta_agregacion_por_genero(3)
    print(f"      top géneros: {[(g['_id'], g['total']) for g in generos]}")
    top = db.consulta_top_rating(3)
    print(f"      top rating: {[(p['title'][:30], p['imdb']['rating']) for p in top]}")
    print("=== FIN DEMO SÍNCRONA ===\n")
    return stats


def menu(db):
    while True:
        print("\n---- APP SÍNCRONA - sample_mflix/movies ----")
        print(" 1. Carga masiva (generador + bulk_write)")
        print(" 2. Crear película")
        print(" 3. Buscar por título")
        print(" 4. Listar (paginado)")
        print(" 5. Actualizar rating por título")
        print(" 6. Eliminar por título")
        print(" 7. Consultas (género / años / top / agregación)")
        print(" 8. Contar documentos")
        print(" 9. Vaciar colección de trabajo")
        print(" 0. Salir")
        op = input("Opción: ").strip()
        try:
            if op == "1":
                n = int(input("¿Cuántos documentos? [2000]: ") or 2000)
                lote = int(input("¿Tamaño de lote? [1000]: ") or 1000)
                stats = db.carga_masiva_bulk_write(generar_peliculas(n), lote)
                print(f"Insertados {stats['insertados']} en {stats['segundos']:.2f}s")
            elif op == "2":
                titulo = input("Título: ").strip()
                anio = int(input("Año: ").strip() or 2024)
                doc = generar_peliculas(1)[0]
                doc.update({"title": titulo, "year": anio})
                print("Creado:", db.crear_pelicula(doc))
            elif op == "3":
                titulo = input("Título exacto: ").strip()
                print(db.obtener_por_titulo(titulo) or "No encontrada.")
            elif op == "4":
                pag = int(input("Página [1]: ") or 1)
                docs, total = db.listar(pagina=pag, por_pagina=10,
                                        orden=[("year", -1)])
                print(f"Total: {total}. Página {pag}:")
                for d in docs:
                    print(f"  - {d.get('title')} ({d.get('year')})")
            elif op == "5":
                titulo = input("Título exacto: ").strip()
                rating = float(input("Nuevo rating: ").strip())
                doc = db.obtener_por_titulo(titulo)
                if doc:
                    db.actualizar_pelicula(doc["_id"], {"imdb.rating": rating})
                    print("Actualizada.")
                else:
                    print("No encontrada.")
            elif op == "6":
                titulo = input("Título exacto: ").strip()
                doc = db.obtener_por_titulo(titulo)
                if doc:
                    db.eliminar_pelicula(doc["_id"])
                    print("Eliminada.")
                else:
                    print("No encontrada.")
            elif op == "7":
                print(" a) Por género  b) Rango años  c) Top rating")
                print(" d) Buscar título  e) Agregación géneros  f) Top directores")
                sub = input("Consulta: ").strip().lower()
                if sub == "a":
                    for p in db.consulta_por_genero(input("Género: ").strip()):
                        print(f"  - {p.get('title')} ({p.get('year')})")
                elif sub == "b":
                    a = int(input("Año inicial: ")); b = int(input("Año final: "))
                    for p in db.consulta_por_rango_anios(a, b):
                        print(f"  - {p.get('title')} ({p.get('year')})")
                elif sub == "c":
                    for p in db.consulta_top_rating(10):
                        print(f"  - {p.get('title')} -> {p['imdb']['rating']}")
                elif sub == "d":
                    for p in db.consulta_buscar_titulo(input("Texto: ").strip()):
                        print(f"  - {p.get('title')} ({p.get('year')})")
                elif sub == "e":
                    for g in db.consulta_agregacion_por_genero():
                        print(f"  - {g['_id']}: {g['total']} "
                              f"(prom {g['rating_promedio']:.2f})")
                elif sub == "f":
                    for d in db.consulta_top_directores():
                        print(f"  - {d['_id']}: {d['total']}")
            elif op == "8":
                print("Documentos:", db.contar())
            elif op == "9":
                if input("Vaciar proyecto2a.movies_carga? (s/N): ").lower() == "s":
                    print("Eliminados:", db.limpiar_trabajo())
            elif op == "0":
                break
            else:
                print("Opción no válida.")
        except Exception as exc:
            print(f"Error: {exc}")


def main():
    parser = argparse.ArgumentParser(description="App síncrona MongoDB (PyMongo)")
    parser.add_argument("--demo", action="store_true", help="Demostración automática")
    parser.add_argument("--carga", action="store_true", help="Solo carga masiva")
    parser.add_argument("--n", type=int, default=2000, help="Nº documentos")
    parser.add_argument("--lote", type=int, default=1000, help="Tamaño de lote")
    parser.add_argument("--metodo", choices=["bulk", "insert"], default="bulk")
    parser.add_argument("--uri", default=None, help="URI de MongoDB")
    args = parser.parse_args()

    db = SyncMoviesDB(uri=args.uri)
    try:
        db.ping()
        print(f"Conectado a {config.DB_WORK}.{config.COLL_WORK}")
    except Exception as exc:
        print(f"No se pudo conectar a MongoDB ({exc}).")
        print("Revisa MONGO_URI en .env o levanta MongoDB local/Atlas.")
        return 1
    try:
        config.ensure_sync_indexes(db.work)
        if args.demo:
            demo(db, n=args.n, tam_lote=args.lote, metodo=args.metodo)
        elif args.carga:
            docs = generar_peliculas(args.n)
            fn = (db.carga_masiva_bulk_write if args.metodo == "bulk"
                  else db.carga_masiva_insert_many)
            stats = fn(docs, args.lote)
            print(f"Insertados {stats['insertados']} en {stats['segundos']:.2f}s "
                  f"({stats['docs_por_seg']:.0f} docs/s)")
        else:
            menu(db)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
