"""
Aplicación ASÍNCRONA de administración de películas (PyMongo Async).

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839
Profesor: José Saúl de Lira Miramontes

Mismas funcionalidades que la app síncrona:
  1. Carga masiva de datos (bulk_write concurrente por lotes)
  2. CRUD asíncrono completo
  3. Consultas asíncronas (filtros, top-N, texto, agregaciones, paginación)

Primitivas exigidas por la tarea (todas se usan aquí):
  - asyncio.gather()    -> consultas concurrentes y variante de carga masiva
  - asyncio.TaskGroup() -> carga masiva principal por lotes concurrentes
  - asyncio.Semaphore   -> limita cuántos lotes/consultas van en paralelo
  - bulk_write()        -> inserciones/actualizaciones/borrados masivos

Requiere Python 3.11+ (asyncio.TaskGroup) y pymongo >= 4.10.

Uso:
    # Menú interactivo
    python -m async_app.app_async

    # Demostración automática
    python -m async_app.app_async --demo --n 2000 --concurrencia 10

    # Solo carga masiva (TaskGroup + Semaphore + bulk_write)
    python -m async_app.app_async --carga --n 10000 --lote 1000 --concurrencia 20
"""

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId
from pymongo import DeleteOne, InsertOne, UpdateOne

import config
from dataset.generador_peliculas import en_lotes, generar_peliculas


class AsyncMoviesDB:
    """Administración asíncrona de la colección de películas."""

    def __init__(self, client=None, uri=None, concurrencia=None):
        from pymongo import AsyncMongoClient

        # Tope de operaciones concurrentes contra MongoDB.
        # El Semaphore se crea DENTRO de cada operación (no aquí), porque
        # un Semaphore queda ligado al event loop en uso y el objeto DB
        # puede usarse desde varios loops (p. ej. el benchmark).
        self.concurrencia = concurrencia or config.DEFAULT_CONCURRENCY
        self.client = (client if client is not None else AsyncMongoClient(
            uri or config.MONGO_URI, serverSelectionTimeoutMS=5000
        ))
        self.sample = self.client[config.DB_SAMPLE][config.COLL_SAMPLE]
        self.work = self.client[config.DB_WORK][config.COLL_WORK]

    # -- utilidades ------------------------------------------------------
    async def close(self):
        await self.client.close()

    async def ping(self):
        return await self.client.admin.command("ping")

    async def coleccion_lectura(self):
        return await config.elegir_coleccion_lectura_async(self.sample, self.work)

    # -- 1. CARGA MASIVA -------------------------------------------------
    async def _insertar_lote(self, lote, sem):
        """Inserta un lote con bulk_write, respetando el Semaphore."""
        async with sem:
            ops = [InsertOne(doc) for doc in lote]
            resultado = await self.work.bulk_write(ops, ordered=False)
            return resultado.inserted_count

    async def carga_masiva_taskgroup(self, docs, tam_lote=1000):
        """Carga masiva con asyncio.TaskGroup + Semaphore + bulk_write.

        Cada lote es una tarea hija del TaskGroup; el Semaphore limita
        cuántas corren a la vez. Es el método principal de carga.
        """
        lotes = list(en_lotes(docs, tam_lote))
        sem = asyncio.Semaphore(self.concurrencia)
        insertados = 0
        inicio = time.perf_counter()
        if hasattr(asyncio, "TaskGroup"):
            async with asyncio.TaskGroup() as grupo:
                tareas = [grupo.create_task(self._insertar_lote(lote, sem))
                          for lote in lotes]
            insertados = sum(t.result() for t in tareas)
        else:  # Compatibilidad con Python < 3.11: gather + Semaphore.
            insertados = sum(await asyncio.gather(
                *(self._insertar_lote(lote, sem) for lote in lotes)))
        fin = time.perf_counter()
        return {
            "metodo": "TaskGroup+Semaphore+bulk_write",
            "insertados": insertados,
            "lotes": len(lotes),
            "segundos": fin - inicio,
            "docs_por_seg": insertados / (fin - inicio) if fin > inicio else 0,
        }

    async def carga_masiva_gather(self, docs, tam_lote=1000):
        """Variante de carga masiva con asyncio.gather + Semaphore."""
        lotes = list(en_lotes(docs, tam_lote))
        sem = asyncio.Semaphore(self.concurrencia)
        inicio = time.perf_counter()
        conteos = await asyncio.gather(
            *(self._insertar_lote(lote, sem) for lote in lotes))
        fin = time.perf_counter()
        insertados = sum(conteos)
        return {
            "metodo": "gather+Semaphore+bulk_write",
            "insertados": insertados,
            "lotes": len(lotes),
            "segundos": fin - inicio,
            "docs_por_seg": insertados / (fin - inicio) if fin > inicio else 0,
        }

    async def bulk_actualizar_ratings(self, pares_id_rating, tam_lote=1000):
        """Actualización masiva concurrente con bulk_write (UpdateOne)."""
        sem = asyncio.Semaphore(self.concurrencia)

        async def _lote(lote):
            async with sem:
                ops = [
                    UpdateOne({"_id": ObjectId(o) if isinstance(o, str) else o},
                              {"$set": {"imdb.rating": r}})
                    for o, r in lote
                ]
                res = await self.work.bulk_write(ops, ordered=False)
                return res.modified_count

        lotes = list(en_lotes(list(pares_id_rating), tam_lote))
        conteos = await asyncio.gather(*(_lote(l) for l in lotes))
        return sum(conteos)

    async def bulk_eliminar(self, ids, tam_lote=1000):
        """Eliminación masiva concurrente con bulk_write (DeleteOne)."""
        sem = asyncio.Semaphore(self.concurrencia)

        async def _lote(lote):
            async with sem:
                ops = [DeleteOne({"_id": ObjectId(i) if isinstance(i, str) else i})
                       for i in lote]
                res = await self.work.bulk_write(ops, ordered=False)
                return res.deleted_count

        lotes = list(en_lotes(list(ids), tam_lote))
        conteos = await asyncio.gather(*(_lote(l) for l in lotes))
        return sum(conteos)

    # -- 2. CRUD ---------------------------------------------------------
    async def crear_pelicula(self, doc):
        return (await self.work.insert_one(doc)).inserted_id

    async def crear_muchas(self, docs):
        """Crea varias películas de una vez. Devuelve lista de _id."""
        return (await self.work.insert_many(docs, ordered=False)).inserted_ids

    async def obtener_por_id(self, oid):
        if isinstance(oid, str):
            oid = ObjectId(oid)
        return await self.work.find_one({"_id": oid})

    async def obtener_por_titulo(self, titulo):
        return await self.work.find_one({"title": titulo})

    async def listar(self, filtro=None, pagina=1, por_pagina=10, orden=None):
        filtro = filtro or {}
        total = await self.work.count_documents(filtro)
        cursor = self.work.find(filtro).skip((pagina - 1) * por_pagina)
        cursor = cursor.limit(por_pagina)
        if orden:
            cursor = cursor.sort(orden)
        return await cursor.to_list(length=por_pagina), total

    async def actualizar_pelicula(self, oid, cambios):
        if isinstance(oid, str):
            oid = ObjectId(oid)
        res = await self.work.update_one({"_id": oid}, {"$set": cambios})
        return res.modified_count

    async def actualizar_muchos(self, filtro, cambios):
        res = await self.work.update_many(filtro, {"$set": cambios})
        return res.modified_count

    async def eliminar_pelicula(self, oid):
        if isinstance(oid, str):
            oid = ObjectId(oid)
        res = await self.work.delete_one({"_id": oid})
        return res.deleted_count

    async def eliminar_muchos(self, filtro):
        res = await self.work.delete_many(filtro)
        return res.deleted_count

    async def contar(self, filtro=None):
        return await self.work.count_documents(filtro or {})

    async def limpiar_trabajo(self):
        res = await self.work.delete_many({})
        return res.deleted_count

    # -- 3. CONSULTAS (individuales) -------------------------------------
    async def consulta_por_genero(self, genero, limite=10):
        coll, _ = await self.coleccion_lectura()
        cursor = coll.find({"genres": genero},
                           {"title": 1, "year": 1, "imdb.rating": 1}
                           ).limit(limite)
        return await cursor.to_list(length=limite)

    async def consulta_por_rango_anios(self, anio_ini, anio_fin, limite=10):
        coll, _ = await self.coleccion_lectura()
        cursor = (coll.find({"year": {"$gte": anio_ini, "$lte": anio_fin}},
                            {"title": 1, "year": 1, "genres": 1})
                  .sort("year", 1).limit(limite))
        return await cursor.to_list(length=limite)

    async def consulta_top_rating(self, n=10, min_votos=1000):
        coll, _ = await self.coleccion_lectura()
        cursor = (coll.find({"imdb.votes": {"$gte": min_votos}},
                            {"title": 1, "year": 1, "imdb": 1})
                  .sort("imdb.rating", -1).limit(n))
        return await cursor.to_list(length=n)

    async def consulta_buscar_titulo(self, texto, limite=10):
        coll, _ = await self.coleccion_lectura()
        cursor = coll.find({"title": {"$regex": texto, "$options": "i"}},
                           {"title": 1, "year": 1, "genres": 1}
                           ).limit(limite)
        return await cursor.to_list(length=limite)

    async def consulta_agregacion_por_genero(self, limite=10):
        coll, _ = await self.coleccion_lectura()
        pipeline = [
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres",
                        "total": {"$sum": 1},
                        "rating_promedio": {"$avg": "$imdb.rating"}}},
            {"$sort": {"total": -1}},
            {"$limit": limite},
        ]
        # aggregate() SÍ es corutina (a diferencia de find()): hay que
        # esperarla para obtener el cursor y luego pedir la lista.
        cursor = await coll.aggregate(pipeline)
        return await cursor.to_list(length=limite)

    async def consulta_promedio_por_anio(self, anio_ini=2000, anio_fin=2024):
        coll, _ = await self.coleccion_lectura()
        pipeline = [
            {"$match": {"year": {"$gte": anio_ini, "$lte": anio_fin}}},
            {"$group": {"_id": "$year",
                        "total": {"$sum": 1},
                        "rating_promedio": {"$avg": "$imdb.rating"}}},
            {"$sort": {"_id": 1}},
        ]
        cursor = await coll.aggregate(pipeline)
        return await cursor.to_list(length=None)

    async def consulta_top_directores(self, n=10):
        coll, _ = await self.coleccion_lectura()
        pipeline = [
            {"$unwind": "$directors"},
            {"$group": {"_id": "$directors", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": n},
        ]
        cursor = await coll.aggregate(pipeline)
        return await cursor.to_list(length=n)

    # -- 3b. CONSULTAS CONCURRENTES --------------------------------------
    @staticmethod
    async def _consulta_limitada(sem, coro_fn, *args, **kwargs):
        """Ejecuta una consulta respetando el Semaphore."""
        async with sem:
            return await coro_fn(*args, **kwargs)

    async def consultas_concurrentes_gather(self, n_consultas=20):
        """Lanza n consultas mezcladas en paralelo con asyncio.gather.

        Devuelve (resultados, segundos). Las consultas se reparten entre
        los distintos tipos para simular carga realista.
        """
        consultas = []
        sem = asyncio.Semaphore(self.concurrencia)
        generos = ["Drama", "Comedia", "Acción", "Thriller", "Sci-Fi"]
        for i in range(n_consultas):
            tipo = i % 5
            if tipo == 0:
                consultas.append(self._consulta_limitada(
                    sem, self.consulta_por_genero, generos[i % len(generos)], 10))
            elif tipo == 1:
                consultas.append(self._consulta_limitada(
                    sem, self.consulta_top_rating, 10))
            elif tipo == 2:
                consultas.append(self._consulta_limitada(
                    sem, self.consulta_por_rango_anios, 2000, 2020, 10))
            elif tipo == 3:
                consultas.append(self._consulta_limitada(
                    sem, self.consulta_buscar_titulo, "sombras", 10))
            else:
                consultas.append(self._consulta_limitada(
                    sem, self.consulta_agregacion_por_genero, 5))
        inicio = time.perf_counter()
        resultados = await asyncio.gather(*consultas)
        fin = time.perf_counter()
        return resultados, fin - inicio

    async def consultas_concurrentes_taskgroup(self, n_consultas=20):
        """Mismo experimento con asyncio.TaskGroup. Devuelve (total_docs, s)."""
        total = {"docs": 0}
        sem = asyncio.Semaphore(self.concurrencia)

        async def _una(i):
            async with sem:
                if i % 2 == 0:
                    res = await self.consulta_top_rating(10)
                else:
                    res = await self.consulta_por_genero("Drama", 10)
                total["docs"] += len(res)

        inicio = time.perf_counter()
        if hasattr(asyncio, "TaskGroup"):
            async with asyncio.TaskGroup() as grupo:
                for i in range(n_consultas):
                    grupo.create_task(_una(i))
        else:
            await asyncio.gather(*(_una(i) for i in range(n_consultas)))
        fin = time.perf_counter()
        return total["docs"], fin - inicio


# ---------------------------------------------------------------------------
# Demostración automática y menú interactivo
# ---------------------------------------------------------------------------
async def demo(db, n=2000, tam_lote=1000, concurrencia=10):
    print(f"\n=== DEMO ASÍNCRONA ({n} docs, lote={tam_lote}, "
          f"conc={concurrencia}) ===")
    print("[1/5] Generando documentos...", flush=True)
    docs = generar_peliculas(n)

    print("[2/5] Carga masiva (TaskGroup + Semaphore + bulk_write)...",
          flush=True)
    stats = await db.carga_masiva_taskgroup(docs, tam_lote)
    print(f"      Insertados: {stats['insertados']} en {stats['segundos']:.2f}s "
          f"({stats['docs_por_seg']:.0f} docs/s)")

    print("[3/5] CRUD: crear -> leer -> actualizar -> eliminar...", flush=True)
    nueva = generar_peliculas(1, semilla=999)[0]
    nueva["title"] = "Película de prueba ASYNC 362839"
    oid = await db.crear_pelicula(nueva)
    print(f"      creado: {oid}")
    print(f"      leído: {(await db.obtener_por_id(oid))['title']}")
    await db.actualizar_pelicula(oid, {"imdb.rating": 9.9})
    print(f"      actualizado rating -> "
          f"{(await db.obtener_por_id(oid))['imdb']['rating']}")
    await db.eliminar_pelicula(oid)
    print(f"      eliminado (quedan {await db.contar()} docs)")

    print("[4/5] Consultas concurrentes con gather (20 consultas)...",
          flush=True)
    resultados, seg = await db.consultas_concurrentes_gather(20)
    total_docs = sum(len(r) for r in resultados)
    print(f"      {total_docs} docs en {seg:.2f}s")

    print("[5/5] Consultas concurrentes con TaskGroup (20 consultas)...",
          flush=True)
    total_docs, seg = await db.consultas_concurrentes_taskgroup(20)
    print(f"      {total_docs} docs en {seg:.2f}s")
    print("=== FIN DEMO ASÍNCRONA ===\n")
    return stats


async def menu(db):
    while True:
        print("\n---- APP ASÍNCRONA - sample_mflix/movies ----")
        print(" 1. Carga masiva (TaskGroup + Semaphore + bulk_write)")
        print(" 2. Carga masiva variante (gather + Semaphore + bulk_write)")
        print(" 3. Crear película")
        print(" 4. Buscar por título")
        print(" 5. Listar (paginado)")
        print(" 6. Actualizar rating por título")
        print(" 7. Eliminar por título")
        print(" 8. Consultas individuales")
        print(" 9. Consultas concurrentes (gather vs TaskGroup)")
        print("10. Contar documentos")
        print("11. Vaciar colección de trabajo")
        print(" 0. Salir")
        op = input("Opción: ").strip()
        try:
            if op in ("1", "2"):
                n = int(input("¿Cuántos documentos? [2000]: ") or 2000)
                lote = int(input("¿Tamaño de lote? [1000]: ") or 1000)
                fn = (db.carga_masiva_taskgroup if op == "1"
                      else db.carga_masiva_gather)
                stats = await fn(generar_peliculas(n), lote)
                print(f"Insertados {stats['insertados']} en "
                      f"{stats['segundos']:.2f}s ({stats['docs_por_seg']:.0f} docs/s)")
            elif op == "3":
                titulo = input("Título: ").strip()
                anio = int(input("Año: ").strip() or 2024)
                doc = generar_peliculas(1)[0]
                doc.update({"title": titulo, "year": anio})
                print("Creado:", await db.crear_pelicula(doc))
            elif op == "4":
                titulo = input("Título exacto: ").strip()
                print(await db.obtener_por_titulo(titulo) or "No encontrada.")
            elif op == "5":
                pag = int(input("Página [1]: ") or 1)
                docs, total = await db.listar(pagina=pag, por_pagina=10,
                                              orden=[("year", -1)])
                print(f"Total: {total}. Página {pag}:")
                for d in docs:
                    print(f"  - {d.get('title')} ({d.get('year')})")
            elif op == "6":
                titulo = input("Título exacto: ").strip()
                rating = float(input("Nuevo rating: ").strip())
                doc = await db.obtener_por_titulo(titulo)
                if doc:
                    await db.actualizar_pelicula(doc["_id"], {"imdb.rating": rating})
                    print("Actualizada.")
                else:
                    print("No encontrada.")
            elif op == "7":
                titulo = input("Título exacto: ").strip()
                doc = await db.obtener_por_titulo(titulo)
                if doc:
                    await db.eliminar_pelicula(doc["_id"])
                    print("Eliminada.")
                else:
                    print("No encontrada.")
            elif op == "8":
                print("Top rating:")
                for p in await db.consulta_top_rating(5):
                    print(f"  - {p.get('title')} -> {p['imdb']['rating']}")
                print("Agregación por género:")
                for g in await db.consulta_agregacion_por_genero(5):
                    print(f"  - {g['_id']}: {g['total']}")
            elif op == "9":
                n = int(input("¿Cuántas consultas? [20]: ") or 20)
                _, seg_g = await db.consultas_concurrentes_gather(n)
                _, seg_t = await db.consultas_concurrentes_taskgroup(n)
                print(f"gather: {seg_g:.2f}s | TaskGroup: {seg_t:.2f}s")
            elif op == "10":
                print("Documentos:", await db.contar())
            elif op == "11":
                if input("Vaciar proyecto2a.movies_carga? (s/N): ").lower() == "s":
                    print("Eliminados:", await db.limpiar_trabajo())
            elif op == "0":
                break
            else:
                print("Opción no válida.")
        except Exception as exc:
            print(f"Error: {exc}")


async def amain(args):
    db = AsyncMoviesDB(uri=args.uri, concurrencia=args.concurrencia)
    try:
        await db.ping()
        print(f"Conectado a {config.DB_WORK}.{config.COLL_WORK}")
    except Exception as exc:
        print(f"No se pudo conectar a MongoDB ({exc}).")
        print("Revisa MONGO_URI en .env o levanta MongoDB local/Atlas.")
        await db.close()
        return 1
    try:
        await config.ensure_async_indexes(db.work)
        if args.demo:
            await demo(db, n=args.n, tam_lote=args.lote,
                       concurrencia=args.concurrencia)
        elif args.carga:
            docs = generar_peliculas(args.n)
            stats = await db.carga_masiva_taskgroup(docs, args.lote)
            print(f"Insertados {stats['insertados']} en {stats['segundos']:.2f}s "
                  f"({stats['docs_por_seg']:.0f} docs/s)")
        else:
            await menu(db)
    finally:
        await db.close()
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="App asíncrona MongoDB (PyMongo Async)")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--carga", action="store_true")
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--lote", type=int, default=1000)
    parser.add_argument("--concurrencia", type=int,
                        default=config.DEFAULT_CONCURRENCY)
    parser.add_argument("--uri", default=None)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
