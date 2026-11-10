"""
Benchmark comparativo: aplicación síncrona vs asíncrona (MongoDB real).

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839
Profesor: José Saúl de Lira Miramontes

Experimentos (misma carga de trabajo en ambas apps):
  A. Carga masiva: N documentos en lotes (sync secuencial vs async TaskGroup)
  B. Consultas: Q consultas mezcladas (sync secuencial vs async gather)
  C. Actualización masiva: bulk_write secuencial vs concurrente

Cada experimento limpia la colección de trabajo antes de correr, para que
ambas versiones partan del mismo estado. NUNCA toca sample_mflix.

Uso:
    python -m comparacion.benchmark --n 5000 --lote 1000 --consultas 50
    python -m comparacion.benchmark --n 5000 --rapido   # menos repeticiones

Resultados: imprime tabla comparativa y guarda
  comparacion/resultados_benchmark.json
"""

import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from async_app.app_async import AsyncMoviesDB
from dataset.generador_peliculas import generar_peliculas
from sync_app.app_sync import SyncMoviesDB


def tabla(filas, encabezados):
    """Imprime una tabla ASCII sin dependencias externas."""
    anchos = [len(h) for h in encabezados]
    for fila in filas:
        for i, celda in enumerate(fila):
            anchos[i] = max(anchos[i], len(str(celda)))
    linea = "+" + "+".join("-" * (a + 2) for a in anchos) + "+"

    def _fila(celdas):
        return "|" + "|".join(f" {str(c).ljust(a)} " for c, a in zip(celdas, anchos)) + "|"

    print(linea)
    print(_fila(encabezados))
    print(linea)
    for fila in filas:
        print(_fila(fila))
    print(linea)


def experimento_carga_sync(db, docs, tam_lote):
    db.limpiar_trabajo()
    return db.carga_masiva_bulk_write(docs, tam_lote)


async def experimento_carga_async(db, docs, tam_lote):
    await db.limpiar_trabajo()
    return await db.carga_masiva_taskgroup(docs, tam_lote)


def experimento_consultas_sync(db, n_consultas):
    """Q consultas mezcladas ejecutadas una tras otra (secuencial)."""
    generos = ["Drama", "Comedia", "Acción", "Thriller", "Sci-Fi"]
    inicio = time.perf_counter()
    total_docs = 0
    for i in range(n_consultas):
        tipo = i % 5
        if tipo == 0:
            total_docs += len(db.consulta_por_genero(generos[i % len(generos)], 10))
        elif tipo == 1:
            total_docs += len(db.consulta_top_rating(10))
        elif tipo == 2:
            total_docs += len(db.consulta_por_rango_anios(2000, 2020, 10))
        elif tipo == 3:
            total_docs += len(db.consulta_buscar_titulo("sombras", 10))
        else:
            total_docs += len(db.consulta_agregacion_por_genero(5))
    fin = time.perf_counter()
    return {"docs": total_docs, "consultas": n_consultas,
            "segundos": fin - inicio}


async def experimento_consultas_async(db, n_consultas):
    resultados, segundos = await db.consultas_concurrentes_gather(n_consultas)
    return {"docs": sum(len(r) for r in resultados), "consultas": n_consultas,
            "segundos": segundos}


def experimento_update_sync(db, n):
    """Actualización masiva secuencial con bulk_write."""
    ids = [d["_id"] for d in
           db.work.find({}, {"_id": 1}).limit(n).sort("_id", 1)]
    pares = [(i, 7.5) for i in ids]
    inicio = time.perf_counter()
    mod = db.bulk_actualizar_ratings(pares, tam_lote=1000)
    return {"modificados": mod, "segundos": time.perf_counter() - inicio}


async def experimento_update_async(db, n):
    ids = [d["_id"] async for d in
           db.work.find({}, {"_id": 1}).sort("_id", 1).limit(n)]
    pares = [(i, 7.5) for i in ids]
    inicio = time.perf_counter()
    mod = await db.bulk_actualizar_ratings(pares, tam_lote=1000)
    return {"modificados": mod, "segundos": time.perf_counter() - inicio}


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark sync vs async (requiere MongoDB)")
    parser.add_argument("--n", type=int, default=5000, help="Docs carga masiva")
    parser.add_argument("--lote", type=int, default=1000, help="Tamaño de lote")
    parser.add_argument("--consultas", type=int, default=50,
                        help="Nº de consultas por experimento")
    parser.add_argument("--concurrencia", type=int,
                        default=config.DEFAULT_CONCURRENCY)
    parser.add_argument("--repetir", type=int, default=3,
                        help="Repeticiones por experimento")
    parser.add_argument("--rapido", action="store_true",
                        help="1 repetición (corrida rápida)")
    parser.add_argument("--uri", default=None)
    args = parser.parse_args()
    if args.rapido:
        args.repetir = 1

    print("=" * 70)
    print("BENCHMARK: app síncrona (PyMongo) vs app asíncrona (PyMongo Async)")
    print(f"Docs={args.n} lote={args.lote} consultas={args.consultas} "
          f"conc={args.concurrencia} repeticiones={args.repetir}")
    print("=" * 70)

    sync_db = SyncMoviesDB(uri=args.uri)
    try:
        sync_db.ping()
    except Exception as exc:
        print(f"No hay conexión a MongoDB ({exc}).")
        print("Opciones:")
        print("  1. Levanta MongoDB local:  mongod  o  docker run -d -p 27017:27017 mongo")
        print("  2. Usa Atlas gratis y crea .env con MONGO_URI=mongodb+srv://...")
        print("  3. Corre la demo simulada sin BD: python -m comparacion.demo_concurrencia")
        return 1
    config.ensure_sync_indexes(sync_db.work)

    async_db = AsyncMoviesDB(uri=args.uri, concurrencia=args.concurrencia)
    resultados = {"parametros": vars(args), "experimentos": {}}

    # Todo lo asíncrono corre en UN solo event loop (un único asyncio.run):
    # más seguro para el AsyncMongoClient y los Semaphore.
    async def _correr_todo():
        try:
            await async_db.ping()
        except Exception as exc:
            print(f"La app asíncrona no pudo conectar: {exc}")
            return None
        await config.ensure_async_indexes(async_db.work)

        docs = generar_peliculas(args.n)
        resumen = []

        # -- Experimento A: carga masiva ---------------------------------
        print("\n[A] Carga masiva (bulk_write por lotes)...")
        t_sync, t_async = [], []
        for r in range(args.repetir):
            s = experimento_carga_sync(sync_db, docs, args.lote)
            t_sync.append(s["segundos"])
            print(f"    sync  rep{r + 1}: {s['segundos']:.2f}s ({s['docs_por_seg']:.0f} docs/s)")
            s = await experimento_carga_async(async_db, docs, args.lote)
            t_async.append(s["segundos"])
            print(f"    async rep{r + 1}: {s['segundos']:.2f}s ({s['docs_por_seg']:.0f} docs/s)")
        resultados["experimentos"]["carga_masiva"] = {
            "sync_seg": t_sync, "async_seg": t_async}
        resumen.append(("A. Carga masiva", min(t_sync), min(t_async)))

        # -- Experimento B: consultas ------------------------------------
        print(f"\n[B] {args.consultas} consultas mezcladas...")
        t_sync, t_async = [], []
        for r in range(args.repetir):
            s = experimento_consultas_sync(sync_db, args.consultas)
            t_sync.append(s["segundos"])
            print(f"    sync  rep{r + 1}: {s['segundos']:.2f}s ({s['docs']} docs)")
            s = await experimento_consultas_async(async_db, args.consultas)
            t_async.append(s["segundos"])
            print(f"    async rep{r + 1}: {s['segundos']:.2f}s ({s['docs']} docs)")
        resultados["experimentos"]["consultas"] = {
            "sync_seg": t_sync, "async_seg": t_async}
        resumen.append((f"B. {args.consultas} consultas", min(t_sync), min(t_async)))

        # -- Experimento C: actualización masiva --------------------------
        n_upd = min(args.n, 2000)
        print(f"\n[C] Actualización masiva de {n_upd} ratings (bulk_write)...")
        t_sync, t_async = [], []
        for r in range(args.repetir):
            s = experimento_update_sync(sync_db, n_upd)
            t_sync.append(s["segundos"])
            print(f"    sync  rep{r + 1}: {s['segundos']:.2f}s ({s['modificados']} mods)")
            s = await experimento_update_async(async_db, n_upd)
            t_async.append(s["segundos"])
            print(f"    async rep{r + 1}: {s['segundos']:.2f}s ({s['modificados']} mods)")
        resultados["experimentos"]["update_masivo"] = {
            "sync_seg": t_sync, "async_seg": t_async}
        resumen.append(("C. Update masivo", min(t_sync), min(t_async)))
        return resumen

    resumen = asyncio.run(_correr_todo())
    if resumen is None:
        return 1

    # -- Tabla comparativa final ------------------------------------------
    print("\n" + "=" * 70)
    print("RESULTADO (mejor tiempo de cada experimento)")
    print("=" * 70)
    filas = []
    for nombre, ts, ta in resumen:
        speedup = ts / ta if ta > 0 else 0
        ganador = "ASYNC" if ta < ts else "SYNC"
        filas.append((nombre, f"{ts:.2f} s", f"{ta:.2f} s",
                      f"x{speedup:.2f}", ganador))
    tabla(filas, ["Experimento", "Síncrono", "Asíncrono", "Speedup", "Ganador"])

    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "resultados_benchmark.json")
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=1, default=str)
    print(f"\nResultados guardados en {ruta}")

    sync_db.close()
    asyncio.run(async_db.close())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
