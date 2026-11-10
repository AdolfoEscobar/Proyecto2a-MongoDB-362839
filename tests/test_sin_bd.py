"""
Pruebas SIN base de datos para el Proyecto 2a.

Verifican toda la lógica de ambas apps con colecciones simuladas
(en memoria) que imitan la API de PyMongo / PyMongo Async:
  - generador de documentos y partición en lotes
  - CRUD síncrono y asíncrono
  - carga masiva (insert_many, bulk_write, TaskGroup, gather)
  - consultas, agregaciones y paginación
  - respeto del Semaphore (concurrencia máxima)

Uso (desde la raíz del proyecto, sin MongoDB):
    python -m unittest discover -s tests -v
"""

import asyncio
import copy
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId
from pymongo import DeleteOne, InsertOne, UpdateOne

from dataset.generador_peliculas import en_lotes, generar_pelicula, generar_peliculas


# ---------------------------------------------------------------------------
# Motor de filtros/agregaciones mínimo (solo lo que usan las apps)
# ---------------------------------------------------------------------------
def _valor(doc, clave_punteada):
    actual = doc
    for parte in clave_punteada.split("."):
        if not isinstance(actual, dict) or parte not in actual:
            return None
        actual = actual[parte]
    return actual


def coincide(doc, filtro):
    """Subconjunto de la semántica de filtros de MongoDB."""
    for clave, cond in (filtro or {}).items():
        valor = _valor(doc, clave)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, esperado in cond.items():
                if op == "$gte" and not (valor is not None and valor >= esperado):
                    return False
                if op == "$lte" and not (valor is not None and valor <= esperado):
                    return False
                if op == "$regex":
                    flags = re.IGNORECASE if cond.get("$options") == "i" else 0
                    if not re.search(esperado, str(valor or ""), flags):
                        return False
                if op == "$options":
                    continue
        else:
            if isinstance(valor, list):
                if cond not in valor:
                    return False
            elif valor != cond:
                return False
    return True


def _aplicar_set(doc, cambios):
    for clave, valor in cambios.items():
        partes = clave.split(".")
        destino = doc
        for parte in partes[:-1]:
            destino = destino.setdefault(parte, {})
        destino[partes[-1]] = valor


def ejecutar_pipeline(docs, pipeline):
    """Mini-motor de agregación: $match, $unwind, $group, $sort, $limit."""
    datos = [copy.deepcopy(d) for d in docs]
    for etapa in pipeline:
        if "$match" in etapa:
            datos = [d for d in datos if coincide(d, etapa["$match"])]
        elif "$unwind" in etapa:
            campo = etapa["$unwind"].lstrip("$")
            nuevos = []
            for d in datos:
                for v in (_valor(d, campo) or []):
                    copia = copy.deepcopy(d)
                    copia[campo] = v
                    nuevos.append(copia)
            datos = nuevos
        elif "$group" in etapa:
            spec = etapa["$group"]
            grupos = {}
            for d in datos:
                gid = _valor(d, spec["_id"].lstrip("$")) if spec["_id"] != 1 else 1
                g = grupos.setdefault(gid, {"_id": gid, "__n__": 0})
                g["__n__"] += 1
                for nombre, expr in spec.items():
                    if nombre == "_id":
                        continue
                    if expr == {"$sum": 1}:
                        g[nombre] = g.get(nombre, 0) + 1
                    elif isinstance(expr, dict) and "$avg" in expr:
                        g.setdefault(nombre + "__sum__", 0.0)
                        g.setdefault(nombre + "__c__", 0)
                        v = _valor(d, expr["$avg"].lstrip("$"))
                        if v is not None:
                            g[nombre + "__sum__"] += v
                            g[nombre + "__c__"] += 1
            datos = []
            for g in grupos.values():
                fila = {"_id": g["_id"]}
                for nombre, expr in spec.items():
                    if nombre == "_id":
                        continue
                    if expr == {"$sum": 1}:
                        fila[nombre] = g.get(nombre, 0)
                    elif isinstance(expr, dict) and "$avg" in expr:
                        c = g.get(nombre + "__c__", 0)
                        fila[nombre] = (g.get(nombre + "__sum__", 0.0) / c
                                        if c else None)
                datos.append(fila)
        elif "$sort" in etapa:
            for campo, direccion in reversed(list(etapa["$sort"].items())):
                datos.sort(key=lambda d: (_valor(d, campo)
                                           if not campo.startswith("_")
                                           else d.get(campo)),
                           reverse=(direccion < 0))
        elif "$limit" in etapa:
            datos = datos[:etapa["$limit"]]
    return datos


class Resultado:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _aplicar_ops(almacen, ops):
    ins = mod = dele = 0
    for op in ops:
        if isinstance(op, InsertOne):
            doc = copy.deepcopy(op._doc)
            doc.setdefault("_id", ObjectId())
            almacen[str(doc["_id"])] = doc
            ins += 1
        elif isinstance(op, UpdateOne):
            for doc in almacen.values():
                if coincide(doc, op._filter):
                    _aplicar_set(doc, op._doc.get("$set", {}))
                    mod += 1
                    break
        elif isinstance(op, DeleteOne):
            for k, doc in list(almacen.items()):
                if coincide(doc, op._filter):
                    del almacen[k]
                    dele += 1
                    break
    return Resultado(inserted_count=ins, modified_count=mod, deleted_count=dele,
                     inserted_ids=[])


# ---------------------------------------------------------------------------
# Colecciones simuladas síncronas
# ---------------------------------------------------------------------------
class CursorSync:
    def __init__(self, docs):
        self.docs = docs

    def skip(self, n):
        self.docs = self.docs[n:]
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def sort(self, key_or_list, direction=None):
        pares = ([(key_or_list, direction)] if isinstance(key_or_list, str)
                 else list(key_or_list))
        for campo, direccion in reversed(pares):
            self.docs.sort(key=lambda d: (_valor(d, campo) or 0),
                           reverse=(direccion < 0))
        return self

    def __iter__(self):
        return iter(self.docs)


class ColeccionFakeSync:
    def __init__(self):
        self.almacen = {}

    # escrituras
    def insert_one(self, doc):
        copia = copy.deepcopy(doc)
        copia.setdefault("_id", ObjectId())
        self.almacen[str(copia["_id"])] = copia
        return Resultado(inserted_id=copia["_id"])

    def insert_many(self, docs, ordered=True):
        ids = [self.insert_one(d).inserted_id for d in docs]
        return Resultado(inserted_ids=ids)

    def bulk_write(self, ops, ordered=True):
        return _aplicar_ops(self.almacen, ops)

    def update_one(self, filtro, cambio):
        for doc in self.almacen.values():
            if coincide(doc, filtro):
                _aplicar_set(doc, cambio.get("$set", {}))
                return Resultado(modified_count=1)
        return Resultado(modified_count=0)

    def update_many(self, filtro, cambio):
        n = 0
        for doc in self.almacen.values():
            if coincide(doc, filtro):
                _aplicar_set(doc, cambio.get("$set", {}))
                n += 1
        return Resultado(modified_count=n)

    def delete_one(self, filtro):
        for k, doc in list(self.almacen.items()):
            if coincide(doc, filtro):
                del self.almacen[k]
                return Resultado(deleted_count=1)
        return Resultado(deleted_count=0)

    def delete_many(self, filtro):
        antes = len(self.almacen)
        self.almacen = {k: d for k, d in self.almacen.items()
                        if not coincide(d, filtro)}
        return Resultado(deleted_count=antes - len(self.almacen))

    # lecturas
    def find_one(self, filtro=None, *args, **kwargs):
        for doc in self.almacen.values():
            if coincide(doc, filtro):
                return copy.deepcopy(doc)
        return None

    def find(self, filtro=None, proyeccion=None, *args, **kwargs):
        docs = [copy.deepcopy(d) for d in self.almacen.values()
                if coincide(d, filtro)]
        return CursorSync(docs)

    def count_documents(self, filtro):
        return sum(1 for d in self.almacen.values() if coincide(d, filtro))

    def estimated_document_count(self):
        return len(self.almacen)

    def aggregate(self, pipeline):
        return ejecutar_pipeline(list(self.almacen.values()), pipeline)

    def create_index(self, *args, **kwargs):
        return "fake_index"


class BDFakeSync(dict):
    def __getitem__(self, nombre):
        if nombre not in self:
            self[nombre] = ColeccionFakeSync()
        return super().__getitem__(nombre)


class ClienteFakeSync(dict):
    def __getitem__(self, nombre):
        if nombre not in self:
            self[nombre] = BDFakeSync()
        return super().__getitem__(nombre)

    def close(self):
        pass

    @property
    def admin(self):
        class _Admin:
            @staticmethod
            def command(*args, **kwargs):
                return {"ok": 1}
        return _Admin()


# ---------------------------------------------------------------------------
# Colecciones simuladas asíncronas (misma lógica, API awaitable)
# ---------------------------------------------------------------------------
class CursorAsync(CursorSync):
    async def to_list(self, length=None):
        docs = self.docs if length is None else self.docs[:length]
        return [copy.deepcopy(d) for d in docs]

    def __aiter__(self):
        async def _gen():
            for d in self.docs:
                yield copy.deepcopy(d)
        return _gen()


class ColeccionFakeAsync(ColeccionFakeSync):
    """Versión async: añade latencia artificial y mide concurrencia."""

    def __init__(self, latencia=0.001):
        super().__init__()
        self.latencia = latencia
        self.en_vuelo = 0
        self.max_en_vuelo = 0

    async def _espera(self):
        self.en_vuelo += 1
        self.max_en_vuelo = max(self.max_en_vuelo, self.en_vuelo)
        try:
            await asyncio.sleep(self.latencia)
        finally:
            self.en_vuelo -= 1

    async def insert_one(self, doc):
        await self._espera()
        return super().insert_one(doc)

    async def insert_many(self, docs, ordered=True):
        await self._espera()
        # Llamada directa a la versión síncrona para no re-despachar
        # a insert_one asíncrono (que devolvería corutinas).
        ids = [ColeccionFakeSync.insert_one(self, d).inserted_id
               for d in docs]
        return Resultado(inserted_ids=ids)

    async def bulk_write(self, ops, ordered=True):
        await self._espera()
        return _aplicar_ops(self.almacen, ops)

    async def update_one(self, filtro, cambio):
        await self._espera()
        return super().update_one(filtro, cambio)

    async def update_many(self, filtro, cambio):
        await self._espera()
        return super().update_many(filtro, cambio)

    async def delete_one(self, filtro):
        await self._espera()
        return super().delete_one(filtro)

    async def delete_many(self, filtro):
        await self._espera()
        return super().delete_many(filtro)

    async def find_one(self, filtro=None, *args, **kwargs):
        await self._espera()
        return super().find_one(filtro, *args, **kwargs)

    def find(self, filtro=None, proyeccion=None, *args, **kwargs):
        docs = [copy.deepcopy(d) for d in self.almacen.values()
                if coincide(d, filtro)]
        return CursorAsync(docs)

    async def count_documents(self, filtro):
        await self._espera()
        return super().count_documents(filtro)

    async def estimated_document_count(self):
        await self._espera()
        return super().estimated_document_count()

    async def aggregate(self, pipeline):
        # En PyMongo Async real, aggregate() es corutina (find() no).
        return CursorAsync(ejecutar_pipeline(
            list(self.almacen.values()), pipeline))

    async def create_index(self, *args, **kwargs):
        return "fake_index"


class BDFakeAsync(dict):
    def __getitem__(self, nombre):
        if nombre not in self:
            self[nombre] = ColeccionFakeAsync()
        return super().__getitem__(nombre)


class ClienteFakeAsync(dict):
    def __getitem__(self, nombre):
        if nombre not in self:
            self[nombre] = BDFakeAsync()
        return super().__getitem__(nombre)

    async def close(self):
        pass

    @property
    def admin(self):
        class _Admin:
            @staticmethod
            async def command(*args, **kwargs):
                return {"ok": 1}
        return _Admin()


# ---------------------------------------------------------------------------
# Pruebas
# ---------------------------------------------------------------------------
class TestGenerador(unittest.TestCase):
    def test_esquema_completo(self):
        import random
        doc = generar_pelicula(random.Random(1))
        for campo in ("title", "year", "genres", "directors", "cast",
                      "imdb", "num_mflix_comments", "type"):
            self.assertIn(campo, doc)
        self.assertIn("rating", doc["imdb"])
        self.assertGreaterEqual(doc["imdb"]["rating"], 1.0)
        self.assertLessEqual(doc["imdb"]["rating"], 10.0)

    def test_determinista(self):
        a = generar_peliculas(50, semilla=362839)
        b = generar_peliculas(50, semilla=362839)
        self.assertEqual([d["title"] for d in a], [d["title"] for d in b])

    def test_en_lotes(self):
        docs = list(range(10))
        lotes = list(en_lotes(docs, 3))
        self.assertEqual([len(l) for l in lotes], [3, 3, 3, 1])
        with self.assertRaises(ValueError):
            list(en_lotes(docs, 0))


class TestAppSync(unittest.TestCase):
    def setUp(self):
        from sync_app.app_sync import SyncMoviesDB
        self.db = SyncMoviesDB(client=ClienteFakeSync())
        self.db.work.insert_many(generar_peliculas(200))

    def test_carga_masiva_insert_many(self):
        stats = self.db.carga_masiva_insert_many(generar_peliculas(500, semilla=7),
                                                 tam_lote=100)
        self.assertEqual(stats["insertados"], 500)
        self.assertEqual(stats["lotes"], 5)
        self.assertEqual(self.db.contar(), 700)

    def test_carga_masiva_bulk_write(self):
        stats = self.db.carga_masiva_bulk_write(generar_peliculas(500, semilla=8),
                                                tam_lote=200)
        self.assertEqual(stats["insertados"], 500)
        self.assertEqual(self.db.contar(), 700)

    def test_crud(self):
        doc = generar_peliculas(1, semilla=99)[0]
        doc["title"] = "Prueba SYNC única"
        oid = self.db.crear_pelicula(doc)
        self.assertIsNotNone(oid)
        self.assertEqual(self.db.obtener_por_id(oid)["title"], "Prueba SYNC única")
        self.assertEqual(self.db.obtener_por_id(str(oid))["title"],
                         "Prueba SYNC única")
        self.assertEqual(self.db.obtener_por_titulo("Prueba SYNC única")["year"],
                         doc["year"])
        self.assertEqual(self.db.actualizar_pelicula(oid, {"imdb.rating": 9.9}), 1)
        self.assertEqual(self.db.obtener_por_id(oid)["imdb"]["rating"], 9.9)
        self.assertEqual(self.db.actualizar_muchos({"title": "Prueba SYNC única"},
                                                   {"rated": "G"}), 1)
        docs, total = self.db.listar({"title": "Prueba SYNC única"})
        self.assertEqual(total, 1)
        self.assertEqual(self.db.eliminar_pelicula(oid), 1)
        self.assertIsNone(self.db.obtener_por_id(oid))

    def test_bulk_update_delete(self):
        ids = [d["_id"] for d in list(self.db.work.find({}, {"_id": 1}))[:50]]
        mod = self.db.bulk_actualizar_ratings([(i, 8.8) for i in ids])
        self.assertEqual(mod, 50)
        self.assertEqual(self.db.obtener_por_id(ids[0])["imdb"]["rating"], 8.8)
        eliminados = self.db.bulk_eliminar([str(i) for i in ids[:10]])
        self.assertEqual(eliminados, 10)

    def test_consultas(self):
        # sample está vacío -> las consultas usan la colección de trabajo
        por_genero = self.db.consulta_por_genero("Drama", limite=5)
        self.assertLessEqual(len(por_genero), 5)
        for p in por_genero:
            self.assertIn("Drama", p["genres"])
        rango = self.db.consulta_por_rango_anios(2000, 2010, limite=50)
        for p in rango:
            self.assertTrue(2000 <= p["year"] <= 2010)
        top = self.db.consulta_top_rating(5, min_votos=0)
        ratings = [p["imdb"]["rating"] for p in top]
        self.assertEqual(ratings, sorted(ratings, reverse=True))
        encontrados = self.db.consulta_buscar_titulo("sombras", limite=50)
        for p in encontrados:
            self.assertIn("sombras", p["title"].lower())
        agg = self.db.consulta_agregacion_por_genero(5)
        self.assertTrue(all("total" in g and "rating_promedio" in g for g in agg))
        por_anio = self.db.consulta_promedio_por_anio(2000, 2020)
        self.assertTrue(all("total" in f for f in por_anio))
        directores = self.db.consulta_top_directores(3)
        self.assertLessEqual(len(directores), 3)


class TestAppAsync(unittest.TestCase):
    def setUp(self):
        from async_app.app_async import AsyncMoviesDB
        self.db = AsyncMoviesDB(client=ClienteFakeAsync(), concurrencia=4)

    def _run(self, coro):
        return asyncio.run(coro)

    def test_carga_masiva_taskgroup(self):
        stats = self._run(self.db.carga_masiva_taskgroup(
            generar_peliculas(500, semilla=11), tam_lote=100))
        self.assertEqual(stats["insertados"], 500)
        self.assertEqual(stats["lotes"], 5)
        self.assertEqual(self._run(self.db.contar()), 500)
        # El Semaphore limitó la concurrencia real.
        self.assertLessEqual(self.db.work.max_en_vuelo, 4)
        self.assertGreater(self.db.work.max_en_vuelo, 1)

    def test_carga_masiva_gather(self):
        stats = self._run(self.db.carga_masiva_gather(
            generar_peliculas(400, semilla=12), tam_lote=100))
        self.assertEqual(stats["insertados"], 400)
        self.assertEqual(self._run(self.db.contar()), 400)

    def test_crud_async(self):
        doc = generar_peliculas(1, semilla=101)[0]
        doc["title"] = "Prueba ASYNC única"
        oid = self._run(self.db.crear_pelicula(doc))
        self.assertEqual(self._run(self.db.obtener_por_id(oid))["title"],
                         "Prueba ASYNC única")
        self.assertEqual(self._run(self.db.obtener_por_id(str(oid)))["title"],
                         "Prueba ASYNC única")
        self.assertIsNotNone(self._run(self.db.obtener_por_titulo("Prueba ASYNC única")))
        self.assertEqual(self._run(self.db.actualizar_pelicula(
            oid, {"imdb.rating": 9.9})), 1)
        self.assertEqual(self._run(self.db.obtener_por_id(oid))["imdb"]["rating"], 9.9)
        docs, total = self._run(self.db.listar({"title": "Prueba ASYNC única"}))
        self.assertEqual(total, 1)
        self.assertEqual(self._run(self.db.eliminar_pelicula(oid)), 1)
        self.assertIsNone(self._run(self.db.obtener_por_id(oid)))

    def test_bulk_async(self):
        ids = self._run(self.db.crear_muchas(generar_peliculas(60, semilla=13)))
        mod = self._run(self.db.bulk_actualizar_ratings([(i, 8.8) for i in ids]))
        self.assertEqual(mod, 60)
        eliminados = self._run(self.db.bulk_eliminar([str(i) for i in ids[:10]]))
        self.assertEqual(eliminados, 10)
        self.assertEqual(self._run(self.db.contar()), 50)

    def test_consultas_concurrentes(self):
        self._run(self.db.crear_muchas(generar_peliculas(200, semilla=14)))
        resultados, segundos = self._run(
            self.db.consultas_concurrentes_gather(10))
        self.assertEqual(len(resultados), 10)
        self.assertGreaterEqual(segundos, 0)
        total, segundos = self._run(
            self.db.consultas_concurrentes_taskgroup(10))
        self.assertGreaterEqual(total, 0)

    def test_consultas_individuales_async(self):
        self._run(self.db.crear_muchas(generar_peliculas(100, semilla=15)))
        top = self._run(self.db.consulta_top_rating(5, min_votos=0))
        ratings = [p["imdb"]["rating"] for p in top]
        self.assertEqual(ratings, sorted(ratings, reverse=True))
        agg = self._run(self.db.consulta_agregacion_por_genero(5))
        self.assertTrue(all("total" in g for g in agg))
        por_anio = self._run(self.db.consulta_promedio_por_anio(2000, 2020))
        self.assertTrue(all("total" in f for f in por_anio))
        directores = self._run(self.db.consulta_top_directores(3))
        self.assertLessEqual(len(directores), 3)

    def test_fakes_replican_api_async_real(self):
        """Paridad: en PyMongo Async, aggregate() es corutina y find() no.

        Este test impide que los fakes vuelvan a enmascarar un uso
        incorrecto (bug real: `aggregate(...).to_list(...)` sin await).
        """
        import inspect

        from pymongo.asynchronous.collection import AsyncCollection

        self.assertTrue(inspect.iscoroutinefunction(AsyncCollection.aggregate))
        self.assertTrue(inspect.iscoroutinefunction(
            ColeccionFakeAsync.aggregate))
        self.assertFalse(inspect.iscoroutinefunction(AsyncCollection.find))


if __name__ == "__main__":
    unittest.main(verbosity=2)
