# Proyecto 2a — Base de datos asíncrona con Python y MongoDB

**Universidad Autónoma de Chihuahua — 8CC2 Cómputo Paralelo y Distribuido**
Alumno: **Adolfo Escobar Pereyra** · Matrícula: **362839**
Profesor: **José Saúl de Lira Miramontes**

Dos aplicaciones que administran la base de datos `sample_mflix (movies)`:
una **síncrona** (PyMongo clásico, `MongoClient`) y una **asíncrona**
(PyMongo Async, `AsyncMongoClient`), con las **mismas funcionalidades**,
más un **benchmark comparativo**.

| Funcionalidad | Síncrona | Asíncrona |
|---|---|---|
| Carga masiva de datos | `insert_many` / `bulk_write` por lotes | `TaskGroup` + `Semaphore` + `bulk_write` (y variante con `gather`) |
| CRUD completo | Sí | Sí (`async`/`await`) |
| Consultas (filtros, top-N, texto, agregaciones, paginación) | Sí, secuenciales | Sí, individuales + concurrentes (`gather` y `TaskGroup`) |
| Actualización/borrado masivo | `bulk_write` (`UpdateOne`, `DeleteOne`) | `bulk_write` concurrente por lotes |

## Estructura

```
Proyecto2a_MongoDB_362839/
├── config.py                  # URI, BD/colecciones, clientes, índices
├── sync_app/app_sync.py       # App síncrona (PyMongo) + menú + demo
├── async_app/app_async.py     # App asíncrona (PyMongo Async) + menú + demo
├── dataset/generador_peliculas.py  # Generador determinista estilo mflix
├── dataset/movies_sample.json # Muestra de 30 docs de ejemplo
├── comparacion/benchmark.py   # Benchmark sync vs async (requiere MongoDB)
├── comparacion/demo_concurrencia.py  # Demo simulada SIN BD
├── scripts/verificar_conexion.py     # Prueba la conexión
├── scripts/cargar_sample_mflix.py    # Importa un movies.json descargado
├── tests/test_sin_bd.py       # 14 pruebas sin servidor (fakes en memoria)
├── reporte/                   # Reporte del proyecto (.md y .pdf)
├── requirements.txt  .env.example  LEEME.txt  README.md
```

**Diseño de colecciones (no se daña el sample original):**
`sample_mflix.movies` solo se usa para **lectura** si existe;
toda escritura y benchmark va a `proyecto2a.movies_carga`.
Si no tienes el sample oficial, el generador incluido llena la
colección de trabajo con documentos compatibles (mismo esquema).

## Requisitos

- Python 3.11+ (por `asyncio.TaskGroup`) — probado con 3.14
- MongoDB local o Atlas. Opciones:
  - Local: `mongod`, o `docker run -d -p 27017:27017 mongo`
  - Atlas gratis: crea un clúster, opcionalmente **Load Sample Dataset**
    para tener `sample_mflix`, y apunta `MONGO_URI` a tu clúster.

## Instalación

```bash
cd Proyecto2a_MongoDB_362839
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # y edita MONGO_URI si usas Atlas
python -m scripts.verificar_conexion
```

## Uso

```bash
source .venv/bin/activate

# Menús interactivos
python -m sync_app.app_sync
python -m async_app.app_async

# Demos automáticas (carga + CRUD + consultas)
python -m sync_app.app_sync --demo --n 2000
python -m async_app.app_async --demo --n 2000 --concurrencia 10

# Benchmark comparativo (guarda comparacion/resultados_benchmark.json)
python -m comparacion.benchmark --n 5000 --lote 1000 --consultas 50

# Pruebas sin BD (14 tests, ~0.2 s)
python -m unittest discover -s tests -v

# Demo simulada sin BD (concepto de concurrencia I/O-bound)
python -m comparacion.demo_concurrencia --consultas 40 --concurrencia 10
```

## Comparación (cómo leer los resultados)

El benchmark corre **la misma carga de trabajo** en ambas apps y reporta
el mejor tiempo de 3 repeticiones por experimento:

- **A. Carga masiva** — inserción por lotes (`bulk_write` secuencial vs concurrente).
- **B. Consultas mezcladas** — Q consultas (una tras otra vs `gather` + `Semaphore`).
- **C. Update masivo** — N `UpdateOne` vía `bulk_write` (secuencial vs concurrente).

Resultado esperado (trabajo I/O-bound): la app asíncrona gana claramente
en B y C, y también en A cuando el servidor/latencia permite traslapar
lotes; el `speedup` crece con la latencia y se topa en la concurrencia
configurada. Ver el [reporte](reporte/Reporte_Proyecto2a_MongoAsync_AdolfoEscobar_362839.md)
para el análisis completo.

## Evidencia para la tarea

- Código Python: este repositorio.
- Reporte: `reporte/Reporte_Proyecto2a_MongoAsync_AdolfoEscobar_362839.pdf`
  (fuente Markdown + script `reporte/generar_reporte_pdf.py`).
