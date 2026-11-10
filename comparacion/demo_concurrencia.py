"""
Demo de concurrencia SIN base de datos (simulación de latencia I/O).

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839

¿Para qué sirve?
  - Muestra el beneficio de gather/TaskGroup/Semaphore cuando cada
    operación pasa tiempo ESPERANDO (como una consulta a MongoDB).
  - Corre en cualquier máquina sin MongoDB: cada "consulta" es un
    asyncio.sleep(latencia) que simula el viaje de ida y vuelta al servidor.
  - NO sustituye al benchmark real (benchmark.py); es material didáctico
    y permite verificar la lógica concurrente sin servidor.

Uso:
    python -m comparacion.demo_concurrencia --consultas 50 --latencia 0.05
"""

import argparse
import asyncio
import time


def _lat(i):
    """Latencia determinista por consulta (50-100 ms aprox, escalada)."""
    return 0.5 + (i % 5) * 0.125  # múltiplo de la latencia base


async def consulta_simulada(i, latencia_base, semaforo):
    async with semaforo:
        await asyncio.sleep(latencia_base * _lat(i))
        return f"doc-{i}"


async def experimento_gather(n, latencia_base, concurrencia):
    sem = asyncio.Semaphore(concurrencia)
    inicio = time.perf_counter()
    res = await asyncio.gather(
        *(consulta_simulada(i, latencia_base, sem) for i in range(n)))
    return res, time.perf_counter() - inicio


async def experimento_taskgroup(n, latencia_base, concurrencia):
    sem = asyncio.Semaphore(concurrencia)
    resultados = [None] * n

    async def _una(i):
        resultados[i] = await consulta_simulada(i, latencia_base, sem)

    inicio = time.perf_counter()
    if hasattr(asyncio, "TaskGroup"):
        async with asyncio.TaskGroup() as grupo:
            for i in range(n):
                grupo.create_task(_una(i))
    else:
        await asyncio.gather(*(_una(i) for i in range(n)))
    return resultados, time.perf_counter() - inicio


def experimento_sync(n, latencia_base):
    inicio = time.perf_counter()
    res = []
    for i in range(n):
        time.sleep(latencia_base * _lat(i))
        res.append(f"doc-{i}")
    return res, time.perf_counter() - inicio


def main():
    parser = argparse.ArgumentParser(
        description="Demo simulada sync vs gather vs TaskGroup (sin BD)")
    parser.add_argument("--consultas", type=int, default=50)
    parser.add_argument("--latencia", type=float, default=0.05,
                        help="Latencia base simulada por consulta (s)")
    parser.add_argument("--concurrencia", type=int, default=10)
    args = parser.parse_args()

    print("=" * 64)
    print("DEMO SIMULADA (sin MongoDB): latencia I/O por consulta = "
          f"{args.latencia * 1000:.0f} ms base, conc={args.concurrencia}")
    print("=" * 64)

    _, t_sync = experimento_sync(args.consultas, args.latencia)
    print(f"Síncrono secuencial : {t_sync:.2f} s")
    _, t_gather = asyncio.run(experimento_gather(
        args.consultas, args.latencia, args.concurrencia))
    print(f"gather + Semaphore  : {t_gather:.2f} s  (speedup x{t_sync / t_gather:.2f})")
    _, t_tg = asyncio.run(experimento_taskgroup(
        args.consultas, args.latencia, args.concurrencia))
    print(f"TaskGroup+Semaphore : {t_tg:.2f} s  (speedup x{t_sync / t_tg:.2f})")
    print("\nConclusión: con I/O-bound, traslapar esperas reduce el tiempo "
          "total casi en proporción a la concurrencia.")


if __name__ == "__main__":
    main()
