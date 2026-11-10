"""
Generador de películas con esquema compatible con sample_mflix.movies.

Universidad Autónoma de Chihuahua
8CC2 - Cómputo Paralelo y Distribuido
Alumno: Adolfo Escobar Pereyra | Matrícula: 362839

¿Por qué un generador?
  - El dataset oficial sample_mflix no siempre está disponible (requiere
    Atlas con "Sample Dataset" o una descarga de ~250 MB).
  - Con este generador, la carga masiva, el CRUD y los benchmarks funcionan
    en cualquier MongoDB vacío (local o Atlas) con documentos realistas.
  - Es determinista: con la misma semilla genera los mismos documentos.

Uso:
    from dataset.generador_peliculas import generar_peliculas
    docs = generar_peliculas(n=5000, semilla=362839)
    coleccion.insert_many(docs)

Esquema generado (subconjunto fiel de sample_mflix.movies):
    title, year, runtime, rated, plot, fullplot, genres, directors,
    writers, cast, languages, countries, released, poster,
    imdb {rating, votes, id}, tomatoes {...}, num_mflix_comments, type
"""

import datetime as _dt
import random as _random

TITULOS_A = [
    "Sombras", "Ecos", "Crónicas", "Horizonte", "Memorias", "Leyendas",
    "Secretos", "Senderos", "Reflejos", "Destinos", "Voces", "Tiempos",
    "Fronteras", "Aurora", "Crepúsculo", "Laberinto", "Marea", "Cenizas",
]
TITULOS_B = [
    "del desierto", "de medianoche", "perdidos", "del mañana", "de cristal",
    "olvidados", "del norte", "infinitos", "de papel", "del silencio",
    "imposibles", "del valle", "de neón", "ancestrales", "del puerto",
]
GENEROS = [
    "Drama", "Comedia", "Acción", "Thriller", "Romance", "Sci-Fi",
    "Documental", "Terror", "Aventura", "Crimen", "Fantasía", "Misterio",
]
NOMBRES = [
    "María Fernández", "Juan García", "Ana López", "Carlos Ruiz",
    "Sofía Hernández", "Diego Morales", "Lucía Torres", "Miguel Vargas",
    "Elena Castro", "Jorge Ramos", "Carmen Ortega", "Raúl Mendoza",
    "Paula Jiménez", "Andrés Aguilar", "Valeria Ríos", "Hugo Delgado",
]
DIRECTORES = [
    "Alejandro González", "Lucrecia Martel", "Guillermo del Toro",
    "Icíar Bollaín", "Pablo Larraín", "Carla Simón",
    "Alfonso Cuarón", "María Novaro", "Damián Szifron", "Jonás Trueba",
]
IDIOMAS = ["Español", "Inglés", "Francés", "Portugués", "Alemán", "Italiano"]
PAISES = ["México", "España", "Argentina", "Chile", "Colombia", "EE. UU."]
RATED = ["G", "PG", "PG-13", "R", "UNRATED", "PASSED"]
PLOTS = [
    "Una historia de superación en tiempos difíciles.",
    "Dos desconocidos se encuentran y cambian sus vidas para siempre.",
    "Un misterio que desafía todo lo que creían saber.",
    "Aventura épica a través de tierras desconocidas.",
    "Retrato íntimo de una familia a lo largo de tres generaciones.",
    "Un viaje inesperado que revela secretos del pasado.",
]


def generar_pelicula(rng):
    """Genera un documento película. rng: instancia de random.Random."""
    titulo = f"{rng.choice(TITULOS_A)} {rng.choice(TITULOS_B)} {rng.randint(1, 999)}"
    anio = rng.randint(1950, 2024)
    rating = round(rng.uniform(1.0, 10.0), 1)
    votos = rng.randint(0, 500000)
    n_generos = rng.randint(1, 3)
    return {
        "title": titulo,
        "year": anio,
        "runtime": rng.randint(70, 210),
        "rated": rng.choice(RATED),
        "plot": rng.choice(PLOTS),
        "fullplot": rng.choice(PLOTS) + " " + rng.choice(PLOTS),
        "genres": rng.sample(GENEROS, n_generos),
        "directors": rng.sample(DIRECTORES, rng.randint(1, 2)),
        "writers": rng.sample(NOMBRES, rng.randint(1, 3)),
        "cast": rng.sample(NOMBRES, rng.randint(2, 5)),
        "languages": rng.sample(IDIOMAS, rng.randint(1, 2)),
        "countries": rng.sample(PAISES, rng.randint(1, 2)),
        "released": _dt.datetime(anio, rng.randint(1, 12), rng.randint(1, 28)),
        "poster": f"https://example.com/posters/{abs(hash(titulo)) % 100000}.jpg",
        "imdb": {
            "rating": rating,
            "votes": votos,
            "id": rng.randint(1, 9999999),
        },
        "tomatoes": {
            "viewer": {
                "rating": round(rng.uniform(1.0, 5.0), 1),
                "numReviews": rng.randint(0, 100000),
            },
            "lastUpdated": _dt.datetime(anio, 1, 1),
        },
        "num_mflix_comments": rng.randint(0, 300),
        "type": "movie",
    }


def generar_peliculas(n, semilla=362839):
    """Genera n documentos película de forma determinista."""
    rng = _random.Random(semilla)
    return [generar_pelicula(rng) for _ in range(n)]


def en_lotes(docs, tam_lote):
    """Parte una lista de documentos en lotes (generador).

    Se usa tanto en la app síncrona como en la asíncrona para dividir
    la carga masiva en bloques manejables.
    """
    if tam_lote <= 0:
        raise ValueError("tam_lote debe ser mayor que 0")
    for i in range(0, len(docs), tam_lote):
        yield docs[i:i + tam_lote]


if __name__ == "__main__":
    import json
    import os
    import sys

    n = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    docs = generar_peliculas(n)
    salida = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "movies_sample.json")
    with open(salida, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=1, default=str)
    print(f"Se generaron {n} películas en {salida}")
