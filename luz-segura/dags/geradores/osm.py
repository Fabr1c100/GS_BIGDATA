"""Coleta de paradas de ônibus no OpenStreetMap (Overpass API) ao redor dos pontos de teste."""
import csv
import math
import os
from pathlib import Path

CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "LuzSegura-FIAP-GS/1.0 (projeto academico)"

CENTROS = [  # nome, latitude, longitude
    ("Casa", -23.506342276290244, -46.770098868597955),
    ("Trabalho", -23.547850923692437, -46.771811523254016),
    ("Faculdade", -23.542398389074698, -46.76675925251543),
]
RAIO_M = 2000
DEDUP_M = 15  # paradas a menos de 15 m uma da outra contam como a mesma


def montar_query():
    partes = []
    for _, lat, lon in CENTROS:
        a = f"(around:{RAIO_M},{lat},{lon})"
        partes.append(f'node["highway"="bus_stop"]{a};')
        partes.append(f'node["public_transport"="platform"]["bus"="yes"]{a};')
        partes.append(f'way["public_transport"="platform"]["bus"="yes"]{a};')
    return "[out:json][timeout:60];(" + "".join(partes) + ");out center tags;"


def distancia_m(lat1, lon1, lat2, lon2):
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def achatar(elementos):
    """Converte o JSON do Overpass em linhas planas, sem duplicados."""
    candidatos = []
    for e in elementos:
        lat = e.get("lat", (e.get("center") or {}).get("lat"))
        lon = e.get("lon", (e.get("center") or {}).get("lon"))
        if lat is None or lon is None:
            continue
        nome = (e.get("tags") or {}).get("name", "")
        candidatos.append({
            "id_local": f'OSM-{e["type"][0]}{e["id"]}',
            "nome": nome, "latitude": round(lat, 6), "longitude": round(lon, 6),
            "tipo_ponto": "ponto de ônibus", "origem": "osm"})
    # prefere paradas com nome; ordem estável
    candidatos.sort(key=lambda c: (c["nome"] == "", c["id_local"]))
    mantidos = []
    for c in candidatos:
        if any(m["id_local"] == c["id_local"] or
               distancia_m(m["latitude"], m["longitude"], c["latitude"], c["longitude"]) < DEDUP_M
               for m in mantidos):
            continue
        mantidos.append(c)
    return sorted(mantidos, key=lambda c: c["id_local"])


def coletar_pontos_osm(ds, **_):
    import requests
    r = requests.get(OVERPASS_URL, params={"data": montar_query()},
                     headers={"User-Agent": USER_AGENT}, timeout=90)
    r.raise_for_status()
    linhas = achatar(r.json().get("elements", []))
    if not linhas:
        raise ValueError("Overpass não retornou nenhuma parada")
    destino = CHEGADA / "pontos" / f"pontos_osm_{ds}.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(linhas[0].keys()))
        w.writeheader()
        w.writerows(linhas)
    return len(linhas)