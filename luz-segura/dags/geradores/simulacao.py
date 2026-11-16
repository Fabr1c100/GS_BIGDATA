"""Dados SIMULADOS (sensor, relatos e chamados) para as paradas coletadas do OpenStreetMap.

O perfil de iluminação de cada ponto é sorteado UMA vez, com semente baseada no id do ponto,
para que a criticidade seja estável entre os dias e reproduzível.
"""
import csv
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
PERFIS_ADC = {"escuro": 100, "fraco": 400, "iluminado": 1700}  # base do valor bruto do sensor
CLASSES = ["iluminado", "fraco", "escuro"]
RUIDO_RELATO = 0.15          # 15% dos relatos não batem com o perfil do ponto
RELATOS_POR_DIA = 30
BASE_CHAMADOS = date(2026, 11, 3)


def _salvar(pasta, nome, cabecalho, linhas):
    destino = CHEGADA / pasta / nome
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)


def ler_pontos(ds):
    arq = CHEGADA / "pontos" / f"pontos_osm_{ds}.csv"
    with open(arq, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def perfil_do_ponto(id_local):
    x = random.Random(f"perfil-{id_local}").random()
    return "escuro" if x < 0.20 else ("fraco" if x < 0.50 else "iluminado")


def gerar_sensor(ds, **_):
    pontos = ler_pontos(ds)
    rng = random.Random(f"sensor-{ds}")
    linhas = []
    for p in pontos:
        base = PERFIS_ADC[perfil_do_ponto(p["id_local"])]
        for h in range(18, 24):
            adc = max(0, int(rng.gauss(base, base * 0.1)))
            linhas.append([f'{p["id_local"]}-{ds}-{h:02d}', "SIM01", p["id_local"],
                           f"{ds}T{h:02d}:00:00", adc, "simulado"])
    if len(linhas) > 9:          # defeitos injetados de propósito
        linhas[3][4] = ""        # leitura nula
        linhas[9][4] = 9999      # fora da faixa 0-4095
    _salvar("sensor", f"sensor_{ds}.csv",
            ["id_leitura", "id_dispositivo", "id_local", "data_hora", "valor_adc", "origem"], linhas)


def gerar_relatos(ds, **_):
    pontos = ler_pontos(ds)
    rng = random.Random(f"relatos-{ds}")
    linhas = []
    for n, p in enumerate(rng.sample(pontos, min(RELATOS_POR_DIA, len(pontos))), 1):
        classe = perfil_do_ponto(p["id_local"])
        if rng.random() < RUIDO_RELATO:
            classe = rng.choice(CLASSES)
        t = datetime.fromisoformat(f"{ds}T{rng.randint(18, 22):02d}:{rng.randint(0, 59):02d}:00")
        k = rng.randint(1, 20)
        linhas.append([f"R{ds}-{n:03d}", p["id_local"], t.isoformat(), classe,
                       f"Pessoa Teste {k}", f"1190000{k:04d}", "simulado"])
    # defeitos injetados de propósito: duplicado, classe inválida, ponto inexistente
    d = list(linhas[0])
    d[0] = f"R{ds}-{len(linhas) + 1:03d}"
    d[2] = (datetime.fromisoformat(d[2]) + timedelta(minutes=5)).isoformat()
    linhas.append(d)
    linhas.append([f"R{ds}-{len(linhas) + 1:03d}", pontos[0]["id_local"], f"{ds}T22:20:00",
                   "muito escuro", "Pessoa Teste 99", "11900009999", "simulado"])
    linhas.append([f"R{ds}-{len(linhas) + 1:03d}", "OSM-n0", f"{ds}T20:00:00",
                   "escuro", "Pessoa Teste 98", "11900009998", "simulado"])
    _salvar("relatos", f"relatos_{ds}.csv",
            ["id_relato", "id_local", "data_hora", "classe", "relator_nome", "relator_contato", "origem"], linhas)


def gerar_chamados(ds, **_):
    pontos = ler_pontos(ds)
    linhas = []
    for p in pontos:
        if perfil_do_ponto(p["id_local"]) != "escuro":
            continue
        rng = random.Random(f"chamado-{p['id_local']}")
        if rng.random() > 0.7:   # nem todo ponto escuro tem chamado aberto
            continue
        abertura = BASE_CHAMADOS + timedelta(days=rng.randint(0, 10))
        conserto = abertura + timedelta(days=rng.randint(2, 9)) if rng.random() < 0.4 else None
        a = abertura.isoformat()
        c = conserto.isoformat() if conserto else ""
        if a <= ds:              # o chamado só existe depois de aberto
            linhas.append([f'C-{p["id_local"]}', p["id_local"], a, c if c and c <= ds else "", "simulado"])
    _salvar("chamados", f"chamados_{ds}.csv",
            ["id_chamado", "id_local", "data_abertura", "data_conserto", "origem"], linhas)