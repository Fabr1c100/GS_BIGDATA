import csv, random
from datetime import datetime, timedelta
from pathlib import Path

CHEGADA = Path("/opt/airflow/chegada")
ADC_NOITE = {"P01": 1800, "P02": 900, "P03": 120, "P04": 300, "P05": 60, "P06": 1500}
CLASSE = {"P01": "iluminado", "P02": "fraco", "P03": "escuro",
          "P04": "fraco", "P05": "escuro", "P06": "iluminado"}

def _salvar(pasta, nome, cabecalho, linhas):
    destino = CHEGADA / pasta / nome
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)

def gerar_sensor(ds, **_):
    rng = random.Random(f"sensor-{ds}")          # semente fixa: reprodutível
    linhas = []
    for local, base in ADC_NOITE.items():
        for h in range(18, 24):
            adc = max(0, int(rng.gauss(base, base * 0.1)))
            linhas.append([f"{local}-{ds}-{h:02d}", "SIM01", local,
                           f"{ds}T{h:02d}:00:00", adc, "simulado"])
    linhas[3][4] = ""      # defeito injetado: leitura nula
    linhas[9][4] = 9999    # defeito injetado: fora da faixa 0-4095
    _salvar("sensor", f"sensor_{ds}.csv",
            ["id_leitura", "id_dispositivo", "id_local", "data_hora", "valor_adc", "origem"], linhas)

def gerar_relatos(ds, **_):
    rng = random.Random(f"relatos-{ds}")
    linhas = []
    for n in range(1, 7):
        local = rng.choice(list(ADC_NOITE))
        t = datetime.fromisoformat(f"{ds}T{rng.randint(18, 22):02d}:{rng.randint(0, 59):02d}:00")
        k = rng.randint(1, 5)
        linhas.append([f"R{ds}-{n:03d}", local, t.isoformat(), CLASSE[local],
                       f"Pessoa Teste {k}", f"1190000{k:04d}", "simulado"])
    # defeitos injetados: duplicado, classe inválida, local inexistente
    d = list(linhas[0]); d[0] = f"R{ds}-007"
    d[2] = (datetime.fromisoformat(d[2]) + timedelta(minutes=5)).isoformat()
    linhas.append(d)
    linhas.append([f"R{ds}-008", "P05", f"{ds}T22:20:00", "muito escuro", "Pessoa Teste 9", "11900000009", "simulado"])
    linhas.append([f"R{ds}-009", "P99", f"{ds}T20:00:00", "escuro", "Pessoa Teste 8", "11900000008", "simulado"])
    _salvar("relatos", f"relatos_{ds}.csv",
            ["id_relato", "id_local", "data_hora", "classe", "relator_nome", "relator_contato", "origem"], linhas)

CHAMADOS = [  # id, local, abertura, conserto
    ("C01", "P01", "2026-11-05", "2026-11-09"),
    ("C02", "P04", "2026-11-08", "2026-11-10"),
    ("C03", "P03", "2026-11-08", ""),
    ("C04", "P05", "2026-11-03", ""),
]

def gerar_chamados(ds, **_):
    linhas = []
    for cid, local, abertura, conserto in CHAMADOS:
        if abertura <= ds:                          # só existe depois de aberto
            linhas.append([cid, local, abertura, conserto if conserto and conserto <= ds else "", "simulado"])
    _salvar("chamados", f"chamados_{ds}.csv",
            ["id_chamado", "id_local", "data_abertura", "data_conserto", "origem"], linhas)