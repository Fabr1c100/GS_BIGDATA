"""Exporta para chegada/relatos os relatos novos enviados pelo formulário do MVP."""
import csv
import logging
import os
from pathlib import Path

from datetime import datetime

CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
APP = Path(os.environ.get("GS_APP_DATA", "/opt/airflow/app_data"))
COLUNAS = ["id_relato", "id_local", "data_hora", "classe", "relator_nome", "relator_contato", "origem"]
log = logging.getLogger(__name__)


def exportar_relatos_app(**_):
    fonte = APP / "relatos_app.csv"
    if not fonte.exists():
        log.warning("Nenhum relato do app encontrado em %s", fonte)
        return 0
    controle = APP / "relatos_exportados.txt"
    feitos = set(controle.read_text(encoding="utf-8").split()) if controle.exists() else set()
    with open(fonte, encoding="utf-8", newline="") as f:
        novos = [l for l in csv.DictReader(f) if l.get("id_relato") and l["id_relato"] not in feitos]
    if not novos:
        log.info("Nenhum relato novo para exportar")
        return 0
    carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = CHEGADA / "relatos" / f"relatos_app_{carimbo}.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS)
        w.writeheader()
        for l in novos:
            w.writerow({c: l.get(c, "") for c in COLUNAS})
    with open(controle, "a", encoding="utf-8") as f:
        f.write("\n".join(l["id_relato"] for l in novos) + "\n")
    log.info("%d relatos exportados para %s", len(novos), destino.name)
    return len(novos)