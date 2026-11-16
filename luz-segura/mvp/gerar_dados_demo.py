"""Gera DADOS DE TESTE para o MVP a partir dos arquivos de chegada/ (sem Databricks).

Aplica as mesmas regras da pipeline (bairro mais próximo, qualidade, lux = ADC x 0,008 e
critério de ponto crítico), mas é só um atalho para testar o app localmente.
Os dados DEFINITIVOS do projeto devem vir das views da camada Ouro.

Uso (na pasta mvp):  python gerar_dados_demo.py
"""
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).parent.parent
CHEGADA, REFERENCIA, SAIDA = RAIZ / "chegada", RAIZ / "referencia", Path(__file__).parent / "dados"
FATOR_LUX = 0.008
LIMITE_ESCURO, LIMITE_FRACO = 1.5, 5.0


def ler_todos(pasta):
    arquivos = sorted((CHEGADA / pasta).glob("*.csv"))
    if not arquivos:
        raise SystemExit(f"Nenhum arquivo em chegada/{pasta}")
    return pd.concat([pd.read_csv(a, dtype=str).assign(arquivo=a.name) for a in arquivos], ignore_index=True)


def haversine_m(lat, lon, lats, lons):
    p1, p2 = np.radians(lat), np.radians(lats)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lons - lon) / 2) ** 2
    return 2 * 6371000 * np.arcsin(np.sqrt(a))


def main():
    # pontos (último arquivo) + bairro mais próximo
    ultimo = sorted((CHEGADA / "pontos").glob("pontos_osm_*.csv"))[-1]
    pontos = pd.read_csv(ultimo).drop_duplicates("id_local")
    ref = pd.read_csv(REFERENCIA / "bairros_referencia.csv")
    dist = np.column_stack([haversine_m(r.latitude, r.longitude, pontos["latitude"].values, pontos["longitude"].values)
                            for r in ref.itertuples()])
    pontos["bairro"] = ref["bairro"].values[dist.argmin(axis=1)]
    ids = set(pontos["id_local"])

    # sensor: qualidade e lux médio por ponto
    s = ler_todos("sensor")
    s["adc"] = pd.to_numeric(s["valor_adc"], errors="coerce")
    s = s[s["adc"].between(0, 4095) & s["id_local"].isin(ids)].drop_duplicates("id_leitura")
    lux = (s.groupby("id_local")["adc"].mean() * FATOR_LUX).rename("lux_medio")

    # relatos: qualidade (classe, ponto, duplicado em 15 min)
    r = ler_todos("relatos").drop_duplicates("id_relato")
    r = r[r["classe"].isin(["iluminado", "fraco", "escuro"]) & r["id_local"].isin(ids)].copy()
    r["t"] = pd.to_datetime(r["data_hora"])
    r = r.sort_values("t")
    r["delta"] = r.groupby([r["id_local"], r["relator_contato"].fillna(""), "classe"])["t"].diff().dt.total_seconds() / 60
    r = r[~(r["delta"] < 15)]
    tot = r.groupby("id_local").size().rename("relatos_total")
    esc = r[r["classe"] == "escuro"].groupby("id_local").size().rename("relatos_escuro")

    # chamados: foto mais recente de cada chamado
    c = ler_todos("chamados").sort_values("arquivo").drop_duplicates("id_chamado", keep="last")
    c = c[c["id_local"].isin(ids)].copy()
    c["abertura"] = pd.to_datetime(c["data_abertura"])
    c["chamado_status"] = np.where(c["data_conserto"].notna() & (c["data_conserto"] != ""), "resolvido", "pendente")
    c["dias_pendente"] = np.where(c["chamado_status"] == "pendente", (pd.Timestamp(date.today()) - c["abertura"]).dt.days, np.nan)
    c = c.set_index("id_local")[["chamado_status", "dias_pendente"]]

    p = pontos.set_index("id_local").join([lux, tot, esc, c])
    p["relatos_total"] = p["relatos_total"].fillna(0).astype(int)
    p["relatos_escuro"] = p["relatos_escuro"].fillna(0).astype(int)
    p["chamado_status"] = p["chamado_status"].fillna("sem chamado")
    p["situacao"] = np.where(p["lux_medio"] < LIMITE_ESCURO, "Escuro", np.where(p["lux_medio"] < LIMITE_FRACO, "Fraco", "Iluminado"))
    p["critico"] = ((p["lux_medio"] < LIMITE_ESCURO) & (p["relatos_escuro"] >= 1)).astype(int)
    p["lux_medio"] = p["lux_medio"].round(2)
    p = p.reset_index()
    cols = ["id_local", "nome", "bairro", "latitude", "longitude", "tipo_ponto", "lux_medio",
            "relatos_total", "relatos_escuro", "situacao", "critico", "chamado_status", "dias_pendente"]

    b = p.groupby("bairro").agg(pontos=("id_local", "count"), criticos=("critico", "sum"),
                                lux_medio_bairro=("lux_medio", "mean"), lux_pior_ponto=("lux_medio", "min")).reset_index()
    b["pct_criticos"] = (100 * b["criticos"] / b["pontos"]).round(0)
    ch = p[p["chamado_status"] != "sem chamado"].groupby("bairro").agg(
        chamados=("id_local", "count"), resolvidos=("chamado_status", lambda x: (x == "resolvido").sum()),
        maior_pendencia_dias=("dias_pendente", "max")).reset_index()
    b = b.merge(ch, on="bairro", how="left").fillna({"chamados": 0, "resolvidos": 0, "maior_pendencia_dias": 0})
    b["lux_medio_bairro"] = b["lux_medio_bairro"].round(1)
    b = b[["bairro", "pontos", "criticos", "pct_criticos", "lux_medio_bairro", "lux_pior_ponto",
           "chamados", "resolvidos", "maior_pendencia_dias"]]

    SAIDA.mkdir(exist_ok=True)
    p[cols].to_csv(SAIDA / "vw_painel_pontos.csv", index=False)
    b.to_csv(SAIDA / "vw_painel_bairros.csv", index=False)
    print(f"{len(p)} pontos e {len(b)} bairros gravados em {SAIDA}")


if __name__ == "__main__":
    main()