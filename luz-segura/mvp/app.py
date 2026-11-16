"""Luz Segura | MVP: pontos de ônibus com iluminação crítica perto do usuário.

Lê o snapshot das views da camada Ouro (mvp/dados/*.csv). A iluminação é SIMULADA;
as paradas vêm do OpenStreetMap. O formulário não coleta dado pessoal.
"""
import csv
import os
import random
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import streamlit as st

try:
    from streamlit_geolocation import streamlit_geolocation
except ImportError:  # a localização por GPS é opcional
    streamlit_geolocation = None

BASE = Path(__file__).parent
DADOS = BASE / "dados"
APP_DATA = Path(os.environ.get("GS_APP_DATA", BASE.parent / "app_data"))
RELATOS_APP = APP_DATA / "relatos_app.csv"
EXPORTADOS = APP_DATA / "relatos_exportados.txt"

LOCAIS = {  # pontos de teste pré-definidos
    "Casa": (-23.506342276290244, -46.770098868597955),
    "Trabalho": (-23.547850923692437, -46.771811523254016),
    "Faculdade": (-23.542398389074698, -46.76675925251543),
}
RAIO_MAX_KM = 2.0
CORES = {"Escuro": "#D32F2F", "Fraco": "#F9A825", "Iluminado": "#2E7D32"}
COR_VOCE = "#1565C0"
COLUNAS_PONTOS = ["id_local", "nome", "bairro", "latitude", "longitude", "tipo_ponto", "lux_medio",
                  "relatos_total", "relatos_escuro", "situacao", "critico", "chamado_status", "dias_pendente"]
COLUNAS_BAIRROS = ["bairro", "pontos", "criticos", "pct_criticos", "lux_medio_bairro",
                   "lux_pior_ponto", "chamados", "resolvidos", "maior_pendencia_dias"]


# ---------- utilidades ----------
def distancia_m(lat, lon, lats, lons):
    """Distância (Haversine) em metros entre um ponto e vários."""
    r = 6371000.0
    p1, p2 = np.radians(lat), np.radians(lats)
    dp, dl = p2 - p1, np.radians(lons - lon)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


@st.cache_data
def _carregar(caminho: str, mtime: float) -> pd.DataFrame:
    return pd.read_csv(caminho)


def ler_csv(nome: str, colunas: list) -> pd.DataFrame:
    caminho = DADOS / nome
    if not caminho.exists():
        st.error(f"Arquivo `dados/{nome}` não encontrado. Exporte a view da camada Ouro "
                 "(ou rode `python gerar_dados_demo.py` para dados de teste).")
        st.stop()
    df = _carregar(str(caminho), caminho.stat().st_mtime)
    faltando = [c for c in colunas if c not in df.columns]
    if faltando:
        st.error(f"`dados/{nome}` está sem as colunas: {', '.join(faltando)}")
        st.stop()
    return df


def rotulo(linha) -> str:
    nome = linha["nome"] if isinstance(linha["nome"], str) and linha["nome"].strip() else "Parada sem nome"
    return f"{nome} ({linha['bairro']})"


def texto_chamado(linha) -> str:
    if linha["chamado_status"] == "pendente":
        dias = linha["dias_pendente"]
        return f"Pendente há {int(dias)} dias" if pd.notna(dias) else "Pendente"
    if linha["chamado_status"] == "resolvido":
        return "Resolvido"
    return "Sem chamado"


def pontos_proximos(pontos, lat, lon, raio_km):
    df = pontos.copy()
    df["dist_m"] = distancia_m(lat, lon, df["latitude"].values, df["longitude"].values)
    return df[df["dist_m"] <= raio_km * 1000].sort_values("dist_m")


# ---------- formulário e Airflow ----------
def gravar_relato(id_local: str, classe: str) -> str:
    """Grava o relato SEM dados pessoais (nome e contato ficam vazios)."""
    APP_DATA.mkdir(parents=True, exist_ok=True)
    agora = datetime.now()
    id_relato = f"APP-{agora:%Y%m%d%H%M%S}-{random.randint(1000, 9999)}"
    novo = not RELATOS_APP.exists()
    with open(RELATOS_APP, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo:
            w.writerow(["id_relato", "id_local", "data_hora", "classe", "relator_nome", "relator_contato", "origem"])
        w.writerow([id_relato, id_local, agora.isoformat(timespec="seconds"), classe, "", "", "app"])
    return id_relato


def relatos_pendentes() -> int:
    if not RELATOS_APP.exists():
        return 0
    with open(RELATOS_APP, encoding="utf-8", newline="") as f:
        ids = {l["id_relato"] for l in csv.DictReader(f) if l.get("id_relato")}
    feitos = set(EXPORTADOS.read_text(encoding="utf-8").split()) if EXPORTADOS.exists() else set()
    return len(ids - feitos)


def disparar_airflow():
    """Dispara a DAG de exportação pela REST API do Airflow 3 (token JWT + dagRuns)."""
    url = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
    usuario, senha = os.environ.get("AIRFLOW_USER"), os.environ.get("AIRFLOW_PASSWORD")
    dag_id = os.environ.get("AIRFLOW_DAG_ID", "luz_segura_relatos_app")
    if not usuario or not senha:
        return False, "Defina as variáveis de ambiente AIRFLOW_USER e AIRFLOW_PASSWORD antes de abrir o app."
    try:
        t = requests.post(f"{url}/auth/token", json={"username": usuario, "password": senha}, timeout=15)
        token = t.json().get("access_token") if t.ok else None
        if not token:
            return False, f"Não foi possível autenticar no Airflow (HTTP {t.status_code})."
        r = requests.post(f"{url}/api/v2/dags/{dag_id}/dagRuns",
                          headers={"Authorization": f"Bearer {token}"}, json={"logical_date": None}, timeout=15)
        if r.status_code in (200, 201):
            return True, r.json().get("dag_run_id", "")
        return False, f"O Airflow recusou o disparo (HTTP {r.status_code}): {r.text[:200]}"
    except (requests.RequestException, ValueError) as e:
        return False, f"Não consegui falar com o Airflow em {url}: {e}"


# ---------- página ----------
st.set_page_config(page_title="Luz Segura", layout="wide")
st.title("Luz Segura")
st.caption("Pontos de ônibus de Osasco com iluminação crítica. "
           "As paradas vêm do OpenStreetMap; **a iluminação é simulada para demonstração**.")

pontos = ler_csv("vw_painel_pontos.csv", COLUNAS_PONTOS)
bairros = ler_csv("vw_painel_bairros.csv", COLUNAS_BAIRROS).fillna(0)
atualizado = datetime.fromtimestamp((DADOS / "vw_painel_pontos.csv").stat().st_mtime)
st.caption(f"Dados atualizados em {atualizado:%d/%m/%Y %H:%M}.")

aba1, aba2, aba3 = st.tabs(["Perto de você", "Panorama por bairro", "Relatar problema"])

# ----- aba 1: perto de você
with aba1:
    st.subheader("Quais pontos perto de você estão escuros?")
    c1, c2 = st.columns([2, 1])
    origem = c1.selectbox("Onde você está?", list(LOCAIS), key="origem_busca")
    raio_km = c2.slider("Raio de busca (km)", 0.5, RAIO_MAX_KM, RAIO_MAX_KM, 0.5)
    lat0, lon0 = LOCAIS[origem]
    nome_origem = origem

    if streamlit_geolocation is not None:
        with st.expander("Usar minha localização (opcional)"):
            st.caption("Pede permissão ao navegador. Sua posição é usada só nesta tela e não é gravada. "
                       "Se negar ou falhar, escolha um dos locais acima.")
            loc = streamlit_geolocation()
            if isinstance(loc, dict) and loc.get("latitude") is not None:
                lat0, lon0, nome_origem = loc["latitude"], loc["longitude"], "Sua localização"
                st.success("Usando sua localização atual.")

    perto = pontos_proximos(pontos, lat0, lon0, raio_km)
    if perto.empty:
        st.warning("Não há pontos cadastrados neste raio. A base cobre as áreas de Casa, Trabalho e Faculdade.")
    else:
        criticos = perto[perto["critico"] == 1]
        m1, m2, m3 = st.columns(3)
        m1.metric("Pontos no raio", len(perto))
        m2.metric("Pontos críticos", f"{len(criticos)} ({100 * len(criticos) / len(perto):.0f}%)")
        if criticos.empty:
            m3.metric("Crítico mais próximo", "nenhum")
        else:
            c = criticos.iloc[0]
            m3.metric("Crítico mais próximo", f"{c['dist_m']:.0f} m", help=rotulo(c))

        ver = st.radio("Mostrar no mapa", ["Todos os pontos", "Só os críticos"], horizontal=True)
        base_mapa = criticos if ver == "Só os críticos" else perto
        mapa = pd.DataFrame({
            "latitude": base_mapa["latitude"], "longitude": base_mapa["longitude"],
            "cor": base_mapa["situacao"].map(CORES).fillna("#757575"), "tamanho": 25})
        mapa = pd.concat([mapa, pd.DataFrame([{"latitude": lat0, "longitude": lon0,
                                               "cor": COR_VOCE, "tamanho": 60}])], ignore_index=True)
        st.map(mapa, latitude="latitude", longitude="longitude", color="cor", size="tamanho")
        st.markdown("🔵 **Você** (" + nome_origem + ") · 🔴 **Escuro** · 🟡 **Fraco** · 🟢 **Iluminado**")

        st.markdown("#### Pontos críticos mais próximos")
        if criticos.empty:
            st.info("Nenhum ponto crítico neste raio.")
        else:
            tabela = criticos.head(10).copy()
            tabela["Parada"] = tabela.apply(rotulo, axis=1)
            tabela["Distância (m)"] = tabela["dist_m"].round(0).astype(int)
            tabela["Lux médio"] = tabela["lux_medio"].round(1)
            tabela["Relatos 'escuro'"] = tabela["relatos_escuro"].astype(int)
            tabela["Chamado"] = tabela.apply(texto_chamado, axis=1)
            tabela["Situação"] = tabela["situacao"]
            st.dataframe(tabela[["Parada", "Distância (m)", "Lux médio", "Situação", "Relatos 'escuro'", "Chamado"]],
                         hide_index=True)

# ----- aba 2: panorama por bairro
with aba2:
    st.subheader("Quais bairros precisam de atenção primeiro?")
    ordem = bairros.sort_values(["pct_criticos", "maior_pendencia_dias"], ascending=False)
    pior = ordem.iloc[0]
    st.markdown(f"**Bairro prioritário: {pior['bairro']}**")
    a, b, c = st.columns(3)
    a.metric("Pontos críticos", f"{int(pior['criticos'])} de {int(pior['pontos'])}")
    b.metric("Pior ponto (lux)", f"{pior['lux_pior_ponto']:.2f}")
    c.metric("Maior pendência (dias)", int(pior["maior_pendencia_dias"]))
    st.bar_chart(ordem.set_index("bairro")["pct_criticos"], y_label="% de pontos críticos")
    st.dataframe(ordem.rename(columns={
        "bairro": "Bairro", "pontos": "Pontos", "criticos": "Críticos", "pct_criticos": "% críticos",
        "lux_medio_bairro": "Lux médio", "lux_pior_ponto": "Pior ponto (lux)", "chamados": "Chamados",
        "resolvidos": "Resolvidos", "maior_pendencia_dias": "Maior pendência (dias)"}), hide_index=True)

# ----- aba 3: relatar problema
with aba3:
    st.subheader("Relatar a iluminação de um ponto")
    st.info("Não pedimos nome, contato nem sua localização. Gravamos apenas o ponto escolhido, "
            "a classificação e a data e hora do relato.")
    origem_f = st.selectbox("Perto de qual local?", list(LOCAIS), key="origem_form")
    candidatos = pontos_proximos(pontos, *LOCAIS[origem_f], RAIO_MAX_KM).head(40)
    opcoes = {f"{rotulo(l)} | a {l['dist_m']:.0f} m": l["id_local"] for _, l in candidatos.iterrows()}
    with st.form("form_relato", clear_on_submit=True):
        escolha = st.selectbox("Ponto de ônibus", list(opcoes))
        classe = st.radio("Como está a iluminação?", ["iluminado", "fraco", "escuro"], horizontal=True)
        enviar = st.form_submit_button("Enviar relato")
    if enviar and escolha:
        st.success(f"Relato registrado: {gravar_relato(opcoes[escolha], classe)}")

    st.divider()
    st.markdown("#### Enviar relatos ao pipeline (teste local)")
    n = relatos_pendentes()
    st.write(f"Relatos aguardando exportação: **{n}**")
    if st.button("Processar relatos agora", disabled=(n == 0)):
        ok, info = disparar_airflow()
        if ok:
            st.success(f"DAG disparada no Airflow ({info}). Os arquivos aparecem em `chegada/relatos/`.")
        else:
            st.error(info)