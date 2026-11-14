import pandas as pd
import streamlit as st

st.set_page_config(page_title="Luz Segura", layout="wide")
st.title("Luz Segura: iluminação pública por bairro")
st.caption("Dados simulados para demonstração, exceto o clima (Open-Meteo).")

bairros = pd.read_csv("dados/vw_painel_bairros.csv").fillna(0)
pontos = pd.read_csv("dados/vw_painel_pontos.csv")

st.header("Quais bairros precisam de atenção primeiro?")
ordem = bairros.sort_values(["pct_criticos", "maior_pendencia_dias"], ascending=False)
pior = ordem.iloc[0]
st.subheader(f"Bairro prioritário: {pior['bairro']}")
c1, c2, c3 = st.columns(3)
c1.metric("Pontos críticos", f"{int(pior['criticos'])} de {int(pior['pontos'])}")
c2.metric("Pior ponto (lux)", pior["lux_pior_ponto"])
c3.metric("Maior pendência (dias)", int(pior["maior_pendencia_dias"]))
st.dataframe(ordem, use_container_width=True, hide_index=True)

st.header("Pontos do bairro")
escolha = st.selectbox("Escolha o bairro", ordem["bairro"])
sel = pontos[pontos["bairro"] == escolha].sort_values("lux_medio").copy()
sel["situação"] = sel["lux_medio"].apply(
    lambda x: "Escuro" if x < 1.5 else ("Fraco" if x < 5 else "Iluminado"))
st.dataframe(sel[["endereco", "tipo_ponto", "lux_medio", "relatos_escuro", "situação"]],
             use_container_width=True, hide_index=True)