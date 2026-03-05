import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests
import io
import re

# ─────────────────────────────────────────────
# CONFIGURACIÓN — pega aquí tu link de OneDrive
# ─────────────────────────────────────────────
ONEDRIVE_SHARE_URL = "https://1drv.ms/u/c/0da2ba1c949328fa/IQDMIYZ2FvFKRK9wfOwiXBHJAWuFfp5HKSdBt3g3l_eaRKk?e=irbnIK"
# Ejemplo: "https://1drv.ms/u/s!Abc123..."

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def onedrive_direct_url(share_url: str) -> str:
    """Convierte un link compartido de OneDrive en URL de descarga directa."""
    import base64
    b64 = base64.b64encode(share_url.encode()).decode()
    b64 = b64.rstrip("=").replace("/", "_").replace("+", "-")
    return f"https://api.onedrive.com/v1.0/shares/u!{b64}/root/content"


@st.cache_data(show_spinner="Cargando datos desde OneDrive…", ttl=3600)
def load_data(url: str) -> pd.DataFrame:
    direct = onedrive_direct_url(url)
    resp = requests.get(direct, timeout=120)
    resp.raise_for_status()
    df = pd.read_parquet(io.BytesIO(resp.content))
    # normalizar nombres
    df.columns = [c.strip() for c in df.columns]
    return df


def periodo_label(ym: int) -> str:
    """202304 → 'Abr 2023'"""
    meses = ["Ene","Feb","Mar","Abr","May","Jun",
             "Jul","Ago","Sep","Oct","Nov","Dic"]
    y, m = divmod(ym, 100)
    return f"{meses[m-1]} {y}"


def semestre(mes: int) -> str:
    return "Oct–Mar" if mes in [10,11,12,1,2,3] else "Abr–Sep"


TIPO_COLORS = {
    "Lunes a Viernes No Feriado": "#1f77b4",
    "Lunes a Viernes (No Feriado)": "#1f77b4",
    "Sábado": "#ff7f0e",
    "Domingo": "#2ca02c",
    "Feriado": "#d62728",
}

# ─────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────

st.set_page_config(page_title="Dashboard Retiros", layout="wide", page_icon="⚡")

# CSS personalizado
st.markdown("""
<style>
    .main { background-color: #f4f6f9; }
    .header-box {
        background: linear-gradient(135deg, #1a3a5c 0%, #2e6da4 100%);
        border-radius: 10px; padding: 20px 28px; margin-bottom: 20px; color: white;
    }
    .header-box h2 { margin: 0; font-size: 1.5rem; font-weight: 700; }
    .header-box p  { margin: 4px 0 0 0; font-size: 0.9rem; opacity: 0.85; }
    .kpi-card {
        background: white; border-radius: 8px; padding: 16px 20px;
        box-shadow: 0 1px 4px rgba(0,0,0,.10); text-align: center;
    }
    .kpi-label { font-size: 0.75rem; color: #666; text-transform: uppercase; letter-spacing: .05em; }
    .kpi-value { font-size: 1.6rem; font-weight: 700; color: #1a3a5c; }
    .kpi-sub   { font-size: 0.75rem; color: #888; }
    .section-title {
        font-size: 1rem; font-weight: 700; color: #1a3a5c;
        border-left: 4px solid #2e6da4; padding-left: 10px; margin: 24px 0 12px 0;
    }
    div[data-testid="stSelectbox"] label { font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ─────── Sidebar: filtros ───────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/lightning-bolt.png", width=40)
    st.title("Filtros")

    if ONEDRIVE_SHARE_URL == "PEGA_AQUI_TU_LINK_DE_ONEDRIVE":
        st.error("⚠️ Configura el link de OneDrive en `app.py`")
        st.stop()

    try:
        df_all = load_data(ONEDRIVE_SHARE_URL)
    except Exception as e:
        st.error(f"Error al cargar datos:\n{e}")
        st.stop()

    retiros = sorted(df_all["Retiro"].dropna().unique())
    sel_retiro = st.selectbox("Retiro", retiros)

    claves_disp = sorted(df_all.loc[df_all["Retiro"] == sel_retiro, "clave"].dropna().unique())
    sel_clave = st.selectbox("Clave", claves_disp)

    st.markdown("---")
    st.caption("Datos actualizados cada hora")

# ─────── Filtrar ───────
df = df_all[(df_all["Retiro"] == sel_retiro) & (df_all["clave"] == sel_clave)].copy()

if df.empty:
    st.warning("No hay datos para esta selección.")
    st.stop()

df["año"] = df["Clave_Año_Mes"] // 100
df["mes"]  = df["Clave_Año_Mes"]  % 100
df["semestre"] = df["mes"].apply(semestre)
df["periodo_label"] = df["Clave_Año_Mes"].apply(periodo_label)

# ─────── Header ───────
barra_val = df["Barra"].iloc[0] if "Barra" in df.columns else "—"
sum_val = df["Suministrador"].iloc[0] if "Suministrador" in df.columns else "—"

st.markdown(f"""
<div class="header-box">
  <h2>⚡ Dashboard de Retiros</h2>
  <p>Retiro: <b>{sel_retiro}</b> &nbsp;|&nbsp; Clave: <b>{sel_clave}</b>
     &nbsp;|&nbsp; Barra: <b>{barra_val}</b> &nbsp;|&nbsp; Suministrador: <b>{sum_val}</b></p>
</div>
""", unsafe_allow_html=True)

# ─────── KPIs ───────
mensual = df.groupby(["Clave_Año_Mes","periodo_label"])["Medida_kWh"].sum().reset_index()
mensual.columns = ["ym","label","total_kwh"]
mensual["total_mwh"] = mensual["total_kwh"] / 1000

total_anual   = mensual["total_kwh"].sum() / 1_000_000   # GWh
min_mes       = mensual.loc[mensual["total_kwh"].idxmin()]
max_mes       = mensual.loc[mensual["total_kwh"].idxmax()]
promedio_mes  = mensual["total_kwh"].mean() / 1000        # MWh

# Bloques solar (08–17) y nocturno (18–07)
solar_kwh  = df[df["Hora_Mensual"].between(8, 17)]["Medida_kWh"].sum()
noct_kwh   = df[~df["Hora_Mensual"].between(8, 17)]["Medida_kWh"].sum()
total_kwh_all = df["Medida_kWh"].sum()
pct_solar  = solar_kwh / total_kwh_all * 100 if total_kwh_all > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
def kpi(col, label, value, sub=""):
    col.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

kpi(k1, "Energía Anual", f"{total_anual:.3f} GWh")
kpi(k2, "Promedio Mensual", f"{promedio_mes:,.0f} MWh")
kpi(k3, "Menor Consumo", f"{min_mes['total_kwh']/1000:,.0f} MWh", min_mes['label'])
kpi(k4, "Mayor Consumo",  f"{max_mes['total_kwh']/1000:,.0f} MWh", max_mes['label'])
kpi(k5, "Bloque Solar 08–17h", f"{pct_solar:.1f}%", f"{solar_kwh/1_000_000:.2f} GWh")

# ─────── Gráfico 1: Consumo Mensual ───────
st.markdown('<div class="section-title">Consumo Mensual [MWh]</div>', unsafe_allow_html=True)

fig_bar = go.Figure()
fig_bar.add_trace(go.Bar(
    x=mensual["label"], y=mensual["total_mwh"],
    marker_color="#2e6da4",
    text=mensual["total_mwh"].apply(lambda v: f"{v:,.0f}"),
    textposition="outside", textfont_size=11,
))
fig_bar.update_layout(
    height=320, margin=dict(t=20, b=10, l=40, r=20),
    plot_bgcolor="white", paper_bgcolor="white",
    yaxis=dict(title="MWh", gridcolor="#eee"),
    xaxis=dict(tickangle=-30),
    showlegend=False,
)
st.plotly_chart(fig_bar, use_container_width=True)

# ─────── Gráficos 2 & 3: Curvas por semestre ───────
st.markdown('<div class="section-title">Curvas de Consumo por Hora y Tipo de Día</div>', unsafe_allow_html=True)

col_oct, col_abr = st.columns(2)

for col_ui, sem_label in [(col_oct, "Oct–Mar"), (col_abr, "Abr–Sep")]:
    df_sem = df[df["semestre"] == sem_label]
    if df_sem.empty:
        col_ui.info(f"Sin datos para {sem_label}")
        continue
    curva = df_sem.groupby(["Hora_Mensual","Tipo"])["Medida_kWh"].mean().reset_index()
    total_sem = curva["Medida_kWh"].max() if not curva.empty else 1

    fig_c = go.Figure()
    for tipo in curva["Tipo"].unique():
        d = curva[curva["Tipo"] == tipo].sort_values("Hora_Mensual")
        pct = d["Medida_kWh"] / total_sem * 100
        color = TIPO_COLORS.get(tipo, None)
        fig_c.add_trace(go.Scatter(
            x=d["Hora_Mensual"], y=pct,
            mode="lines", name=tipo,
            line=dict(color=color, width=2),
        ))
    fig_c.update_layout(
        title=dict(text=sem_label, font=dict(size=13, color="#1a3a5c")),
        height=300, margin=dict(t=40, b=10, l=40, r=10),
        plot_bgcolor="white", paper_bgcolor="white",
        yaxis=dict(title="%", ticksuffix="%", range=[0, 110], gridcolor="#eee"),
        xaxis=dict(title="Hora", dtick=2, gridcolor="#eee"),
        legend=dict(font=dict(size=10), orientation="h", y=-0.25),
    )
    col_ui.plotly_chart(fig_c, use_container_width=True)

# ─────── Gráfico 4: Consumo Diario Promedio por día semana ───────
st.markdown('<div class="section-title">Consumo Diario Promedio [kWh] por Hora</div>', unsafe_allow_html=True)

diario = df.groupby(["Hora_Mensual","Tipo"])["Medida_kWh"].mean().reset_index()
fig_d = go.Figure()
palette = px.colors.qualitative.Set2
for i, tipo in enumerate(sorted(diario["Tipo"].unique())):
    d = diario[diario["Tipo"] == tipo].sort_values("Hora_Mensual")
    fig_d.add_trace(go.Scatter(
        x=d["Hora_Mensual"], y=d["Medida_kWh"],
        mode="lines+markers", name=tipo,
        line=dict(color=palette[i % len(palette)], width=2),
        marker=dict(size=5),
    ))
fig_d.update_layout(
    height=350, margin=dict(t=20, b=10, l=60, r=20),
    plot_bgcolor="white", paper_bgcolor="white",
    yaxis=dict(title="kWh", gridcolor="#eee"),
    xaxis=dict(title="Hora", dtick=1, gridcolor="#eee"),
    legend=dict(font=dict(size=11), orientation="h", y=-0.2),
)
st.plotly_chart(fig_d, use_container_width=True)

# ─────── Tabla histórico ───────
st.markdown('<div class="section-title">Histórico por Período</div>', unsafe_allow_html=True)

hist = df.groupby(["Clave_Año_Mes","periodo_label"]).agg(
    Energía_kWh=("Medida_kWh","sum"),
    Consumo_Solar_kWh=("Medida_kWh", lambda x: x[df.loc[x.index,"Hora_Mensual"].between(8,17)].sum()),
    Consumo_Noche_kWh=("Medida_kWh", lambda x: x[~df.loc[x.index,"Hora_Mensual"].between(8,17)].sum()),
    Potencia_Max_kW=("Medida_kWh","max"),
).reset_index().sort_values("Clave_Año_Mes", ascending=False)

hist["Energía MWh"]      = (hist["Energía_kWh"]      / 1000).map("{:,.1f}".format)
hist["Solar 08–17 MWh"]  = (hist["Consumo_Solar_kWh"] / 1000).map("{:,.1f}".format)
hist["Noche 18–07 MWh"]  = (hist["Consumo_Noche_kWh"] / 1000).map("{:,.1f}".format)
hist["Potencia Máx kW"]  = hist["Potencia_Max_kW"].map("{:,.2f}".format)

st.dataframe(
    hist[["periodo_label","Energía MWh","Solar 08–17 MWh","Noche 18–07 MWh","Potencia Máx kW"]]
    .rename(columns={"periodo_label":"Período"}),
    use_container_width=True, hide_index=True,
)

# ─────── Footer ───────
st.markdown("---")
st.caption("Dashboard de Retiros · Datos del archivo Parquet cargado desde OneDrive")
