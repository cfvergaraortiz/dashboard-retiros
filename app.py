import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import plotly.graph_objects as go
import plotly.express as px
import requests
import io
import tempfile

DATA_URL = "https://github.com/cfvergaraortiz/dashboard-retiros/releases/download/v1.0/retiros_normalizado_sinR.parquet"

# ─────────────────────────────────────────────
# PALETA Y ESTILOS
# ─────────────────────────────────────────────
AZUL_OSCURO  = "#0f2942"
AZUL_MEDIO   = "#1a5276"
AZUL_CLARO   = "#2e86c1"
ACENTO       = "#1abc9c"
GRIS_FONDO   = "#f0f4f8"
BLANCO       = "#ffffff"

TIPO_COLORS = {
    "Lunes a Viernes No Feriado":   AZUL_CLARO,
    "Lunes a Viernes (No Feriado)": AZUL_CLARO,
    "Sábado":   "#e67e22",
    "Domingo":  ACENTO,
    "Feriado":  "#e74c3c",
}

# ─────────────────────────────────────────────
# DESCARGA — solo una vez, guarda en disco
# ─────────────────────────────────────────────
@st.cache_resource(show_spinner="Descargando datos (primera vez ~1 min)…")
def get_parquet_path():
    resp = requests.get(DATA_URL, timeout=300, stream=True)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
        tmp.write(chunk)
    tmp.close()
    return tmp.name

@st.cache_data(show_spinner=False)
def get_retiros(path):
    return sorted(pq.read_table(path, columns=["Retiro"])
                  .to_pandas()["Retiro"].dropna().unique().tolist())

@st.cache_data(show_spinner=False)
def get_claves(path, retiro):
    t = pq.read_table(path, columns=["Retiro","clave"],
                      filters=[("Retiro","=",retiro)])
    return sorted(t.to_pandas()["clave"].dropna().unique().tolist())

@st.cache_data(show_spinner="Filtrando datos…")
def load_filtered(path, retiro, clave):
    t = pq.read_table(path, filters=[("Retiro","=",retiro),("clave","=",clave)])
    return t.to_pandas()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def detect_col_ym(df):
    for c in ["Clave_Año_Mes", "Clave_Anio_Mes"]:
        if c in df.columns:
            return c
    matches = [c for c in df.columns if "mes" in c.lower()]
    return matches[0] if matches else None

def periodo_label(ym):
    meses = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
    y, m = divmod(int(ym), 100)
    return f"{meses[m-1]} {y}"

def semestre_label(mes):
    return "Oct–Mar" if mes in [10,11,12,1,2,3] else "Abr–Sep"

# ─────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(page_title="Dashboard Retiros", layout="wide", page_icon="⚡")

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=DM+Sans:wght@400;500;700&display=swap');

    html, body, [class*="css"] {{ font-family: 'DM Sans', sans-serif; }}
    .main {{ background-color: {GRIS_FONDO}; }}
    .block-container {{ padding-top: 1.5rem; }}

    .header-box {{
        background: linear-gradient(135deg, {AZUL_OSCURO} 0%, {AZUL_MEDIO} 60%, {AZUL_CLARO} 100%);
        border-radius: 12px; padding: 22px 32px; margin-bottom: 24px; color: white;
        box-shadow: 0 4px 16px rgba(15,41,66,.25);
    }}
    .header-box h2 {{ margin: 0; font-size: 1.4rem; font-weight: 700; letter-spacing: -.02em; }}
    .header-box p  {{ margin: 6px 0 0 0; font-size: .88rem; opacity: .80; }}
    .header-box b  {{ opacity: 1; font-weight: 600; }}

    .kpi-card {{
        background: {BLANCO}; border-radius: 10px; padding: 18px 16px 14px;
        box-shadow: 0 2px 8px rgba(0,0,0,.07); text-align: center;
        border-top: 3px solid {AZUL_CLARO};
    }}
    .kpi-label {{ font-size: .68rem; color: #7f8c8d; text-transform: uppercase;
                  letter-spacing: .08em; font-weight: 600; margin-bottom: 6px; }}
    .kpi-value {{ font-size: 1.55rem; font-weight: 700; color: {AZUL_OSCURO}; line-height: 1.1; }}
    .kpi-sub   {{ font-size: .72rem; color: #95a5a6; margin-top: 4px; }}
    .kpi-card.acento {{ border-top-color: {ACENTO}; }}
    .kpi-card.acento .kpi-value {{ color: #0e6655; }}

    .section-title {{
        font-size: .9rem; font-weight: 700; color: {AZUL_OSCURO};
        border-left: 4px solid {ACENTO}; padding-left: 10px;
        margin: 28px 0 14px 0; text-transform: uppercase; letter-spacing: .05em;
    }}

    section[data-testid="stSidebar"] {{
        background: {AZUL_OSCURO};
    }}
    section[data-testid="stSidebar"] * {{ color: white !important; }}
    section[data-testid="stSidebar"] .stSelectbox > div > div {{
        background: rgba(255,255,255,.1) !important;
        border: 1px solid rgba(255,255,255,.2) !important;
        border-radius: 8px !important;
    }}
    hr {{ border-color: rgba(255,255,255,.15) !important; }}
</style>
""", unsafe_allow_html=True)

# ─── Descargar archivo ───
try:
    parquet_path = get_parquet_path()
except Exception as e:
    st.error(f"Error al descargar el archivo: {e}")
    st.stop()

# ─── Sidebar ───
with st.sidebar:
    st.markdown("### ⚡ Dashboard Retiros")
    st.markdown("---")

    retiros    = get_retiros(parquet_path)
    sel_retiro = st.selectbox("Retiro", retiros)

    claves    = get_claves(parquet_path, sel_retiro)
    sel_clave = st.selectbox("Clave", claves)

    st.markdown("---")
    st.caption(f"📦 {len(retiros):,} retiros disponibles")

# ─── Datos filtrados ───
df = load_filtered(parquet_path, sel_retiro, sel_clave)

if df.empty:
    st.warning("No hay datos para esta selección.")
    st.stop()

# Detectar columna de período
col_ym = detect_col_ym(df)
if not col_ym:
    st.error(f"No se encontró columna de período. Columnas: {list(df.columns)}")
    st.stop()

# ── Calcular hora del día (0–23) desde Hora_Mensual (1–744)
df["Medida_kWh"]    = df["Medida_kWh"].abs()
df["hora_dia"]      = (df["Hora_Mensual"] - 1) % 24
df["mes"]           = df[col_ym] % 100
df["semestre"]      = df["mes"].apply(semestre_label)
df["periodo_label"] = df[col_ym].apply(periodo_label)

# ─── Header ───
barra_val = df["Barra"].iloc[0]        if "Barra"         in df.columns else "—"
sum_val   = df["Suministrador"].iloc[0] if "Suministrador" in df.columns else "—"
st.markdown(f"""
<div class="header-box">
  <h2>⚡ &nbsp;{sel_retiro}</h2>
  <p>Clave: <b>{sel_clave}</b> &nbsp;·&nbsp; Barra: <b>{barra_val}</b> &nbsp;·&nbsp; Suministrador: <b>{sum_val}</b></p>
</div>""", unsafe_allow_html=True)

# ─── KPIs ───
mensual = df.groupby([col_ym,"periodo_label"])["Medida_kWh"].sum().reset_index()
mensual.columns = ["ym","label","total_kwh"]
mensual["total_mwh"] = mensual["total_kwh"] / 1000

total_anual  = mensual["total_kwh"].sum() / 1_000_000
min_mes      = mensual.loc[mensual["total_kwh"].idxmin()]
max_mes      = mensual.loc[mensual["total_kwh"].idxmax()]
promedio_mes = mensual["total_kwh"].mean() / 1000
solar_kwh    = df[df["hora_dia"].between(8,17)]["Medida_kWh"].sum()
pct_solar    = solar_kwh / df["Medida_kWh"].sum() * 100 if df["Medida_kWh"].sum() > 0 else 0

def kpi(col, label, value, sub="", acento=False):
    cls = "kpi-card acento" if acento else "kpi-card"
    col.markdown(f"""<div class="{cls}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

k1,k2,k3,k4,k5 = st.columns(5)
kpi(k1, "Energía Anual",       f"{total_anual:.3f} GWh")
kpi(k2, "Promedio Mensual",    f"{promedio_mes:,.0f} MWh")
kpi(k3, "Menor Consumo",       f"{min_mes['total_kwh']/1000:,.0f} MWh", min_mes['label'])
kpi(k4, "Mayor Consumo",       f"{max_mes['total_kwh']/1000:,.0f} MWh", max_mes['label'])
kpi(k5, "Bloque Solar 08–17h", f"{pct_solar:.1f}%", f"{solar_kwh/1_000_000:.2f} GWh", acento=True)

# ─── Consumo Mensual ───
st.markdown('<div class="section-title">Consumo Mensual</div>', unsafe_allow_html=True)

fig_bar = go.Figure(go.Bar(
    x=mensual["label"], y=mensual["total_mwh"],
    marker=dict(
        color=mensual["total_mwh"],
        colorscale=[[0, "#aed6f1"], [1, AZUL_OSCURO]],
        showscale=False,
    ),
    text=mensual["total_mwh"].apply(lambda v: f"{v:,.0f}"),
    textposition="outside", textfont=dict(size=11, color=AZUL_OSCURO),
))
fig_bar.update_layout(
    height=330, margin=dict(t=20,b=10,l=50,r=20),
    plot_bgcolor=BLANCO, paper_bgcolor=GRIS_FONDO,
    yaxis=dict(title="MWh", gridcolor="#e8ecf0", tickformat=",.0f"),
    xaxis=dict(tickangle=-30),
    showlegend=False,
)
st.plotly_chart(fig_bar, use_container_width=True)

# ─── Curvas por semestre ───
st.markdown('<div class="section-title">Curvas de Consumo por Hora y Tipo de Día</div>', unsafe_allow_html=True)

col_oct, col_abr = st.columns(2)
for col_ui, sem in [(col_oct,"Oct–Mar"), (col_abr,"Abr–Sep")]:
    df_sem = df[df["semestre"] == sem]
    if df_sem.empty:
        col_ui.info(f"Sin datos para {sem}")
        continue

    # Promedio por hora_dia (0–23) y tipo de día
    curva = df_sem.groupby(["hora_dia","Tipo"])["Medida_kWh"].mean().reset_index()
    top   = curva["Medida_kWh"].max() or 1

    fig_c = go.Figure()
    for tipo in curva["Tipo"].unique():
        d = curva[curva["Tipo"]==tipo].sort_values("hora_dia")
        fig_c.add_trace(go.Scatter(
            x=d["hora_dia"], y=d["Medida_kWh"]/top*100,
            mode="lines", name=tipo,
            line=dict(color=TIPO_COLORS.get(tipo,"#999"), width=2.5),
            hovertemplate="%{x}h: %{y:.1f}%<extra>" + tipo + "</extra>",
        ))
    fig_c.update_layout(
        title=dict(text=sem, font=dict(size=13, color=AZUL_OSCURO, family="DM Sans")),
        height=300, margin=dict(t=40,b=10,l=45,r=10),
        plot_bgcolor=BLANCO, paper_bgcolor=GRIS_FONDO,
        yaxis=dict(title="%", ticksuffix="%", range=[0,110], gridcolor="#e8ecf0"),
        xaxis=dict(title="Hora", dtick=2, range=[-0.5,23.5], gridcolor="#e8ecf0",
                   tickvals=list(range(0,24,2))),
        legend=dict(font=dict(size=10), orientation="h", y=-0.3, bgcolor="rgba(0,0,0,0)"),
    )
    col_ui.plotly_chart(fig_c, use_container_width=True)

# ─── Consumo Diario Promedio ───
st.markdown('<div class="section-title">Consumo Promedio por Hora y Tipo de Día [kWh]</div>', unsafe_allow_html=True)

diario = df.groupby(["hora_dia","Tipo"])["Medida_kWh"].mean().reset_index()
tipos  = sorted(diario["Tipo"].unique())
palette = [AZUL_CLARO, "#e67e22", ACENTO, "#e74c3c", "#8e44ad", "#2ecc71"]

fig_d = go.Figure()
for i, tipo in enumerate(tipos):
    d = diario[diario["Tipo"]==tipo].sort_values("hora_dia")
    fig_d.add_trace(go.Scatter(
        x=d["hora_dia"], y=d["Medida_kWh"],
        mode="lines+markers", name=tipo,
        line=dict(color=palette[i % len(palette)], width=2.5),
        marker=dict(size=5),
        hovertemplate="%{x}h: %{y:,.1f} kWh<extra>" + tipo + "</extra>",
    ))
fig_d.update_layout(
    height=360, margin=dict(t=20,b=10,l=65,r=20),
    plot_bgcolor=BLANCO, paper_bgcolor=GRIS_FONDO,
    yaxis=dict(title="kWh", gridcolor="#e8ecf0", tickformat=",.0f"),
    xaxis=dict(title="Hora", dtick=1, range=[-0.5,23.5], gridcolor="#e8ecf0",
               tickvals=list(range(0,24))),
    legend=dict(font=dict(size=11), orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)"),
)
st.plotly_chart(fig_d, use_container_width=True)

# ─── Tabla histórico ───
st.markdown('<div class="section-title">Histórico por Período</div>', unsafe_allow_html=True)

hist = df.groupby([col_ym,"periodo_label"]).agg(
    Energía_kWh    =("Medida_kWh","sum"),
    Potencia_Max_kW=("Medida_kWh","max"),
).reset_index().sort_values(col_ym, ascending=False)

solar_pm = df[df["hora_dia"].between(8,17)].groupby(col_ym)["Medida_kWh"].sum()/1000
noche_pm = df[~df["hora_dia"].between(8,17)].groupby(col_ym)["Medida_kWh"].sum()/1000

hist["Energía MWh"]     = (hist["Energía_kWh"]/1000).map("{:,.1f}".format)
hist["Solar 08–17 MWh"] = hist[col_ym].map(solar_pm).map(lambda v: f"{v:,.1f}" if pd.notna(v) else "—")
hist["Noche 18–07 MWh"] = hist[col_ym].map(noche_pm).map(lambda v: f"{v:,.1f}" if pd.notna(v) else "—")
hist["Potencia Máx kW"] = hist["Potencia_Max_kW"].map("{:,.2f}".format)

st.dataframe(
    hist[["periodo_label","Energía MWh","Solar 08–17 MWh","Noche 18–07 MWh","Potencia Máx kW"]]
    .rename(columns={"periodo_label":"Período"}),
    use_container_width=True, hide_index=True,
)

st.markdown("---")
st.caption("Dashboard de Retiros · Datos cargados desde GitHub Releases")
