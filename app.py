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
    """Lee solo la columna Retiro para poblar el selector."""
    return sorted(pq.read_table(path, columns=["Retiro"]).to_pandas()["Retiro"].dropna().unique().tolist())

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
def periodo_label(ym):
    meses = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
    y, m = divmod(int(ym), 100)
    return f"{meses[m-1]} {y}"

def semestre_label(mes):
    return "Oct–Mar" if mes in [10,11,12,1,2,3] else "Abr–Sep"

TIPO_COLORS = {
    "Lunes a Viernes No Feriado":   "#1f77b4",
    "Lunes a Viernes (No Feriado)": "#1f77b4",
    "Sábado":  "#ff7f0e",
    "Domingo": "#2ca02c",
    "Feriado": "#d62728",
}

# ─────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(page_title="Dashboard Retiros", layout="wide", page_icon="⚡")
st.markdown("""
<style>
    .header-box {
        background: linear-gradient(135deg,#1a3a5c 0%,#2e6da4 100%);
        border-radius:10px; padding:20px 28px; margin-bottom:20px; color:white;
    }
    .header-box h2 { margin:0; font-size:1.5rem; font-weight:700; }
    .header-box p  { margin:4px 0 0 0; font-size:.9rem; opacity:.85; }
    .kpi-card { background:white; border-radius:8px; padding:16px 20px;
        box-shadow:0 1px 4px rgba(0,0,0,.10); text-align:center; }
    .kpi-label { font-size:.75rem; color:#666; text-transform:uppercase; letter-spacing:.05em; }
    .kpi-value { font-size:1.6rem; font-weight:700; color:#1a3a5c; }
    .kpi-sub   { font-size:.75rem; color:#888; }
    .section-title { font-size:1rem; font-weight:700; color:#1a3a5c;
        border-left:4px solid #2e6da4; padding-left:10px; margin:24px 0 12px 0; }
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
    st.image("https://img.icons8.com/fluency/48/lightning-bolt.png", width=40)
    st.title("Filtros")

    retiros    = get_retiros(parquet_path)
    sel_retiro = st.selectbox("Retiro", retiros)

    claves    = get_claves(parquet_path, sel_retiro)
    sel_clave = st.selectbox("Clave", claves)
    st.markdown("---")
    st.caption("Datos: GitHub Releases")

# ─── Cargar datos filtrados ───
df = load_filtered(parquet_path, sel_retiro, sel_clave)

if df.empty:
    st.warning("No hay datos para esta selección.")
    st.stop()

# Detectar nombre de columna automáticamente
if "Clave_Año_Mes" in df.columns:
    col_ym = "Clave_Año_Mes"
elif "Clave_Anio_Mes" in df.columns:
    col_ym = "Clave_Anio_Mes"
else:
    # Fallback: buscar cualquier columna que contenga 'mes' (case insensitive)
    matches = [c for c in df.columns if "mes" in c.lower()]
    if matches:
        col_ym = matches[0]
    else:
        st.error(f"No se encontró columna de período. Columnas disponibles: {list(df.columns)}")
        st.stop()

df["mes"]           = df[col_ym] % 100
df["semestre"]      = df["mes"].apply(semestre_label)
df["periodo_label"] = df[col_ym].apply(periodo_label)
df["semestre"]      = df["mes"].apply(semestre_label)
df["periodo_label"] = df[col_ym].apply(periodo_label)

# ─── Header ───
barra_val = df["Barra"].iloc[0]       if "Barra"         in df.columns else "—"
sum_val   = df["Suministrador"].iloc[0] if "Suministrador" in df.columns else "—"
st.markdown(f"""
<div class="header-box">
  <h2>⚡ Dashboard de Retiros</h2>
  <p>Retiro: <b>{sel_retiro}</b> &nbsp;|&nbsp; Clave: <b>{sel_clave}</b>
     &nbsp;|&nbsp; Barra: <b>{barra_val}</b> &nbsp;|&nbsp; Suministrador: <b>{sum_val}</b></p>
</div>""", unsafe_allow_html=True)

# ─── KPIs ───
mensual = df.groupby([col_ym,"periodo_label"])["Medida_kWh"].sum().reset_index()
mensual.columns = ["ym","label","total_kwh"]
mensual["total_mwh"] = mensual["total_kwh"] / 1000

total_anual  = mensual["total_kwh"].sum() / 1_000_000
min_mes      = mensual.loc[mensual["total_kwh"].idxmin()]
max_mes      = mensual.loc[mensual["total_kwh"].idxmax()]
promedio_mes = mensual["total_kwh"].mean() / 1000
solar_kwh    = df[df["Hora_Mensual"].between(8,17)]["Medida_kWh"].sum()
pct_solar    = solar_kwh / df["Medida_kWh"].sum() * 100 if df["Medida_kWh"].sum() > 0 else 0

def kpi(col, label, value, sub=""):
    col.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

k1,k2,k3,k4,k5 = st.columns(5)
kpi(k1,"Energía Anual",       f"{total_anual:.3f} GWh")
kpi(k2,"Promedio Mensual",    f"{promedio_mes:,.0f} MWh")
kpi(k3,"Menor Consumo",       f"{min_mes['total_kwh']/1000:,.0f} MWh", min_mes['label'])
kpi(k4,"Mayor Consumo",       f"{max_mes['total_kwh']/1000:,.0f} MWh", max_mes['label'])
kpi(k5,"Bloque Solar 08–17h", f"{pct_solar:.1f}%", f"{solar_kwh/1_000_000:.2f} GWh")

# ─── Consumo Mensual ───
st.markdown('<div class="section-title">Consumo Mensual [MWh]</div>', unsafe_allow_html=True)
fig_bar = go.Figure(go.Bar(
    x=mensual["label"], y=mensual["total_mwh"],
    marker_color="#2e6da4",
    text=mensual["total_mwh"].apply(lambda v: f"{v:,.0f}"),
    textposition="outside", textfont_size=11,
))
fig_bar.update_layout(height=320, margin=dict(t=20,b=10,l=40,r=20),
    plot_bgcolor="white", paper_bgcolor="white",
    yaxis=dict(title="MWh", gridcolor="#eee"),
    xaxis=dict(tickangle=-30), showlegend=False)
st.plotly_chart(fig_bar, use_container_width=True)

# ─── Curvas por semestre ───
st.markdown('<div class="section-title">Curvas de Consumo por Hora y Tipo de Día</div>', unsafe_allow_html=True)
col_oct, col_abr = st.columns(2)
for col_ui, sem in [(col_oct,"Oct–Mar"),(col_abr,"Abr–Sep")]:
    df_sem = df[df["semestre"]==sem]
    if df_sem.empty:
        col_ui.info(f"Sin datos para {sem}"); continue
    curva = df_sem.groupby(["Hora_Mensual","Tipo"])["Medida_kWh"].mean().reset_index()
    top   = curva["Medida_kWh"].max() or 1
    fig_c = go.Figure()
    for tipo in curva["Tipo"].unique():
        d = curva[curva["Tipo"]==tipo].sort_values("Hora_Mensual")
        fig_c.add_trace(go.Scatter(
            x=d["Hora_Mensual"], y=d["Medida_kWh"]/top*100,
            mode="lines", name=tipo,
            line=dict(color=TIPO_COLORS.get(tipo,"#999"), width=2),
        ))
    fig_c.update_layout(title=dict(text=sem, font=dict(size=13,color="#1a3a5c")),
        height=300, margin=dict(t=40,b=10,l=40,r=10),
        plot_bgcolor="white", paper_bgcolor="white",
        yaxis=dict(title="%",ticksuffix="%",range=[0,110],gridcolor="#eee"),
        xaxis=dict(title="Hora",dtick=2,gridcolor="#eee"),
        legend=dict(font=dict(size=10),orientation="h",y=-0.3))
    col_ui.plotly_chart(fig_c, use_container_width=True)

# ─── Consumo Diario Promedio ───
st.markdown('<div class="section-title">Consumo Diario Promedio [kWh] por Hora</div>', unsafe_allow_html=True)
diario  = df.groupby(["Hora_Mensual","Tipo"])["Medida_kWh"].mean().reset_index()
palette = px.colors.qualitative.Set2
fig_d   = go.Figure()
for i, tipo in enumerate(sorted(diario["Tipo"].unique())):
    d = diario[diario["Tipo"]==tipo].sort_values("Hora_Mensual")
    fig_d.add_trace(go.Scatter(
        x=d["Hora_Mensual"], y=d["Medida_kWh"],
        mode="lines+markers", name=tipo,
        line=dict(color=palette[i%len(palette)],width=2),
        marker=dict(size=5),
    ))
fig_d.update_layout(height=350, margin=dict(t=20,b=10,l=60,r=20),
    plot_bgcolor="white", paper_bgcolor="white",
    yaxis=dict(title="kWh",gridcolor="#eee"),
    xaxis=dict(title="Hora",dtick=1,gridcolor="#eee"),
    legend=dict(font=dict(size=11),orientation="h",y=-0.2))
st.plotly_chart(fig_d, use_container_width=True)

# ─── Tabla histórico ───
st.markdown('<div class="section-title">Histórico por Período</div>', unsafe_allow_html=True)
hist = df.groupby([col_ym,"periodo_label"]).agg(
    Energía_kWh=("Medida_kWh","sum"),
    Potencia_Max_kW=("Medida_kWh","max"),
).reset_index().sort_values(col_ym,ascending=False)

solar_por_mes = df[df["Hora_Mensual"].between(8,17)].groupby(col_ym)["Medida_kWh"].sum()/1000
noche_por_mes = df[~df["Hora_Mensual"].between(8,17)].groupby(col_ym)["Medida_kWh"].sum()/1000
hist["Solar 08–17 MWh"] = hist[col_ym].map(solar_por_mes).map(lambda v: f"{v:,.1f}" if pd.notna(v) else "—")
hist["Noche 18–07 MWh"] = hist[col_ym].map(noche_por_mes).map(lambda v: f"{v:,.1f}" if pd.notna(v) else "—")
hist["Energía MWh"]     = (hist["Energía_kWh"]/1000).map("{:,.1f}".format)
hist["Potencia Máx kW"] = hist["Potencia_Max_kW"].map("{:,.2f}".format)

st.dataframe(
    hist[["periodo_label","Energía MWh","Solar 08–17 MWh","Noche 18–07 MWh","Potencia Máx kW"]]
    .rename(columns={"periodo_label":"Período"}),
    use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("Dashboard de Retiros · Datos cargados desde GitHub Releases")
