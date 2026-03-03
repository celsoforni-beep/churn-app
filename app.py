import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import os
from datetime import date

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Sistema Clientes — Novo / Retorno / Churn",
    layout="wide"
)

DB_PATH = "clientes.db"
SALT = os.getenv("HASH_SALT", "default_salt")

# =====================================================
# HASH FUNCTIONS
# =====================================================

def clean_document(doc):
    return "".join([c for c in str(doc) if c.isdigit()])


def normalize_email(email):
    return str(email).strip().lower()


def hash_id(value):
    raw = (SALT + value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# =====================================================
# DATABASE
# =====================================================

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            customer_id TEXT,
            order_id TEXT UNIQUE,
            data_pedido TEXT,
            mes_compra TEXT,
            valor_pedido REAL
        )
    """)

    conn.commit()
    conn.close()

# =====================================================
# CSV NORMALIZATION
# =====================================================

def normalize_input(df):

    cols = {c.strip().lower(): c for c in df.columns}

    base = df[[
        cols["client document"],
        cols["email"],
        cols["order2"],
        cols["creation d"]
    ]].copy()

    base.columns = [
        "client_document",
        "email",
        "order_id",
        "data_pedido"
    ]

    base["data_pedido"] = pd.to_datetime(
        base["data_pedido"],
        dayfirst=True,
        errors="coerce"
    )

    base = base.dropna(subset=["data_pedido"])

    doc_clean = base["client_document"].apply(clean_document)
    email_clean = base["email"].apply(normalize_email)

    identificador = doc_clean.copy()
    mask = identificador.str.len() == 0
    identificador.loc[mask] = email_clean.loc[mask]

    base["customer_id"] = identificador.apply(
        lambda x: hash_id(str(x))
    )

    base["mes_compra"] = base["data_pedido"].astype(str).str[:7]

    if "total value" in cols:
        valor = df[cols["total value"]].astype(str)

        valor = (
            valor
            .str.replace("R$", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )

        base["valor_pedido"] = pd.to_numeric(valor, errors="coerce")
    else:
        base["valor_pedido"] = None

    return base[
        [
            "customer_id",
            "order_id",
            "data_pedido",
            "mes_compra",
            "valor_pedido"
        ]
    ]

# =====================================================
# INSERT DATA
# =====================================================

def upsert_pedidos(df):

    conn = get_conn()

    before = pd.read_sql(
        "SELECT COUNT(*) n FROM pedidos",
        conn
    )["n"][0]

    df.to_sql(
        "pedidos",
        conn,
        if_exists="append",
        index=False
    )

    conn.commit()

    after = pd.read_sql(
        "SELECT COUNT(*) n FROM pedidos",
        conn
    )["n"][0]

    conn.close()

    return after - before

# =====================================================
# CLIENT BASE
# =====================================================

def build_cliente_base(ref_date):

    conn = get_conn()
    pedidos = pd.read_sql("SELECT * FROM pedidos", conn)
    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(
        pedidos["data_pedido"]
    )

    g = pedidos.groupby("customer_id")

    base = pd.DataFrame({
        "customer_id": g["customer_id"].first(),
        "primeira_compra": g["data_pedido"].min(),
        "ultima_compra": g["data_pedido"].max(),
        "qtd_pedidos": g["order_id"].nunique(),
        "receita_total": g["valor_pedido"].sum()
    }).reset_index(drop=True)

    base["dias_sem_compra"] = (
        pd.to_datetime(ref_date)
        - base["ultima_compra"]
    ).dt.days

    base["status"] = "Ativo"
    base.loc[base["dias_sem_compra"] > 90, "status"] = "Em risco"
    base.loc[base["dias_sem_compra"] > 180, "status"] = "Churn"

    return base.sort_values(
        "ultima_compra",
        ascending=False
    )

# =====================================================
# MONTH REPORT
# =====================================================

def month_report(mes):

    conn = get_conn()
    pedidos = pd.read_sql("SELECT * FROM pedidos", conn)
    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(
        pedidos["data_pedido"]
    )

    first = pedidos.groupby(
        "customer_id"
    )["data_pedido"].min().reset_index()

    first["mes_primeira"] = \
        first["data_pedido"].astype(str).str[:7]

    mes_df = pedidos[
        pedidos["mes_compra"] == mes
    ]

    clientes_mes = mes_df["customer_id"].nunique()

    novos = len(
        set(first[first["mes_primeira"] == mes]["customer_id"])
        &
        set(mes_df["customer_id"])
    )

    retornantes = clientes_mes - novos

    receita = mes_df["valor_pedido"].sum()

    return pd.DataFrame([{
        "Mes": mes,
        "Clientes": clientes_mes,
        "Novos": novos,
        "Retornantes": retornantes,
        "Receita": receita
    }])

# =====================================================
# UI
# =====================================================

init_db()

st.title("📊 Sistema Clientes — Novo / Retorno / Churn")

ref_date = st.sidebar.date_input(
    "Data fechamento",
    value=date.today()
)

uploaded = st.file_uploader(
    "Upload CSV mensal",
    type=["csv"]
)

if uploaded is not None:

    # leitura robusta CSV BR
    try:
        df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        try:
            df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="cp1252")
        except UnicodeDecodeError:
            df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="latin1")

    df = normalize_input(df_raw)

    inserted = upsert_pedidos(df)

    st.success(f"{inserted} pedidos adicionados")

st.divider()

base = build_cliente_base(ref_date)

if not base.empty:

    st.metric("Clientes", len(base))
    st.metric("Churn", (base["status"]=="Churn").sum())

    st.dataframe(base)

    st.download_button(
        "Baixar Base Clientes",
        base.to_csv(index=False),
        "base_clientes.csv"
    )

st.divider()

mes = st.text_input(
    "Mês Report (YYYY-MM)",
    value=ref_date.strftime("%Y-%m")
)

rep = month_report(mes)

if not rep.empty:
    st.dataframe(rep)
