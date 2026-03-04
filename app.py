import streamlit as st
import pandas as pd
import hashlib
import os
from datetime import date
import traceback

import psycopg2
from psycopg2.extras import execute_values

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Sistema Clientes — Novo / Retorno / Churn",
    layout="wide"
)

SALT = os.getenv("HASH_SALT", "default_salt")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

USE_POSTGRES = DATABASE_URL != ""

# =====================================================
# HASH (LGPD SAFE)
# =====================================================

def clean_document(doc) -> str:
    return "".join([c for c in str(doc) if c.isdigit()])


def normalize_email(email) -> str:
    return str(email).strip().lower()


def hash_id(value: str) -> str:
    raw = (SALT + value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# =====================================================
# DATABASE
# =====================================================

def get_conn():
    if not USE_POSTGRES:
        raise RuntimeError("DATABASE_URL não configurado no Secrets.")
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """
    Supabase: assume tabela public.pedidos já criada.
    Aqui apenas validamos conexão.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("select 1;")
    conn.commit()
    conn.close()

# =====================================================
# HELPERS
# =====================================================

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return df


def load_uploaded_file(uploaded) -> pd.DataFrame:
    filename = uploaded.name.lower()

    if filename.endswith(".xlsx"):
        df = pd.read_excel(uploaded)  # pandas escolhe engine (openpyxl)
        return normalize_headers(df)

    # CSV robusto
    try:
        df = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(uploaded, sep=None, engine="python", encoding="cp1252")
        except UnicodeDecodeError:
            df = pd.read_csv(uploaded, sep=None, engine="python", encoding="latin1")

    return normalize_headers(df)

# =====================================================
# NORMALIZAÇÃO INPUT
# =====================================================

def normalize_input(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_headers(df)
    cols = {c.strip().lower(): c for c in df.columns}

    # candidatos (aceita variações)
    doc_candidates = ["client document", "client_document", "clientdocument", "document", "cpf", "documento"]
    email_candidates = ["email", "e-mail", "mail"]
    order_candidates = ["order2", "order 2", "order", "order_id", "orderid", "pedido", "pedido id", "pedidoid"]
    date_candidates = ["creation d", "creation date", "created at", "data", "data pedido", "data_pedido", "creationd"]
    value_candidates = ["total value", "totalvalue", "total", "valor", "valor total", "total_value"]

    def pick(candidates):
        for k in candidates:
            if k in cols:
                return cols[k]
        return None

    doc_col = pick(doc_candidates)
    email_col = pick(email_candidates)
    order_col = pick(order_candidates)
    date_col = pick(date_candidates)
    value_col = pick(value_candidates)

    if order_col is None:
        raise ValueError(f"Não achei coluna de pedido. Colunas detectadas: {list(cols.keys())}")
    if date_col is None:
        raise ValueError(f"Não achei coluna de data. Colunas detectadas: {list(cols.keys())}")
    if (doc_col is None) and (email_col is None):
        raise ValueError("Não achei Client Document nem Email. Preciso de pelo menos 1 identificador.")

    base = df[[order_col, date_col]].copy()
    base.columns = ["order_id", "data_pedido"]

    base["client_document"] = df[doc_col] if doc_col else ""
    base["email"] = df[email_col] if email_col else ""

    # data
    base["data_pedido"] = pd.to_datetime(base["data_pedido"], dayfirst=True, errors="coerce")
    base = base.dropna(subset=["data_pedido", "order_id"])

    # id doc -> email
    doc_clean = base["client_document"].apply(clean_document)
    email_clean = base["email"].apply(normalize_email)

    identificador = doc_clean.copy()
    mask_sem_doc = identificador.str.len() == 0
    identificador.loc[mask_sem_doc] = email_clean.loc[mask_sem_doc]

    base["customer_id"] = identificador.apply(lambda x: hash_id(str(x)))

    base["mes_compra"] = base["data_pedido"].dt.strftime("%Y-%m")

    # valor
    if value_col:
        valor = df[value_col].astype(str)
        valor = (
            valor.str.replace("R$", "", regex=False)
                 .str.replace(".", "", regex=False)
                 .str.replace(",", ".", regex=False)
                 .str.strip()
        )
        base["valor_pedido"] = pd.to_numeric(valor, errors="coerce")
    else:
        base["valor_pedido"] = None

    # SEM PII
    return base[["customer_id", "order_id", "data_pedido", "mes_compra", "valor_pedido"]]

# =====================================================
# UPSERT POSTGRES (SUPABASE)
# =====================================================

def upsert_pedidos(df: pd.DataFrame) -> int:
    conn = get_conn()
    cur = conn.cursor()

    # antes
    before = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]

    df2 = df.copy()
    df2["data_pedido"] = pd.to_datetime(df2["data_pedido"]).dt.date

    rows = list(
        df2[["customer_id", "order_id", "data_pedido", "mes_compra", "valor_pedido"]]
        .itertuples(index=False, name=None)
    )

    execute_values(
        cur,
        """
        insert into public.pedidos (customer_id, order_id, data_pedido, mes_compra, valor_pedido)
        values %s
        on conflict (order_id) do update set
          customer_id = excluded.customer_id,
          data_pedido = excluded.data_pedido,
          mes_compra = excluded.mes_compra,
          valor_pedido = excluded.valor_pedido
        """,
        rows,
        page_size=5000
    )

    conn.commit()

    after = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]
    conn.close()

    return int(after - before)

# =====================================================
# ANALYTICS
# =====================================================

def build_cliente_base(ref_date: date, ativo_dias=90, churn_dias=180) -> pd.DataFrame:
    conn = get_conn()
    pedidos = pd.read_sql("select * from public.pedidos", conn)
    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(pedidos["data_pedido"], errors="coerce")
    pedidos = pedidos.dropna(subset=["data_pedido"])

    g = pedidos.groupby("customer_id")
    base = pd.DataFrame({
        "customer_id": g["customer_id"].first(),
        "primeira_compra": g["data_pedido"].min(),
        "ultima_compra": g["data_pedido"].max(),
        "qtd_pedidos": g["order_id"].nunique(),
        "receita_total": g["valor_pedido"].sum(min_count=1)
    }).reset_index(drop=True)

    base["dias_sem_compra"] = (pd.to_datetime(ref_date) - base["ultima_compra"]).dt.days

    base["status"] = "Ativo"
    base.loc[base["dias_sem_compra"] > ativo_dias, "status"] = "Em risco"
    base.loc[base["dias_sem_compra"] > churn_dias, "status"] = "Churn"

    return base.sort_values("ultima_compra", ascending=False)


def month_report(mes: str) -> pd.DataFrame:
    conn = get_conn()
    pedidos = pd.read_sql("select * from public.pedidos", conn)
    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(pedidos["data_pedido"], errors="coerce")
    pedidos = pedidos.dropna(subset=["data_pedido"])

    first = pedidos.groupby("customer_id")["data_pedido"].min().reset_index()
    first["mes_primeira"] = first["data_pedido"].dt.strftime("%Y-%m")

    mes_df = pedidos[pedidos["mes_compra"] == mes].copy()

    clientes_mes = int(mes_df["customer_id"].nunique())
    pedidos_mes = int(mes_df["order_id"].nunique())
    receita_mes = float(mes_df["valor_pedido"].sum(skipna=True)) if "valor_pedido" in mes_df.columns else 0.0

    novos_ids = set(first.loc[first["mes_primeira"] == mes, "customer_id"].tolist())
    compraram_ids = set(mes_df["customer_id"].tolist())

    novos = len(novos_ids.intersection(compraram_ids))
    retornantes = clientes_mes - novos

    return pd.DataFrame([{
        "Mes": mes,
        "Clientes": clientes_mes,
        "Pedidos": pedidos_mes,
        "Receita": receita_mes,
        "Novos": int(novos),
        "Retornantes": int(retornantes),
        "% Novos": (novos / clientes_mes) if clientes_mes else 0.0
    }])

# =====================================================
# APP UI
# =====================================================

try:
    init_db()
except Exception as e:
    st.error("❌ Falha ao conectar no Supabase. Verifique DATABASE_URL no Secrets.")
    st.exception(e)
    st.stop()

st.title("📊 Sistema Clientes — Novo / Retorno / Churn (Supabase)")

st.sidebar.header("Configurações")
ref_date = st.sidebar.date_input("Data fechamento", value=date.today())
ativo_dias = st.sidebar.number_input("Ativo (dias)", min_value=30, max_value=365, value=90, step=15)
churn_dias = st.sidebar.number_input("Churn (dias)", min_value=90, max_value=720, value=180, step=30)

uploaded = st.file_uploader("Upload mensal (Excel .xlsx ou CSV)", type=["xlsx", "csv"])

if uploaded is not None:
    try:
        df_raw = load_uploaded_file(uploaded)
        st.write("✅ Colunas detectadas:", list(df_raw.columns))

        df = normalize_input(df_raw)
        inserted = upsert_pedidos(df)
        st.success(f"{inserted} pedidos adicionados (Supabase)")

    except Exception as e:
        st.error("Erro ao processar o arquivo. Detalhes:")
        st.exception(e)

st.divider()

base = build_cliente_base(ref_date, ativo_dias=ativo_dias, churn_dias=churn_dias)

if base.empty:
    st.info("Ainda não há dados no Supabase. Faça upload do arquivo do mês para criar a base.")
else:
    c1, c2, c3 = st.columns(3)
    c1.metric("Clientes", f"{len(base):,}".replace(",", "."))
    c2.metric("Ativos", f"{(base['status']=='Ativo').sum():,}".replace(",", "."))
    c3.metric("Churn", f"{(base['status']=='Churn').sum():,}".replace(",", "."))

    st.dataframe(base, use_container_width=True)

    st.download_button(
        "Baixar base_clientes.csv",
        data=base.to_csv(index=False).encode("utf-8"),
        file_name="base_clientes.csv",
        mime="text/csv"
    )

st.divider()

mes_default = ref_date.strftime("%Y-%m")
mes = st.text_input("Mês do report (YYYY-MM)", value=mes_default)

rep = month_report(mes)
if rep.empty:
    st.info("Sem dados para esse mês.")
else:
    st.dataframe(rep, use_container_width=True)
    st.download_button(
        "Baixar report_mes.csv",
        data=rep.to_csv(index=False).encode("utf-8"),
        file_name=f"report_{mes}.csv",
        mime="text/csv"
    )
