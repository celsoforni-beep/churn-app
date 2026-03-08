import streamlit as st
import pandas as pd
import hashlib
import os
from datetime import date
import psycopg2
from psycopg2.extras import execute_values

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Sistema Clientes — Base 2025 por Marca",
    layout="wide"
)

SALT = os.getenv("HASH_SALT", "default_salt")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# =====================================================
# HASH
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
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado no Secrets.")
    return psycopg2.connect(DATABASE_URL)


def init_db():
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
        df = pd.read_excel(uploaded)
        return normalize_headers(df)

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

    seller_candidates = ["seller", "marca", "brand"]
    doc_candidates = ["client document", "client_document", "clientdocument", "document", "cpf", "documento"]
    email_candidates = ["email", "e-mail", "mail"]
    order_candidates = ["order", "order2", "order 2", "order_id", "orderid", "pedido", "pedido id", "pedidoid"]
    date_candidates = ["creation d", "creation date", "created at", "data", "data pedido", "data_pedido", "creationd"]
    value_candidates = ["total value", "totalvalue", "total", "valor", "valor total", "total_value"]

    def pick(candidates):
        for k in candidates:
            if k in cols:
                return cols[k]
        return None

    seller_col = pick(seller_candidates)
    doc_col = pick(doc_candidates)
    email_col = pick(email_candidates)
    order_col = pick(order_candidates)
    date_col = pick(date_candidates)
    value_col = pick(value_candidates)

    if seller_col is None:
        raise ValueError(f"Não achei coluna de marca. Colunas detectadas: {list(cols.keys())}")
    if order_col is None:
        raise ValueError(f"Não achei coluna de pedido. Colunas detectadas: {list(cols.keys())}")
    if date_col is None:
        raise ValueError(f"Não achei coluna de data. Colunas detectadas: {list(cols.keys())}")
    if (doc_col is None) and (email_col is None):
        raise ValueError("Não achei Client Document nem Email. Preciso de pelo menos 1 identificador.")

    base = df[[seller_col, order_col, date_col]].copy()
    base.columns = ["marca", "order_id", "data_pedido"]

    base["client_document"] = df[doc_col] if doc_col else ""
    base["email"] = df[email_col] if email_col else ""

    base["marca"] = base["marca"].astype(str).str.strip().str.upper()
    base["order_id"] = base["order_id"].astype(str).str.strip()

    base["data_pedido"] = pd.to_datetime(base["data_pedido"], dayfirst=True, errors="coerce")
    base = base.dropna(subset=["data_pedido", "order_id", "marca"])

    doc_clean = base["client_document"].apply(clean_document)
    email_clean = base["email"].apply(normalize_email)

    identificador = doc_clean.copy()
    mask_sem_doc = identificador.str.len() == 0
    identificador.loc[mask_sem_doc] = email_clean.loc[mask_sem_doc]

    base["customer_id"] = identificador.apply(lambda x: hash_id(str(x)))
    base["mes_compra"] = base["data_pedido"].dt.strftime("%Y-%m")

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

    return base[["customer_id", "marca", "order_id", "data_pedido", "mes_compra", "valor_pedido"]]

# =====================================================
# UPSERT POSTGRES
# =====================================================

def upsert_pedidos(df: pd.DataFrame) -> int:
    conn = get_conn()
    cur = conn.cursor()

    before = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]

    df2 = df.copy()
    df2["data_pedido"] = pd.to_datetime(df2["data_pedido"]).dt.date

    rows = list(
        df2[["customer_id", "marca", "order_id", "data_pedido", "mes_compra", "valor_pedido"]]
        .itertuples(index=False, name=None)
    )

    execute_values(
        cur,
        """
        insert into public.pedidos (customer_id, marca, order_id, data_pedido, mes_compra, valor_pedido)
        values %s
        on conflict (order_id) do update set
          customer_id = excluded.customer_id,
          marca = excluded.marca,
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

@st.cache_data(ttl=600)
def build_cliente_base(ref_date: date, ativo_dias=90, churn_dias=180) -> pd.DataFrame:
    conn = get_conn()

    q = """
    select
      customer_id,
      marca,
      min(data_pedido) as primeira_compra,
      max(data_pedido) as ultima_compra,
      count(distinct order_id) as qtd_pedidos,
      coalesce(sum(valor_pedido), 0) as receita_total
    from public.pedidos
    group by customer_id, marca
    """

    base = pd.read_sql(q, conn)
    conn.close()

    if base.empty:
        return pd.DataFrame()

    base["primeira_compra"] = pd.to_datetime(base["primeira_compra"])
    base["ultima_compra"] = pd.to_datetime(base["ultima_compra"])

    base["dias_sem_compra"] = (pd.to_datetime(ref_date) - base["ultima_compra"]).dt.days

    base["status"] = "Ativo"
    base.loc[base["dias_sem_compra"] > ativo_dias, "status"] = "Em risco"
    base.loc[base["dias_sem_compra"] > churn_dias, "status"] = "Churn"

    return base.sort_values(["marca", "ultima_compra"], ascending=[True, False])


@st.cache_data(ttl=600)
def resumo_por_marca(ref_date: date, ativo_dias=90, churn_dias=180) -> pd.DataFrame:
    base = build_cliente_base(ref_date, ativo_dias, churn_dias)

    if base.empty:
        return pd.DataFrame()

    resumo = (
        base.groupby("marca")
        .agg(
            clientes=("customer_id", "count"),
            ativos=("status", lambda x: (x == "Ativo").sum()),
            em_risco=("status", lambda x: (x == "Em risco").sum()),
            churn=("status", lambda x: (x == "Churn").sum()),
            receita_total=("receita_total", "sum")
        )
        .reset_index()
    )

    return resumo


# =====================================================
# APP
# =====================================================

try:
    init_db()
except Exception as e:
    st.error("Falha ao conectar no Supabase. Verifique DATABASE_URL no Secrets.")
    st.exception(e)
    st.stop()

st.title("📊 Sistema Clientes — Base 2025 por Marca")

st.sidebar.header("Configurações")
ref_date = st.sidebar.date_input("Data fechamento", value=date(2025, 12, 31))
ativo_dias = st.sidebar.number_input("Ativo (dias)", min_value=30, max_value=365, value=90, step=15)
churn_dias = st.sidebar.number_input("Churn (dias)", min_value=90, max_value=720, value=180, step=30)

uploaded = st.file_uploader("Upload base 2025 (Excel .xlsx ou CSV)", type=["xlsx", "csv"])

if uploaded is not None:
    try:
        df_raw = load_uploaded_file(uploaded)
        st.write("✅ Colunas detectadas:", list(df_raw.columns))

        df = normalize_input(df_raw)
        inserted = upsert_pedidos(df)

        build_cliente_base.clear()
        resumo_por_marca.clear()

        st.success(f"{inserted} pedidos adicionados")

    except Exception as e:
        st.error("Erro ao processar o arquivo.")
        st.exception(e)

st.divider()

resumo = resumo_por_marca(ref_date, ativo_dias, churn_dias)

if resumo.empty:
    st.info("Ainda não há dados. Faça upload das bases de 2025 por marca.")
else:
    st.subheader("Resumo por marca")
    st.dataframe(resumo, use_container_width=True)

    st.subheader("Base de clientes por marca")
    base = build_cliente_base(ref_date, ativo_dias, churn_dias)
    st.dataframe(base, use_container_width=True)

    st.download_button(
        "Baixar base_clientes_2025_por_marca.csv",
        data=base.to_csv(index=False).encode("utf-8"),
        file_name="base_clientes_2025_por_marca.csv",
        mime="text/csv"
    )
