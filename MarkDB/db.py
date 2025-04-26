import sqlite3
import streamlit as st

# === CONFIGURAÇÕES ===
if 'CHUNK_SIZE' not in st.session_state:
    st.session_state.CHUNK_SIZE = 800  # Tamanho padrão dos chunks de texto
DB_PATH = "markdb.sqlite"  # Caminho do banco de dados

# === INICIALIZAÇÃO DO BANCO DE DADOS ===
def init_db():
    """Inicializa o banco de dados SQLite com a estrutura necessária"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            page_number INTEGER,
            chunk_number INTEGER,
            content TEXT,
            chunk_content TEXT,
            upload_time TEXT,
            image_description TEXT,
            image_data TEXT
        )
    ''')
    conn.commit()
    conn.close()