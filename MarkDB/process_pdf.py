import sqlite3
import datetime
import fitz
from db import DB_PATH
from pathlib import Path
import base64
from utils import detect_chunk_type, format_table, generate_image_description, get_openai_client
import textwrap
import streamlit as st
from concurrent.futures import ThreadPoolExecutor

# === PROCESSADOR DE PDF===
def process_pdf_to_chunks(file_path):
    """Processa arquivos PDF e extrai texto, tabelas e imagens de forma otimizada"""
    doc = fitz.open(file_path)
    file_name = Path(file_path).name
    upload_time = datetime.datetime.now().strftime("%d/%m/%Y")
    
    # Conexão única com o banco de dados
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Pré-criar a tabela se não existir 
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
    
    # Desativa temporariamente as verificações de integridade para inserts mais rápidos
    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA journal_mode = MEMORY")
    
    # Processa cada página em paralelo (quando possível)
    def process_page(page_index):
        page = doc.load_page(page_index)
        chunks_to_insert = []
        chunk_counter = 1
        
        # Processa texto
        text = page.get_text("text")
        if text.strip():
            chunks = textwrap.wrap(text, st.session_state.CHUNK_SIZE)
            for chunk in chunks:
                chunk_type = detect_chunk_type(chunk)
                markdown_chunk = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n\n{chunk.strip()}"
                chunks_to_insert.append((
                    file_name, page_index + 1, chunk_counter, markdown_chunk, 
                    chunk_type, upload_time, None, None
                ))
                chunk_counter += 1
        
        # Processa tabelas
        tables = page.find_tables()
        if tables:
            for table in tables:
                matrix = table.extract()
                markdown_table = format_table(matrix)
                if markdown_table:
                    markdown_chunk = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n\n{markdown_table}"
                    chunks_to_insert.append((
                        file_name, page_index + 1, chunk_counter, markdown_chunk,
                        "tabela", upload_time, None, None
                    ))
                    chunk_counter += 1
        
        # Processa imagens (opcional - pode ser desativado para maior velocidade)
        if st.session_state.get('PROCESS_IMAGES', True):
            images = page.get_images(full=True)
            for img in images:
                xref = img[0]
                try:
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    # Gera descrição apenas se a API estiver configurada
                    client = get_openai_client()
                    image_description = "Descrição não disponível"
                    if client is not None:
                        try:
                            image_description = generate_image_description(image_bytes) or image_description
                        except:
                            pass
                    
                    b64_image = base64.b64encode(image_bytes).decode('ascii')
                    image_meta = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n**Descrição**: {image_description}"
                    
                    chunks_to_insert.append((
                        file_name, page_index + 1, chunk_counter, image_meta,
                        "imagem", upload_time, image_description, b64_image
                    ))
                    chunk_counter += 1
                except Exception as e:
                    print(f"Erro ao processar imagem: {str(e)}")
                    continue
        
        # Insere todos os chunks da página de uma vez
        if chunks_to_insert:
            c.executemany('''
                INSERT INTO chunks (file_name, page_number, chunk_number, content, 
                chunk_content, upload_time, image_description, image_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', chunks_to_insert)
    
    # ThreadPool para páginas grandes (ajustar conforme necessidade)
    if len(doc) > 10:  # Usar paralelismo apenas para documentos grandes
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(process_page, range(len(doc)))
    else:
        for page_index in range(len(doc)):
            process_page(page_index)
    
    # Restaura configurações do SQLite e commita
    conn.commit()
    c.execute("PRAGMA synchronous = NORMAL")
    conn.close()
