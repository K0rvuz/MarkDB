import sqlite3
import datetime
import fitz
from db import DB_PATH
from pathlib import Path
import base64
from utils import detect_chunk_type, format_table, generate_image_description,get_openai_client
import textwrap
import streamlit as st




# === PROCESSADOR DE PDF ===
def process_pdf_to_chunks(file_path):
    """Processa arquivos PDF e extrai texto, tabelas e imagens"""
    doc = fitz.open(file_path)
    file_name = Path(file_path).name
    upload_time = datetime.now().strftime("%d/%m/%Y")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for page_index in range(len(doc)):
        page = doc.load_page(page_index)
        text = page.get_text("text")
        tables = page.find_tables()
        images = page.get_images(full=True)
        chunk_counter = 1

        # Processa texto
        if text.strip():
            chunks = textwrap.wrap(text, st.session_state.CHUNK_SIZE)
            for chunk in chunks:
                chunk_type = detect_chunk_type(chunk)
                markdown_chunk = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n\n{chunk.strip()}"
                c.execute('''
                    INSERT INTO chunks (file_name, page_number, chunk_number, content, chunk_content, upload_time, image_description, image_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_name, page_index + 1, chunk_counter, markdown_chunk, chunk_type, upload_time, None, None))
                chunk_counter += 1

        # Processa tabelas
        if tables:
            for table in tables:
                matrix = table.extract()
                markdown_table = format_table(matrix)
                if markdown_table:
                    markdown_chunk = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n\n{markdown_table}"
                    c.execute('''
                        INSERT INTO chunks (file_name, page_number, chunk_number, content, chunk_content, upload_time, image_description, image_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (file_name, page_index + 1, chunk_counter, markdown_chunk, "tabela", upload_time, None, None))
                    chunk_counter += 1

        # Processa imagens
        for img in images:
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            
            try:
                # Verifica se temos cliente OpenAI para gerar descrição
                client = get_openai_client()
                if client is None:
                    image_description = "Descrição não disponível (API não configurada)"
                else:
                    # Gera descrição da imagem
                    image_description = generate_image_description(image_bytes)
                    if image_description is None:
                        image_description = "Descrição não disponível"
                
                # Codifica a imagem em base64
                b64_image = base64.b64encode(image_bytes).decode('ascii')
                
                # Armazena metadados e dados da imagem
                image_meta = f"**Arquivo**: {file_name}\n**Página**: {page_index + 1}\n**Chunk**: {chunk_counter}\n**Descrição**: {image_description}"
                
                c.execute('''
                    INSERT INTO chunks (file_name, page_number, chunk_number, content, chunk_content, upload_time, image_description, image_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_name, page_index + 1, chunk_counter, image_meta, "imagem", upload_time, image_description, b64_image))
                chunk_counter += 1
                
            except Exception as e:
                st.error(f"Erro ao processar imagem: {str(e)}")
                continue

    conn.commit()
    conn.close()