import streamlit as st
from pathlib import Path
import datetime
import docx
import sqlite3
from db import DB_PATH
import textwrap
from utils import detect_chunk_type


# === PROCESSADOR DE DOCX ===
def process_docx_to_chunks(file_path):
    """Processa arquivos DOCX (Word)"""
    file_name = Path(file_path).name
    upload_time = datetime.now().strftime("%d/%m/%Y")
    
    try:
        doc = docx.Document(file_path)
        full_text = []
        
        for para in doc.paragraphs:
            full_text.append(para.text)
        
        text = '\n'.join(full_text)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if text.strip():
            chunks = textwrap.wrap(text, st.session_state.CHUNK_SIZE)  # Usando session_state
            for i, chunk in enumerate(chunks, 1):
                chunk_type = detect_chunk_type(chunk)
                markdown_chunk = f"**Arquivo**: {file_name}\n**Chunk**: {i}\n\n{chunk.strip()}"
                c.execute('''
                    INSERT INTO chunks (file_name, page_number, chunk_number, content, chunk_content, upload_time, image_description, image_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_name, 1, i, markdown_chunk, chunk_type, upload_time, None, None))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao processar documento Word: {str(e)}")
        return False