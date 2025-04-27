from utils import generate_image_description, get_openai_client
import streamlit as st
from db import DB_PATH
from pathlib import Path
import base64
import datetime
import sqlite3


# === PROCESSADOR DE IMAGENS ===
def process_image_to_chunks(file_path):
    """Processa arquivos de imagem (JPG, PNG)"""
    file_name = Path(file_path).name
    
    # Verifica se temos um cliente OpenAI configurado
    client = get_openai_client()
    if client is None:
        st.error("""🔒 API da OpenAI não configurada. 
                Por favor, vá para 'Configurações' e insira sua chave API para gerar descrições de imagens.""")
        return False
    
    try:
        with open(file_path, "rb") as f:
            image_bytes = f.read()
        
        # Gera descrição da imagem
        with st.spinner("Gerando descrição da imagem..."):
            image_description = generate_image_description(image_bytes)
        
        if image_description is None:
            st.warning("Não foi possível gerar uma descrição para a imagem")
            image_description = "Descrição não disponível"
        
        # Codifica a imagem em base64
        b64_image = base64.b64encode(image_bytes).decode('ascii')
        
        # Prepara o conteúdo para o banco de dados
        image_meta = f"**Arquivo**: {file_name}\n**Tipo**: Imagem\n**Descrição**: {image_description}"
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO chunks (file_name, page_number, chunk_number, content, chunk_content, upload_time, image_description, image_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (file_name, 1, 1, image_meta, "imagem", datetime.now().strftime("%d/%m/%Y"), image_description, b64_image))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao processar imagem: {str(e)}")
        return False