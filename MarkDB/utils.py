import base64
import streamlit as st
from openai import OpenAI


# === FUNÇÕES AUXILIARES ===
def get_openai_client():
    """Retorna o cliente OpenAI configurado"""
    # Primeiro verifica se há uma chave no session_state (inserida pelo usuário)
    if hasattr(st.session_state, 'openai_api_key') and st.session_state.openai_api_key:
        return OpenAI(api_key=st.session_state.openai_api_key)
    # Depois verifica secrets.toml (para deploy)
    try:
        if 'openai_api_key' in st.secrets:
            return OpenAI(api_key=st.secrets.openai_api_key)
    except:
        pass
    return None

def generate_image_description(image_bytes):
    """Gera descrição de imagem usando a API da OpenAI"""
    client = get_openai_client()
    if client is None:
        st.error("API key da OpenAI não configurada")
        return None
    
    try:
        b64_image = base64.b64encode(image_bytes).decode('utf-8')
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Descreva esta imagem detalhadamente em português."},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{b64_image}"
                            },
                        },
                    ],
                }
            ],
            max_tokens=500,
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Erro ao gerar descrição: {str(e)}")
        return None

def detect_chunk_type(chunk):
    """Detecta o tipo de conteúdo do chunk"""
    if "```" in chunk:  # Se contém blocos de código
        return "código"
    elif any(char.isdigit() for char in chunk) and sum(c.isalpha() for c in chunk) / len(chunk) < 0.5:
        return "dados numéricos"
    elif len(chunk.split('\n')) > 5 and all(len(line) < 100 for line in chunk.split('\n')):
        return "lista"
    else:
        return "texto"

def format_table(matrix):
    """Formata uma matriz como tabela Markdown"""
    if not matrix or len(matrix) < 2:
        return ""
    
    # Cabeçalho
    markdown = "| " + " | ".join(str(cell) for cell in matrix[0]) + " |\n"
    markdown += "| " + " | ".join(["---"] * len(matrix[0])) + " |\n"
    
    # Linhas
    for row in matrix[1:]:
        markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
    
    return markdown

