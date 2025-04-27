import sqlite3
import os
import base64
import streamlit as st
import pandas as pd
from io import BytesIO
from db import DB_PATH, init_db
from process_img import process_image_to_chunks
from process_docx import process_docx_to_chunks
from process_pdf import process_pdf_to_chunks
from process_xlsx import process_xlsx_to_chunks
from export_md import export_all_chunks_to_md, export_chunk_to_md



# === INTERFACE STREAMLIT ===
def main():
    st.set_page_config(page_title="MarkDB", page_icon="📄", layout="wide")
    st.title("📄 MarkDB - Banco de Dados Markdown")
    
    init_db()
    
    # Configuração inicial da API Key
    if 'openai_api_key' not in st.session_state:
        st.session_state.openai_api_key = ""
    
    menu = st.sidebar.selectbox("Navegar", ["Upload de Arquivo", "Banco de Dados", "Configurações"])

    if menu == "Upload de Arquivo":
        st.header("📤 Upload de Arquivo")
        uploaded_file = st.file_uploader("Escolha um arquivo (.pdf, .docx, .xlsx, .jpg, .png)", 
                                       type=["pdf", "docx", "xlsx", "jpg", "jpeg", "png"])
        
        if uploaded_file is not None:
            file_name = uploaded_file.name
            file_ext = file_name.split('.')[-1].lower()
            temp_dir = "temp_upload"
            os.makedirs(temp_dir, exist_ok=True)
            file_path = os.path.join(temp_dir, file_name)
            
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            if file_ext in ["jpg", "jpeg", "png"]:
                success = process_image_to_chunks(file_path)
                if success:
                    st.success("✅ Imagem processada com sucesso!")
                else:
                    st.error("❌ Falha ao processar a imagem")
            elif file_ext == "pdf":
                with st.spinner("Processando PDF..."):
                    process_pdf_to_chunks(file_path)
                    st.success("✅ PDF processado com sucesso!")
            elif file_ext == "docx":
                with st.spinner("Processando documento Word..."):
                    process_docx_to_chunks(file_path)
                    st.success("✅ Documento Word processado com sucesso!")
            elif file_ext == "xlsx":
                with st.spinner("Processando planilha Excel..."):
                    process_xlsx_to_chunks(file_path)
                    st.success("✅ Planilha Excel processada com sucesso!")
            
            if os.path.exists(file_path):
                os.remove(file_path)

    elif menu == "Banco de Dados":
        st.header("🔎 Banco de Dados")
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM chunks", conn)
        conn.close()

        if df.empty:
            st.info("ℹ️ O banco de dados está vazio. Faça o upload de um arquivo para começar.")
        else:
            st.write("### 📊 Conteúdo do Banco de Dados")
            
            # Filtros
            col1, col2, col3 = st.columns(3)
            with col1:
                filter_file = st.selectbox("Filtrar por arquivo:", ["Todos"] + list(df['file_name'].unique()))
            with col2:
                filter_type = st.selectbox("Filtrar por tipo:", ["Todos"] + list(df['chunk_content'].unique()))
            with col3:
                items_per_page = st.selectbox("Itens por página:", [10, 25, 50, 100])

            # Aplicar filtros
            filtered_df = df.copy()
            if filter_file != "Todos":
                filtered_df = filtered_df[filtered_df['file_name'] == filter_file]
            if filter_type != "Todos":
                filtered_df = filtered_df[filtered_df['chunk_content'] == filter_type]

            # Paginação
            total_pages = max(1, (len(filtered_df) // items_per_page) + 1)
            page_number = st.number_input("Página:", min_value=1, max_value=total_pages, value=1)
            start_idx = (page_number - 1) * items_per_page
            end_idx = start_idx + items_per_page

            # Exibir dados filtrados
            st.dataframe(filtered_df.iloc[start_idx:end_idx], height=400, use_container_width=True)

            # --- Exportar Documento Completo ---
            st.write("### 📥 Exportar Documento Completo para Markdown")

            # Selecionar arquivo para exportar
            export_file_name = st.selectbox("Selecione o arquivo para exportação:", [""] + list(df['file_name'].unique()), key="export_selectbox")

            if export_file_name:
                if st.button("Exportar Documento Completo", key="export_button"):
                    md_content, md_filename = export_all_chunks_to_md(export_file_name)
        
                    if md_content:
                        st.success(f"✅ Documento '{md_filename}' gerado com sucesso!")
                        st.download_button(
                            label="⬇️ Baixar Documento Markdown Completo",
                            data=md_content,
                            file_name=md_filename,
                            mime="text/markdown",
                            key="download_full_md"
                        )
                    else:
                        st.error("❌ Não foi possível exportar o documento.")


            # Visualização detalhada
            st.write("### 🔍 Visualização Detalhada")
            chunk_id = st.number_input("ID do chunk para visualizar:", min_value=1, max_value=df['id'].max(), value=1)
            
            if st.button("Visualizar Chunk"):
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT * FROM chunks WHERE id = ?", (chunk_id,))
                row = c.fetchone()
                conn.close()
                
                if row:
                    columns = ['id', 'file_name', 'page_number', 'chunk_number', 'content', 
                              'chunk_content', 'upload_time', 'image_description', 'image_data']
                    chunk_data = dict(zip(columns, row))
                    
                  
                    
                    st.write("#### Conteúdo")
                    if chunk_data['chunk_content'] == "imagem":
                        try:
                            image_bytes = base64.b64decode(chunk_data['image_data'])
                            st.image(BytesIO(image_bytes), caption=chunk_data['file_name'], use_container_width=True)
                            st.write(chunk_data['content'])
                            
                        except Exception as e:
                            st.error(f"Erro ao exibir imagem: {str(e)}")
                    else:
                        st.markdown(chunk_data['content'])
                    
                    # Exportação para MD
                    md_content, md_filename = export_chunk_to_md(chunk_id)
                    st.download_button(
                        label="⬇️ Baixar como Markdown",
                        data=md_content,
                        file_name=md_filename,
                        mime="text/markdown"
                    )
                else:
                    st.warning("Chunk não encontrado")

    elif menu == "Configurações":
        st.header("⚙️ Configurações")
        
        # Seção de configuração da API Key
        st.subheader("Configuração da API OpenAI")
        
        # Explica como configurar
        st.info("""
        Para usar recursos de IA, você precisa de uma chave API da OpenAI. 
        Você pode obtê-la em: https://platform.openai.com/api-keys
        """)
        
        # Campo para inserir a chave
        api_key = st.text_input("Chave da API OpenAI:", 
                              value=st.session_state.get('openai_api_key', ''), 
                              type="password",
                              help="Insira sua chave API da OpenAI aqui")
        
        if st.button("Salvar Chave API"):
            if api_key:
                st.session_state.openai_api_key = api_key
                st.success("✅ Chave API salva com sucesso!")
            else:
                st.warning("Por favor, insira uma chave API válida")
                
        # Seção de outras configurações
        st.subheader("Outras Configurações")
        new_chunk_size = st.number_input("Tamanho do chunk (caracteres):", 
                                       min_value=100, max_value=2000, 
                                       value=st.session_state.CHUNK_SIZE)
        
        if st.button("Atualizar Tamanho do Chunk"):
            st.session_state.CHUNK_SIZE = new_chunk_size
            st.success(f"✅ Tamanho do chunk atualizado para {st.session_state.CHUNK_SIZE} caracteres")

if __name__ == "__main__":
    main()