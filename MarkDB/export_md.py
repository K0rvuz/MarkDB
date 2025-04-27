import sqlite3
from db import DB_PATH





# === EXPORTAÇÃO PARA MD ===
def export_chunk_to_md(chunk_id):
    """Exporta um chunk específico para um arquivo Markdown"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM chunks WHERE id = ?", (chunk_id,))
    row = c.fetchone()
    conn.close()
    
    if not row:
        return None, None
    
    columns = ['id', 'file_name', 'page_number', 'chunk_number', 'content', 
              'chunk_content', 'upload_time', 'image_description', 'image_data']
    chunk_data = dict(zip(columns, row))
    
    # Constrói o conteúdo Markdown
    md_content = f"# {chunk_data['file_name']}\n\n"
    md_content += f"**ID:** {chunk_data['id']}\n"
    md_content += f"**Página:** {chunk_data['page_number']}\n"
    md_content += f"**Chunk:** {chunk_data['chunk_number']}\n"
    md_content += f"**Tipo:** {chunk_data['chunk_content']}\n"
    md_content += f"**Data de upload:** {chunk_data['upload_time']}\n\n"
    
    if chunk_data['chunk_content'] == "imagem":
        md_content += "## Descrição da Imagem\n"
        md_content += f"{chunk_data['image_description']}\n\n"
        md_content += f"![Imagem](data:image/png;base64,{chunk_data['image_data']})"
    else:
        md_content += chunk_data['content']
    
    return md_content, f"chunk_{chunk_id}_{chunk_data['file_name']}.md"


def export_all_chunks_to_md(file_name):
    """Exporta todos os chunks de um arquivo específico para um único arquivo Markdown"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Ordenar por número da página e número do chunk para manter a sequência lógica
    c.execute("""
        SELECT * FROM chunks 
        WHERE file_name = ? 
        ORDER BY page_number, chunk_number
    """, (file_name,))
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        return None, None

    columns = ['id', 'file_name', 'page_number', 'chunk_number', 'content', 
               'chunk_content', 'upload_time', 'image_description', 'image_data']
    
    md_parts = []  # Usamos lista para juntar tudo no final (muito mais rápido que +=)

    md_parts.append(f"# {file_name}\n\n")

    for row in rows:
        chunk_data = dict(zip(columns, row))

        md_parts.append(f"---\n")
        md_parts.append(f"**ID:** {chunk_data['id']}\n\n")
        md_parts.append(f"**Nome do Arquivo:** {chunk_data['file_name']}\n\n")
        md_parts.append(f"**Página:** {chunk_data['page_number']}\n\n")
        md_parts.append(f"**Chunk:** {chunk_data['chunk_number']}\n\n")
        md_parts.append(f"**Tipo:** {chunk_data['chunk_content']}\n\n")
        md_parts.append(f"**Data de upload:** {chunk_data['upload_time']}\n\n")

        if chunk_data['chunk_content'] == "imagem":
            md_parts.append("## Descrição da Imagem\n\n")
            md_parts.append(f"{chunk_data['image_description']}\n\n")
            if chunk_data['image_data']:
                md_parts.append(f"![Imagem](data:image/png;base64,{chunk_data['image_data']})\n\n")
        else:
            md_parts.append("## Conteúdo\n\n")
            md_parts.append(f"{chunk_data['content']}\n\n")
    
    md_content = "".join(md_parts)  # Junta tudo de uma vez só
    md_filename = f"documento_completo_{file_name}.md"
    
    return md_content, md_filename
