import sqlite3
import hashlib
from typing import Dict, List, Optional, Union
import pandas as pd
import streamlit as st
from datetime import datetime
from utils import PDFProcessor

class Table:
    def __init__(self, name: str, columns: list):
        self.name = name
        self.columns = columns
        self.data = pd.DataFrame(columns=columns)

    def insert(self, row: dict):
        """Insere dados como DataFrame pandas para performance"""
        self.data.loc[len(self.data)] = row

    def search(self, keyword: str) -> List[Dict]:
        """Busca por keyword com pandas.STR.contains()"""
        if 'text' not in self.columns:
            return []
        return self.data[self.data['text'].str.contains(keyword, case=False)].to_dict('records')

class MarkDB:
    def __init__(self, cache_enabled: bool = True, db_path: str = "markdb.sqlite"):
        self.tables = {}
        self.cache_enabled = cache_enabled
        self.DB_PATH = db_path
        self._init_db()
        
        # Configuração padrão
        if 'CHUNK_SIZE' not in st.session_state:
            st.session_state.CHUNK_SIZE = 800

    def _init_db(self):
        """Inicializa o banco de dados SQLite"""
        with sqlite3.connect(self.DB_PATH) as conn:
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

    def create_table(self, name: str, columns: list):
        """Cria uma nova tabela na estrutura MarkDB"""
        self.tables[name] = Table(name, columns)

    def add_document(self, pdf_path: str, metadata: Optional[Dict] = None) -> str:
        """Adiciona documento ao banco com metadados"""
        if 'documents' not in self.tables:
            self.create_table('documents', ['id', 'path', 'metadata', 'text'])

        text = PDFProcessor.pdf_to_text(pdf_path, self.cache_enabled)
        doc_id = hashlib.md5(pdf_path.encode()).hexdigest()

        self.tables['documents'].insert({
            'id': doc_id,
            'path': pdf_path,
            'metadata': metadata or {},
            'text': text
        })
        return doc_id

    def add_chunk(self, file_name: str, page_number: int, chunk_number: int, 
                 content: str, chunk_content: str, 
                 image_description: Optional[str] = None, 
                 image_data: Optional[bytes] = None) -> int:
        """Adiciona um chunk de texto ao banco SQLite"""
        with sqlite3.connect(self.DB_PATH) as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO chunks (
                    file_name, page_number, chunk_number, 
                    content, chunk_content, upload_time,
                    image_description, image_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_name, page_number, chunk_number,
                content, chunk_content, datetime.now().isoformat(),
                image_description, image_data
            ))
            conn.commit()
            return c.lastrowid

    def search_chunks(self, keyword: str, limit: int = 10) -> List[Dict]:
        """Busca chunks no banco SQLite"""
        with sqlite3.connect(self.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute('''
                SELECT * FROM chunks 
                WHERE chunk_content LIKE ? 
                LIMIT ?
            ''', (f'%{keyword}%', limit))
            return [dict(row) for row in c.fetchall()]

    def get_all_chunks(self, file_name: Optional[str] = None) -> List[Dict]:
        """Recupera todos os chunks ou filtrados por arquivo"""
        with sqlite3.connect(self.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            if file_name:
                c.execute('SELECT * FROM chunks WHERE file_name = ?', (file_name,))
            else:
                c.execute('SELECT * FROM chunks')
                
            return [dict(row) for row in c.fetchall()]
