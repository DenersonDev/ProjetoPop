import sqlite3
import PyPDF2
import os
from datetime import datetime
import re
from typing import List, Tuple, Dict

class PDFDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.setup_database()

    def setup_database(self):
        """Configura o banco de dados com FTS e índices"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Criar tabela de metadados com índices
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                total_pages INTEGER,
                creation_date TEXT,
                import_date TEXT
            )
        ''')
        
        # Criar índices para metadados
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_name ON pdf_metadata(file_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_creation_date ON pdf_metadata(creation_date)')

        # Criar tabela FTS para conteúdo do PDF
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS pdf_content_fts USING fts5(
                pdf_id,
                page_number,
                content,
                tokenize='porter unicode61'
            )
        ''')

        # Tabela para armazenar estatísticas de palavras
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS word_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pdf_id INTEGER,
                word TEXT,
                frequency INTEGER,
                FOREIGN KEY (pdf_id) REFERENCES pdf_metadata(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_word_stats ON word_stats(word, pdf_id)')
        
        conn.commit()
        conn.close()

    def import_pdf(self, pdf_path: str) -> Tuple[bool, str]:
        """Importa um PDF para o banco de dados com processamento avançado"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                total_pages = len(pdf_reader.pages)

                # Inserir metadados
                cursor.execute('''
                    INSERT INTO pdf_metadata (file_name, total_pages, creation_date, import_date)
                    VALUES (?, ?, ?, ?)
                ''', (
                    os.path.basename(pdf_path),
                    total_pages,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                pdf_id = cursor.lastrowid

                # Processar cada página
                word_frequencies = {}
                for page_num in range(total_pages):
                    page = pdf_reader.pages[page_num]
                    content = page.extract_text()

                    # Inserir na tabela FTS
                    cursor.execute('''
                        INSERT INTO pdf_content_fts (pdf_id, page_number, content)
                        VALUES (?, ?, ?)
                    ''', (pdf_id, page_num + 1, content))

                    # Contar frequência das palavras
                    words = re.findall(r'\w+', content.lower())
                    for word in words:
                        if len(word) > 2:  # Ignorar palavras muito curtas
                            word_frequencies[word] = word_frequencies.get(word, 0) + 1

                # Armazenar estatísticas de palavras
                for word, freq in word_frequencies.items():
                    cursor.execute('''
                        INSERT INTO word_stats (pdf_id, word, frequency)
                        VALUES (?, ?, ?)
                    ''', (pdf_id, word, freq))

            conn.commit()
            conn.close()
            return True, "PDF importado com sucesso!"

        except Exception as e:
            return False, f"Erro ao processar o PDF: {str(e)}"

    def parse_query(self, query: str) -> List[str]:
        """
        Quebra a frase em palavras-chave, removendo stopwords e palavras muito curtas
        
        Stopwords em português para serem removidas
        """
        stopwords = {
            'de', 'da', 'do', 'das', 'dos', 
            'a', 'o', 'as', 'os', 
            'em', 'no', 'na', 'nos', 'nas', 
            'um', 'uma', 'uns', 'umas', 
            'e', 'ou', 'mas', 'porém', 'contudo', 
            'que', 'se', 'para'
        }
        
        # Converter para minúsculas e remover pontuação
        query = re.sub(r'[^\w\s]', '', query.lower())
        
        # Separar palavras
        words = query.split()
        
        # Filtrar stopwords e palavras muito curtas
        keywords = [word for word in words if word not in stopwords and len(word) > 2]
        
        return keywords

    def search(self, query: str, options: Dict = None) -> List[Dict]:
        """
        Realiza busca avançada com várias opções
        
        Args:
            query: Frase ou termo de busca
            options: Dicionário com opções de busca
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        options = options or {}
        
        # Parse da query para extrair palavras-chave
        keywords = self.parse_query(query)
        
        # Preparar consulta de busca
        if not keywords:
            return []
        
        # Preparar consulta FTS usando OR entre palavras-chave
        base_query = '''
            WITH filtered_results AS (
                SELECT 
                    m.id as pdf_id,
                    m.file_name,
                    c.page_number,
                    c.content,
                    m.creation_date,
                    (
                        SELECT GROUP_CONCAT(word || ':' || frequency)
                        FROM word_stats w
                        WHERE w.pdf_id = m.id
                        ORDER BY frequency DESC
                        LIMIT 5
                    ) as top_words,
                    (
                        SELECT COUNT(*) 
                        FROM word_stats w 
                        WHERE w.pdf_id = m.id AND w.word IN ({})
                    ) as keyword_matches,
                    rank
                FROM pdf_content_fts c
                JOIN pdf_metadata m ON c.pdf_id = m.id
                WHERE pdf_content_fts MATCH ?
            )
            SELECT 
                pdf_id, file_name, page_number, 
                content, creation_date, top_words, 
                keyword_matches, rank
            FROM filtered_results
        '''.format(','.join(['?'] * len(keywords)))
        
        # Preparar parâmetros da consulta
        # Usar formato de busca FTS5: "palavra1 palavra2 palavra3"
        fts_query = ' '.join(keywords)
        
        params = [fts_query] + keywords
        
        # Adicionar opções de filtragem
        if options.get('page_range'):
            min_page, max_page = options['page_range']
            base_query += ' WHERE page_number BETWEEN ? AND ?'
            params.extend([min_page, max_page])

        if options.get('date_range'):
            start_date, end_date = options['date_range']
            base_query += (' AND ' if 'WHERE' in base_query else ' WHERE ') + \
                          'creation_date BETWEEN ? AND ?'
            params.extend([start_date, end_date])

        # Ordenar por relevância e número de correspondências
        base_query += ' ORDER BY keyword_matches DESC, rank'

        # Executar consulta
        try:
            cursor.execute(base_query, params)
            results = cursor.fetchall()
        except sqlite3.OperationalError as e:
            print(f"Erro na consulta: {e}")
            print(f"Consulta FTS: {fts_query}")
            print(f"Parâmetros: {params}")
            return []
        
        # Formatar resultados
        formatted_results = []
        for row in results:
            pdf_id, file_name, page_number, content, creation_date, top_words, keyword_matches, rank = row
            
            # Processar top_words
            word_stats = {}
            if top_words:
                for word_freq in top_words.split(','):
                    word, freq = word_freq.split(':')
                    word_stats[word] = int(freq)

            # Destacar palavras-chave no conteúdo
            highlighted_content = content
            for keyword in keywords:
                highlighted_content = re.sub(
                    r'\b{}\b'.format(keyword), 
                    f'**{keyword}**', 
                    highlighted_content, 
                    flags=re.IGNORECASE
                )

            formatted_results.append({
                'pdf_id': pdf_id,
                'file_name': file_name,
                'page_number': page_number,
                'content': highlighted_content,
                'creation_date': creation_date,
                'top_words': word_stats,
                'keyword_matches': keyword_matches,
                'rank': rank
            })

        conn.close()
        return formatted_results

    def get_statistics(self, pdf_id: int = None) -> Dict:
        """Retorna estatísticas do documento"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {}
        if pdf_id:
            cursor.execute('''
                SELECT 
                    m.file_name,
                    m.total_pages,
                    COUNT(DISTINCT w.word) as unique_words,
                    SUM(w.frequency) as total_words,
                    AVG(w.frequency) as avg_word_frequency
                FROM pdf_metadata m
                LEFT JOIN word_stats w ON m.id = w.pdf_id
                WHERE m.id = ?
                GROUP BY m.id
            ''', (pdf_id,))
        else:
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT m.id) as total_documents,
                    SUM(m.total_pages) as total_pages,
                    COUNT(DISTINCT w.word) as unique_words,
                    SUM(w.frequency) as total_words,
                    AVG(w.frequency) as avg_word_frequency
                FROM pdf_metadata m
                LEFT JOIN word_stats w ON m.id = w.pdf_id
            ''')

        row = cursor.fetchone()
        if row:
            stats = {
                'file_name': row[0] if pdf_id else None,
                'total_documents': 1 if pdf_id else row[0],
                'total_pages': row[1],
                'unique_words': row[2],
                'total_words': row[3],
                'avg_word_frequency': round(row[4], 2) if row[4] else 0
            }

        conn.close()
        return stats