import streamlit as st
import google.generativeai as genai
import sqlite3
import uuid
import pdfplumber

PDF_PATH = "./base.pdf"

# Função para extrair texto de um arquivo PDF
def extract_text_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    return text

# Função para conectar ao banco de dados SQLite
def get_db_connection():
    conn = sqlite3.connect('chatbot.db')
    conn.row_factory = sqlite3.Row
    return conn

# Função para criar a tabela de conversas (se não existir)
def create_conversations_table():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS conversations (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            role TEXT,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Função para salvar uma mensagem no banco de dados
def save_message(user_id, role, message):
    conn = get_db_connection()
    conn.execute('''
        INSERT INTO conversations (id, user_id, role, message)
        VALUES (?, ?, ?, ?)
    ''', (str(uuid.uuid4()), user_id, role, message))
    conn.commit()
    conn.close()

# Função para carregar o histórico de conversas de um usuário
def load_conversation(user_id):
    conn = get_db_connection()
    cursor = conn.execute('''
        SELECT role, message FROM conversations
        WHERE user_id = ?
        ORDER BY timestamp ASC
    ''', (user_id,))
    conversation = cursor.fetchall()
    conn.close()
    return conversation

# Função para gerar uma resposta usando o Gemini
def generate_gemini_response(prompt, context, api_key):
    try:
        genai.configure(api_key=api_key)  # Configura a API Key
        model = genai.GenerativeModel('gemini-pro')  # Usando o modelo Gemini Pro

        # Combina o contexto (texto do PDF) com a pergunta do usuário
        full_prompt = f"Com base no seguinte contexto, responda à pergunta:\n\nContexto:\n{context}\n\nPergunta:\n{prompt}"
        response = model.generate_content(full_prompt)
        return response.text
    except Exception as e:
        return f"Erro ao gerar resposta: {str(e)}"

# Interface do Streamlit
st.title("Chatbot Integrado com Gemini")

# Campo para o usuário inserir a API Key
api_key = st.text_input("Insira sua API Key do Gemini:", type="password")

# Extrai o texto do PDF (no backend)
pdf_text = extract_text_from_pdf(PDF_PATH)
st.success(f"PDF '{PDF_PATH}' carregado com sucesso no backend!")

# Criar a tabela de conversas (se não existir)
create_conversations_table()

# Identificar o usuário (usando um ID único)
if 'user_id' not in st.session_state:
    st.session_state.user_id = str(uuid.uuid4())  # Gera um ID único para o usuário

# Carregar o histórico de conversas do usuário
conversation_history = load_conversation(st.session_state.user_id)

# Exibir o histórico de conversas
for message in conversation_history:
    st.write(f"{message['role']}: {message['message']}")

# Entrada do usuário
user_input = st.text_input("Você: ", "")

# Botão para enviar a mensagem
if st.button("Enviar"):
    if user_input:
        if not api_key:
            st.error("Por favor, insira uma API Key válida.")
        else:
            # Salvar a mensagem do usuário no banco de dados
            save_message(st.session_state.user_id, "Cliente", user_input)

            # Gerar uma resposta usando o Gemini
            response = generate_gemini_response(user_input, pdf_text, api_key)

            # Salvar a resposta do chatbot no banco de dados
            save_message(st.session_state.user_id, "PoppiBot", response)

            # Recarregar a página para exibir o histórico atualizado
            st.rerun()