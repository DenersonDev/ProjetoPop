import pdfplumber

def extract_and_save_text(pdf_path, output_txt_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"  # Adiciona uma quebra de linha entre páginas
    
    # Salva o texto em um arquivo .txt
    with open(output_txt_path, "w", encoding="utf-8") as file:
        file.write(text)
    print(f"Texto salvo em {output_txt_path}")

# Caminho do PDF e arquivo de saída
PDF_PATH = "./base.pdf"  # Substitua pelo caminho do seu PDF
OUTPUT_TXT_PATH = "base.txt"

# Extrai e salva o texto
extract_and_save_text(PDF_PATH, OUTPUT_TXT_PATH)