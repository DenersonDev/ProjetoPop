from pdf_to_bq import *
# Criar instância do banco de dados
db = PDFDatabase("pdfs.db")

# Importar PDF
# db.import_pdf("base.pdf")

# Busca com frase completa
def get_contexto(search)
    try:
        resultados = db.search(search)
        
        # Mostrar resultados
        resultado_final = []
        for resultado in resultados:
            # print(f"Arquivo: {resultado['file_name']}")
            # print(f"Página: {resultado['page_number']}")
            if resultado['content'] not in resultado_final:
                resultado_final.append(resultado['content'])
            # print(f"Correspondências: {resultado['keyword_matches']}")
        
        return resultado_final

    except Exception as e:
        print(f"Erro na busca: {e}")