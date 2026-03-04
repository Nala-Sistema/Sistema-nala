"""
FORMATADORES - Sistema Nala
Funções de formatação padrão brasileiro
- Valores monetários: R$ 1.234,56 (ponto milhares, vírgula decimais)
- Percentuais: 18,50% (sempre 2 casas decimais)
- Datas: dd/mm/aaaa
- Quantidades: 1.234 (sem decimais)
"""

def formatar_valor(valor):
    """
    Formata valor monetário no padrão brasileiro.
    Entrada: 1234.56
    Saída: R$ 1.234,56
    """
    try:
        valor_float = float(valor)
        # Formata com 2 casas decimais
        formatado = f"{valor_float:,.2f}"
        # Troca separadores: , para X (temp), . para ,, X para .
        formatado = formatado.replace(',', 'X').replace('.', ',').replace('X', '.')
        return f"R$ {formatado}"
    except:
        return "R$ 0,00"


def formatar_percentual(valor):
    """
    Formata percentual no padrão brasileiro.
    Entrada: 18.5
    Saída: 18,50%
    SEMPRE 2 casas decimais
    """
    try:
        valor_float = float(valor)
        # Força 2 casas decimais
        formatado = f"{valor_float:.2f}"
        # Troca ponto por vírgula
        formatado = formatado.replace('.', ',')
        return f"{formatado}%"
    except:
        return "0,00%"


def formatar_quantidade(valor):
    """
    Formata quantidade (sem decimais).
    Entrada: 1234
    Saída: 1.234
    """
    try:
        valor_int = int(valor)
        formatado = f"{valor_int:,}".replace(',', '.')
        return formatado
    except:
        return "0"


def converter_data_ml(data_str):
    """
    Converte data do Mercado Livre para dd/mm/aaaa.
    Entrada: "10 de janeiro de 2026"
    Saída: "10/01/2026"
    """
    import pandas as pd
    from datetime import datetime
    
    if pd.isna(data_str):
        return None
    
    try:
        # Se já for datetime, formata direto
        if isinstance(data_str, datetime):
            return data_str.strftime("%d/%m/%Y")
        
        # Dicionário de meses em português
        meses = {
            'janeiro': '01', 'fevereiro': '02', 'março': '03', 'marco': '03',
            'abril': '04', 'maio': '05', 'junho': '06',
            'julho': '07', 'agosto': '08', 'setembro': '09',
            'outubro': '10', 'novembro': '11', 'dezembro': '12'
        }
        
        # Parse: "10 de janeiro de 2026"
        partes = str(data_str).lower().split()
        dia = partes[0].zfill(2)
        mes = meses.get(partes[2], '01')
        ano = partes[4]
        
        return f"{dia}/{mes}/{ano}"
    except:
        return str(data_str)


def limpar_numero(valor):
    """
    Remove formatação de número e converte para float.
    Entrada: "R$ 1.234,56" ou "1.234,56" ou 1234.56 (já numérico)
    Saída: 1234.56
    """
    import pandas as pd
    
    if pd.isna(valor):
        return 0.0
    
    try:
        # Se já for número (int ou float), retorna direto
        if isinstance(valor, (int, float)):
            return float(valor)
        
        # Se for string, limpa formatação brasileira
        valor_str = str(valor).replace('R$', '').strip()
        
        # Se tiver vírgula, é formato BR: 1.234,56
        if ',' in valor_str:
            valor_str = valor_str.replace('.', '')  # Remove ponto de milhares
            valor_str = valor_str.replace(',', '.')  # Troca vírgula decimal
        # Senão, assume que ponto é decimal (formato do Excel)
        
        return float(valor_str)
    except:
        return 0.0
