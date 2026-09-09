"""
Gera o link do capturador para colar na barra de favoritos.

Um bookmarklet é o conteúdo de `capturador_campanhas.js` numa linha só, com
o prefixo `javascript:` e os caracteres especiais codificados — é assim que o
navegador aceita guardar código num favorito.

USO
    python gerar_bookmarklet.py

Ele grava `capturador_bookmarklet.txt`, que contém a linha para colar no
campo de URL do favorito.

POR QUE O COMENTÁRIO DO TOPO É REMOVIDO E O RESTO NÃO
    Só o bloco `/* ... */` inicial sai: ele é grande, é documentação para
    quem lê o arquivo e não serve de nada dentro do favorito. Os comentários
    do meio do código ficam, junto com as quebras de linha.

    Tentar minificar de verdade — tirar comentários de linha e juntar tudo
    numa linha só — exigiria distinguir um `//` de comentário de um `//`
    dentro de texto ou de expressão regular. Errar isso comenta metade do
    script em silêncio, e o favorito passa a não fazer nada sem dizer por
    quê. As quebras de linha codificadas custam alguns bytes e não há limite
    prático de tamanho aqui, então o troco não compensa o risco.
"""

import re
import sys
from pathlib import Path
from urllib.parse import quote

ORIGEM = Path(__file__).with_name('capturador_campanhas.js')
DESTINO = Path(__file__).with_name('capturador_bookmarklet.txt')


def gerar(origem=ORIGEM):
    js = origem.read_text(encoding='utf-8')
    # Remove apenas o bloco de documentação do topo.
    js = re.sub(r'^\s*/\*.*?\*/\s*', '', js, count=1, flags=re.S)
    # `safe=''` porque tudo precisa ser codificado: um `&` ou um `#` cru
    # trunca a URL do favorito e o script chega pela metade.
    return 'javascript:' + quote(js.strip(), safe='')


def main():
    # O console do Windows usa cp1252 e derruba o script num acento.
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    if not ORIGEM.exists():
        raise SystemExit(f'Não encontrei {ORIGEM.name} nesta pasta.')
    link = gerar()
    DESTINO.write_text(link, encoding='utf-8')
    print(f'Gerado: {DESTINO.name} ({len(link):,} caracteres)'.replace(',', '.'))
    print()
    print('Como instalar:')
    print('  1. Abra o arquivo e copie a linha inteira.')
    print('  2. Barra de favoritos > botao direito > Adicionar pagina.')
    print('  3. De o nome "Capturar campanhas" e cole a linha no campo URL.')
    print()
    print('Como usar: abra Publicidade > Campanhas no ML e clique no favorito.')


if __name__ == '__main__':
    main()
