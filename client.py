#!/usr/bin/env python3

import sys
import socket
import json

def main():
    """
    uso:
      ./client.py <IP> <port> <analysis> <output>
    onde:
      <analysis> = 1 ou 2
      <output> = nome do arquivo CSV de saída
    """
    if len(sys.argv) != 5:
        print("Uso: ./client.py <IP> <port> <analysis> <output>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    try:
        analysis = int(sys.argv[3])
    except ValueError:
        print("O parâmetro <analysis> deve ser um inteiro (1 ou 2).")
        sys.exit(1)
    output_file = sys.argv[4]

    if analysis not in (1, 2):
        print("O parâmetro <analysis> deve ser 1 ou 2.")
        sys.exit(1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        print(f"Conectando ao servidor {host}:{port}...")
        sock.connect((host, port))
        print("Conexão estabelecida com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao servidor: {e}")
        sys.exit(1)

    # obter a lista de até 100 jogos relevante ao analysis
    if analysis == 1:
        print("Iniciando Análise 1: Immortals (Top 100 ships_sunk)...")
        # top 100 rank de 'sunk'
        game_ids = get_top_n_games(sock, host, port, "sunk", 100)
        print(f"Total de game_ids obtidos para Análise 1: {len(game_ids)}")
        # análise Immortals
        csv_lines = analysis_immortals(sock, host, port, game_ids)
    else:  # analysis == 2
        print("Iniciando Análise 2: Top Meta (Top 100 ships_escaped)...")
        # top 100 rank de 'escaped'
        game_ids = get_top_n_games(sock, host, port, "escaped", 100)
        print(f"Total de game_ids obtidos para Análise 2: {len(game_ids)}")
        # análise de cannon placement
        csv_lines = analysis_top_meta(sock, host, port, game_ids)

    # salvar o CSV sem cabeçalho
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for line in csv_lines:
                f.write(line + "\n")
        print(f"Arquivo CSV '{output_file}' gerado com sucesso.")
    except Exception as e:
        print(f"Erro ao escrever no arquivo CSV: {e}")

    sock.close()
    print("Conexão encerrada.")

def get_top_n_games(sock, host, port, ranking_type, n=100):
    """
    obtém até 'n' game_ids do ranking 'ranking_type' ('sunk' ou 'escaped'),
    verificando a API em páginas, pois cada página pode ter no máximo 50 results
    retorna uma lista de game_ids (no máximo n)
    """
    game_ids = []
    start = 1
    limit = 50  # máximo permitido

    while len(game_ids) < n:
        print(f"Solicitando página com start={start}, limit={limit} para ranking '{ranking_type}'...")
        # montar a URL: /api/rank/sunk?limit=50&start=<start> (ou 'escaped')
        path = f"/api/rank/{ranking_type}?limit={limit}&start={start}"
        data = http_get(sock, host, port, path)
        if data is None:
            print("Falha ao obter dados da API.")
            break  # algum erro

        try:
            parsed = json.loads(data)
            print(f"Resposta recebida: {len(parsed.get('games', []))} jogos nesta página.")
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da resposta.")
            break

        if "games" not in parsed:
            print("Campo 'games' não encontrado na resposta.")
            break

        page_games = parsed["games"]
        if not page_games:
            print("Nenhum jogo encontrado na página atual. Finalizando coleta de game_ids.")
            break

        for g in page_games:
            if len(game_ids) < n:
                game_ids.append(g)
            else:
                break

        if not parsed.get("next"):
            print("Não há próxima página. Finalizando coleta de game_ids.")
            break

        # caso contrário, incrementar start de 50 em 50
        start += limit

    return game_ids

def http_get(sock, host, port, path):
    """
    Envia uma requisição GET HTTP/1.1 para o socket 'sock'
    e retorna o corpo da resposta (string) ou None em caso de erro

    - manter a conexão aberta (HTTP/1.1)
    - ler o 'Content-Length' e depois ler exatamente esse número de bytes do body
    - o sock se mantém conectado, mas precisamos ter cuidado na hora de ler, pois
    podem haver várias requisições/respostas no mesmo socket
    """
    # construir request HTTP/1.1 com keep-alive
    request_lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}",
        "Connection: keep-alive",
        "",  # linha em branco para terminar o cabeçalho
        ""  # corpo vazio
    ]
    request_data = "\r\n".join(request_lines)

    try:
        sock.sendall(request_data.encode("utf-8"))
        print(f"Requisição GET {path} enviada.")
    except Exception as e:
        print(f"Erro ao enviar requisição {path}: {e}")
        return None

    # ler a resposta HTTP do socket 
    # ler cabeçalho linha a linha até achar linha vazia
    response_header = b""
    while True:
        try:
            chunk = sock.recv(1)
            if not chunk:
                # conexão fechada?
                print("Conexão fechada pelo servidor enquanto recebia o cabeçalho.")
                return None
            response_header += chunk
            # verificar se chegamos ao fim do cabeçalho
            if b"\r\n\r\n" in response_header:
                break
        except Exception as e:
            print(f"Erro ao receber dados do socket: {e}")
            return None

    # dividir cabeçalho e resto (caso tenha lido algo do body por engano)
    parts = response_header.split(b"\r\n\r\n", 1)
    header_part = parts[0].decode("utf-8", errors="replace")
    body_already = parts[1] if len(parts) > 1 else b""

    # analisar status code
    first_line = header_part.split("\r\n")[0]
    if "200" not in first_line:
        print(f"Resposta HTTP não é 200: {first_line}")
        return None

    # achar Content-Length
    content_length = 0
    lines = header_part.split("\r\n")
    for ln in lines[1:]:
        if ln.lower().startswith("content-length:"):
            # pegar valor
            parts_len = ln.split(":", 1)
            if len(parts_len) == 2:
                try:
                    content_length = int(parts_len[1].strip())
                    print(f"Content-Length: {content_length}")
                except:
                    print("Falha ao interpretar Content-Length.")
            break

    # ler exatamente content_length - len(body_already) bytes adicionais
    body = body_already
    to_read = content_length - len(body_already)
    while to_read > 0:
        try:
            chunk = sock.recv(to_read)
            if not chunk:
                print("Conexão fechada pelo servidor antes de receber todo o corpo.")
                break
            body += chunk
            to_read -= len(chunk)
        except Exception as e:
            print(f"Erro ao receber o corpo da resposta: {e}")
            return None

    print(f"Corpo da resposta recebido: {len(body)} bytes.")
    return body.decode("utf-8", errors="replace")

def analysis_immortals(sock, host, port, game_ids):
    """
    recebe a lista dos game_ids do top 100 de 'ships_sunk'
    para cada game_id, faz GET /api/game/<id> e extrai:
      - auth
      - ships_sunk
    agrupa por auth, contando quantos jogos aparecem e qual a média de ships_sunk
    retorna as linhas CSV 
    """
    print("Iniciando Análise 1: Agrupamento por 'auth' e cálculo de médias.")
    gas_data = {}  # dict: auth -> { "count": x, "sum_sunk": y }

    for idx, g_id in enumerate(game_ids, 1):
        print(f"Processando game_id {idx}/{len(game_ids)}: {g_id}")
        info = get_game_info(sock, host, port, g_id)
        if not info:
            print(f"Falha ao obter informações do game_id {g_id}.")
            continue

        game_stats = info.get("game_stats", {})
        
        # usar 'auth' como identificador
        auth = game_stats.get("auth", "UnknownAuth")
        ships_sunk = game_stats.get("ships_sunk", 0)

        if auth not in gas_data:
            gas_data[auth] = {"count": 0, "sum_sunk": 0.0}

        gas_data[auth]["count"] += 1
        gas_data[auth]["sum_sunk"] += ships_sunk

    # montar a lista (auth, count, average_sunk) e ordenar
    result_list = []
    for auth, vals in gas_data.items():
        c = vals["count"]
        s = vals["sum_sunk"]
        avg_sunk = s / c if c > 0 else 0
        result_list.append((auth, c, avg_sunk))

    # ordenar por c desc 
    result_list.sort(key=lambda x: x[1], reverse=True)

    # gerar CSV
    # <auth>,<num_jogos>,<media_sunk>
    lines = []
    for (auth, c, avg) in result_list:
        # Remover vírgulas do 'auth' para não quebrar o CSV
        auth_clean = auth.replace(",", "")
        line = f"{auth_clean},{c},{avg:.2f}"
        lines.append(line)

    print("Análise 1 concluída.")
    return lines

def get_game_info(sock, host, port, game_id):
    """
    faz GET /api/game/<game_id>, retorna JSON em py ou None se erro
    """
    path = f"/api/game/{game_id}"
    print(f"Solicitando informações para game_id {game_id}...")
    data = http_get(sock, host, port, path)
    if not data:
        print(f"Falha ao obter dados para game_id {game_id}.")
        return None
    try:
        parsed = json.loads(data)
        print(f"Informações obtidas para game_id {game_id}.")
        return parsed
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON para game_id {game_id}.")
        return None

# ANÁLISE 2: Top Meta 
def analysis_top_meta(sock, host, port, game_ids):
    """
    recebe a lista dos game_ids do top 100 de 'ships_escaped'
    para cada game_id, faz GET /api/game/<id> e extrai:
      - cannons (lista de listas?)
      - ships_escaped
    normaliza o cannon placement em um string de 8 dígitos
    agrupa por essa string e faz a média de ships_escaped
    retorna as linhas CSV sem cabeçalho
    """
    print("Iniciando Análise 2: Cannon Placement e cálculo de médias de 'ships_escaped'.")
    placement_data = {}   # dict: placement_str -> {"count": x, "sum_escaped": y}

    for idx, g_id in enumerate(game_ids, 1):
        print(f"Processando game_id {idx}/{len(game_ids)}: {g_id}")
        info = get_game_info(sock, host, port, g_id)
        if not info:
            print(f"Falha ao obter informações do game_id {g_id}.")
            continue

        game_stats = info.get("game_stats", {})
        cannons = game_stats.get("cannons", []) 
        ships_escaped = game_stats.get("ships_escaped", 0)

        # contar canhões por row
        row_counters = {}
        for cpos in cannons:
            if not isinstance(cpos, list) or len(cpos) < 2:
                continue
            row_idx = cpos[0]
            row_counters[row_idx] = row_counters.get(row_idx, 0) + 1

        row_counts = [0]*8
        for r in range(8):
            row_counts[r] = row_counters.get(r, 0)

        # "Histograma" de row_counts
        hist = [0]*8  # hist[i] = quantas rows têm i canhões
        for c in row_counts:
            if 0 <= c <= 7:
                hist[c] += 1
            else:
                # se por acaso tem mais que 7 canhões, agrupa em 7
                hist[7] += 1

        # converter hist em string de 8 dígitos
        placement_str = "".join(str(x) for x in hist)

        if placement_str not in placement_data:
            placement_data[placement_str] = {"count": 0, "sum_escaped": 0}

        placement_data[placement_str]["count"] += 1
        placement_data[placement_str]["sum_escaped"] += ships_escaped

    # montar lista e ordenar por media_escaped asc
    result_list = []
    for placement_str, vals in placement_data.items():
        c = vals["count"]
        s = vals["sum_escaped"]
        avg_escaped = s / c if c > 0 else 0
        result_list.append((placement_str, avg_escaped))

    # ordenar por avg_escaped crescente
    result_list.sort(key=lambda x: x[1])

    lines = []
    for (placement_str, avg) in result_list:
        line = f"{placement_str},{avg:.2f}"
        lines.append(line)

    print("Análise 2 concluída.")
    return lines

if __name__ == "__main__":
    main()
