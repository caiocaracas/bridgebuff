import pandas as pd
import numpy as np 
from flask import Flask, request, jsonify

app = Flask(__name__)

def load_scores_ndjson(file_path: str) -> pd.DataFrame:
  df_raw = pd.read_json(file_path, lines=True)
  
  # usa json_normalize para criar colunas separadas a partir de df_raw["score"]
  df_score = pd.json_normalize(df_raw["score"])
  
  # junta as colunas originais de df_raw (exceto 'score') com as colunas de df_score
  df_merged = pd.concat([df_raw.drop(columns=["score"]), df_score], axis=1)
  
  df_merged["ships_sunk"] = df_merged["shot_received"] - df_merged["invalid_shots"]
  df_merged["ships_escaped"] = df_merged["escaped_ships"]
  df_merged["game_id"] = range(1, len(df_merged) + 1)

  df_merged.replace([np.inf, -np.inf], np.nan, inplace=True)
  df_merged.fillna(0, inplace=True)

  return df_merged

try:
  DF_SCORES = load_scores_ndjson("scores.json") 
except FileNotFoundError:
  print("ERRO: arquivo scores.json não foi encontrado. Verifique o caminho.")
  #  DataFrame vazio para evitar que o servidor quebre
  DF_SCORES = pd.DataFrame(columns=["game_id","ships_sunk","ships_escaped"])

# DataFrames ordenados para ranking
DF_SUNK = DF_SCORES.sort_values("ships_sunk", ascending=False).reset_index(drop=True)
DF_ESCAPED = DF_SCORES.sort_values("ships_escaped", ascending=True).reset_index(drop=True)

# índice por 'game_id' para lookup rápido no endpoint /api/game/<id>
DF_SCORES.set_index("game_id", inplace=True, drop=False)

def paginate_df(df: pd.DataFrame, limit: int, start: int) -> pd.DataFrame:
  """
  retorna o slice paginado do DataFrame:
    - start é 1-based (se start=1, pega do índice 0 até limit-1)
    - limit é a quantidade de elementos na página
  """
  start_idx = start - 1
  end_idx = start_idx + limit
  total = len(df)

  if start_idx >= total:
    # página sem registros
    return df.iloc[0:0]  # DataFrame vazio
  return df.iloc[start_idx:end_idx]

def build_pagination_links(ranking_type: str, limit: int, start: int, total_items: int):
  """
  constrói os links 'prev' e 'next' para paginação
    - ranking_type: "sunk" ou "escaped"
    - limit: quantos itens por página
    - start: página atual (1-based)
    - total_items: quantidade total de itens no ranking
  """
  # prev
  prev_start = start - limit
  if prev_start < 1:
      prev_link = None
  else:
      prev_link = f"/api/rank/{ranking_type}?limit={limit}&start={prev_start}"

  # next
  next_start = start + limit
  if next_start > total_items:
      next_link = None
  else:
      next_link = f"/api/rank/{ranking_type}?limit={limit}&start={next_start}"

  return prev_link, next_link

# endpoints
@app.route("/api/game/<int:game_id>", methods=["GET"])
def get_game_info(game_id: int):
    """
    /api/game/<game_id>
      - recupera as informações do jogo cujo ID é <game_id>
      - se não encontrar, retorna 404
    """
    if game_id not in DF_SCORES.index:
        return jsonify({"error": "Game not found"}), 404
    
    row = DF_SCORES.loc[game_id]  # recupera linha (Series)
    game_dict = row.to_dict()
    game_dict.pop("game_id", None)  # remove a chave game_id de dentro de game_stats

    response = {
        "game_id": game_id,
        "game_stats": game_dict
    }
    return jsonify(response), 200


@app.route("/api/rank/sunk", methods=["GET"])
def get_rank_sunk():
    """
    /api/rank/sunk?limit={count}&start={index}
      - retorna com lista de jogos em ordem decrescente de ships_sunk
      - paginação com limit (<= 50) e start (1-based)
    """
    try:
        limit = int(request.args.get("limit", 10))
        start = int(request.args.get("start", 1))
    except ValueError:
        return jsonify({"error": "parametros 'limit' e 'start' devem ser inteiros"}), 400

    if limit < 1 or limit > 50:
        return jsonify({"error": "'limit' deve estar entre 1 e 50"}), 400

    total_games = len(DF_SUNK)
    page_df = paginate_df(DF_SUNK, limit, start)
    game_ids = page_df["game_id"].tolist()

    prev_link, next_link = build_pagination_links("sunk", limit, start, total_games)

    response = {
        "ranking": "sunk",
        "limit": limit,
        "start": start,
        "games": game_ids,
        "prev": prev_link,
        "next": next_link
    }
    return jsonify(response), 200


@app.route("/api/rank/escaped", methods=["GET"])
def get_rank_escaped():
    """
    /api/rank/escaped?limit={count}&start={index}
      - retorna com lista de jogos em ordem crescente de ships_escaped
      - paginação com limit (<= 50) e start (1-based)
    """
    try:
        limit = int(request.args.get("limit", 10))
        start = int(request.args.get("start", 1))
    except ValueError:
        return jsonify({"error": "parametros 'limit' e 'start' devem ser inteiros"}), 400

    if limit < 1 or limit > 50:
        return jsonify({"error": "'limit' deve estar entre 1 e 50"}), 400

    total_games = len(DF_ESCAPED)
    page_df = paginate_df(DF_ESCAPED, limit, start)
    game_ids = page_df["game_id"].tolist()

    prev_link, next_link = build_pagination_links("escaped", limit, start, total_games)

    response = {
        "ranking": "escaped",
        "limit": limit,
        "start": start,
        "games": game_ids,
        "prev": prev_link,
        "next": next_link
    }
    return jsonify(response), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)