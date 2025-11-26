# /opt/airflow/spark/jobs/metabase_dashboard.py
import os
import time
import requests

METABASE_HOST = os.environ.get("METABASE_HOST", "http://metabase:3000")
METABASE_USER = os.environ.get("MB_USER")
METABASE_PASS = os.environ.get("MB_PASS")
MB_API_KEY = os.environ.get("MB_API_KEY")

REQ_TIMEOUT = int(os.environ.get("MB_REQ_TIMEOUT", 8))


def safe_json(res):
    try:
        return res.json()
    except Exception:
        print("⚠ response is not JSON. status:", getattr(res, "status_code", None))
        print("  preview:", (res.text or "")[:400])
        return None


import traceback

def request(method, path, headers=None, **kwargs):
    url = METABASE_HOST.rstrip("/") + path
    print(f"[debug] Request -> {method} {url}")
    if headers:
        print(f"[debug]   headers: {headers}")
    if "json" in kwargs:
        try:
            print(f"[debug]   json payload preview: {str(kwargs['json'])[:500]}")
        except Exception:
            pass

    try:
        res = requests.request(method, url, timeout=REQ_TIMEOUT, headers=headers, **kwargs)
        print(f"[debug]   response status: {getattr(res, 'status_code', None)}")
        # print small preview to not spam logs
        print(f"[debug]   response preview: {(res.text or '')[:400]}")
        return res
    except Exception as e:
        print(f"✖ Request failed: {method} {url} -> {e}")
        traceback.print_exc()
        return None



def wait_for_metabase(max_wait_s=60):
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        res = request("GET", "/api/health")
        if res and res.status_code == 200:
            print("✔ Metabase está pronto!")
            return True
        print("⏳ Aguardando Metabase... status:", getattr(res, "status_code", None))
        time.sleep(2)
    print("❌ Metabase não ficou pronto a tempo.")
    return False


def get_session_headers():
    """
    Tenta login via user/pass (retorna X-Metabase-Session header) e,
    caso não consiga, usa MB_API_KEY se fornecida.
    Retorna dict de headers ou None.
    """
    # 1) Tenta user/pass
    if METABASE_USER and METABASE_PASS:
        try:
            res = requests.post(
                f"{METABASE_HOST.rstrip('/')}/api/session",
                json={"username": METABASE_USER, "password": METABASE_PASS},
                timeout=REQ_TIMEOUT,
            )
        except Exception as e:
            print("✖ Falha ao conectar para login:", e)
            res = None

        data = safe_json(res) if res else None
        if data and data.get("id"):
            print("✔ Login com usuário/senha bem sucedido.")
            return {"X-Metabase-Session": data["id"], "Content-Type": "application/json"}
        else:
            print("⚠ Login com usuário/senha falhou ou retornou inesperado. status:", getattr(res, "status_code", None))

    # 2) Fallback: API KEY
    if MB_API_KEY:
        print("✔ Usando MB_API_KEY para autenticação (fallback).")
        return {"X-Metabase-Api-Key": MB_API_KEY, "Content-Type": "application/json"}

    # 3) Sem credenciais
    print("⚠️ Nenhuma credencial Metabase encontrada (MB_USER/MB_PASS/MB_API_KEY).")
    return None


def get_table_id(headers, table_name="curated_crypto", database_id=2):
    """
    Busca o id da tabela no Metabase. Primeiro usa /api/database/{id}/metadata
    (retorna 'tables' no seu ambiente), em seguida faz fallback para /api/table.
    Retorna int id ou None.
    """
    if not headers:
        print("get_table_id: sem headers de autenticação.")
        return None

    # Tenta metadata endpoint
    res = request("GET", f"/api/database/{database_id}/metadata", headers=headers)
    if res and res.status_code == 200:
        data = safe_json(res)
        if isinstance(data, dict):
            tables = data.get("tables") or []
            for t in tables:
                name = str(t.get("name") or "").lower()
                display = str(t.get("display_name") or "").lower()
                if name == table_name.lower() or display == table_name.lower():
                    print(f"✔ Encontrada tabela (metadata): {t.get('name')} id={t.get('id')}")
                    return t.get("id")
    else:
        print("get_table_id: /metadata status:", None if not res else res.status_code, "preview:", (res.text or "")[:200] if res else None)

    # Fallback: /api/table
    res2 = request("GET", "/api/table", headers=headers)
    if res2 and res2.status_code == 200:
        data2 = safe_json(res2)
        if isinstance(data2, list):
            for t in data2:
                name = str(t.get("name") or "").lower()
                display = str(t.get("display_name") or "").lower()
                if name == table_name.lower() or display == table_name.lower():
                    print(f"✔ Encontrada tabela (/api/table): {t.get('name')} id={t.get('id')}")
                    return t.get("id")
    else:
        print("get_table_id: /api/table status:", None if not res2 else res2.status_code, "preview:", (res2.text or "")[:200] if res2 else None)

    print(f"⚠ Tabela '{table_name}' não encontrada.")
    return None


def create_dashboard_safe(dashboard_name="Crypto Dashboard"):
    """
    Fluxo seguro para uso na DAG:
    - espera Metabase
    - autentica (user/pass ou api key)
    - localiza tabela curated_crypto
    - cria dashboard e card (visualization_settings obrigatório)
    Não levanta exceções fatais — retorna id do dashboard ou None.
    """
    print("=== metabase: início da criação segura do dashboard ===")

    if not wait_for_metabase(max_wait_s=60):
        print("❌ Metabase indisponível — skip.")
        return None

    headers = get_session_headers()
    if not headers:
        print("⚠ Sem credenciais — pulando criação do dashboard.")
        return None

    table_id = get_table_id(headers, table_name="curated_crypto", database_id=2)
    if not table_id:
        print("⚠ Tabela 'curated_crypto' não encontrada — pulando dashboard.")
        return None

    # cria dashboard ou recupera existente
    payload = {"name": dashboard_name}
    res = request("POST", "/api/dashboard", headers=headers, json=payload)
    dash_id = None
    if res and res.status_code in (200, 201):
        dd = safe_json(res)
        dash_id = dd.get("id") if dd else None
        print("🆕 Dashboard criado com id:", dash_id)
    else:
        print("ℹ create_dashboard falhou ou sem resposta:", None if not res else res.status_code)
        existing = request("GET", "/api/dashboard", headers=headers)
        if existing and existing.status_code == 200:
            arr = safe_json(existing) or []
            for d in arr:
                if str(d.get("name") or "").lower() == dashboard_name.lower():
                    dash_id = d.get("id")
                    print("📌 Dashboard já existe ->", dash_id)
                    break
        if not dash_id:
            print("❌ Não foi possível criar ou recuperar dashboard.")
            return None

    # cria card (se não existir)
    card_name = "Preço Criptomoeda"
    card_id = None
    existing_cards = request("GET", "/api/card", headers=headers)
    if existing_cards and existing_cards.status_code == 200:
        cards_arr = safe_json(existing_cards) or []
        for c in cards_arr:
            if str(c.get("name") or "").lower() == card_name.lower():
                card_id = c.get("id")
                print("Card já existe:", card_id)
                break

    if not card_id:
        payload_card = {
            "name": card_name,
            "dataset_query": {
                "type": "query",
                "database": 2,
                "query": {"source-table": table_id}
            },
            "display": "line",
            "visualization_settings": {}
        }
        cr = request("POST", "/api/card", headers=headers, json=payload_card)
        if not cr or cr.status_code not in (200, 201):
            print("Falha ao criar card:", None if not cr else cr.status_code, (cr.text or "")[:400])
            return dash_id
        cd = safe_json(cr)
        card_id = cd.get("id")
        print("Card criado:", card_id)

    # verifica se card já está no dashboard
    res_dash = request("GET", f"/api/dashboard/{dash_id}", headers=headers)
    if res_dash and res_dash.status_code == 200:
        dash_data = safe_json(res_dash) or {}
        ordered = dash_data.get("ordered_cards") or []
        for slot in ordered:
            c = slot.get("card")
            if c and c.get("id") == card_id:
                print("Card já anexado ao dashboard (pulei anexar).")
                print("🎉 Finalizado:", f"{METABASE_HOST}/dashboard/{dash_id}")
                return dash_id

        # tenta anexar card — testar endpoints possíveis (o oficial é /api/dashboard/:id/cards)
    attach = {"cardId": card_id, "sizeX": 8, "sizeY": 6}
    endpoints_to_try = [
        f"/api/dashboard/{dash_id}/cards",
        f"/api/dashboard/{dash_id}/add_card",
    ]
    ar = None
    for ep in endpoints_to_try:
        print(f"[debug] tentando anexar via {ep} payload: {attach}")
        ar = request("POST", ep, headers=headers, json=attach)
        if ar is None:
            print(f"[debug] tentativa {ep} retornou None (exceção ocorreu).")
            continue
        print(f"[debug] tentativa {ep} status {ar.status_code}")
        if ar.status_code in (200, 201):
            print("Card anexado ao dashboard com sucesso via", ep)
            print("🎉 Finalizado:", f"{METABASE_HOST}/dashboard/{dash_id}")
            return dash_id
        else:
            print(f"tentativa anexar via {ep} -> status: {ar.status_code}, preview: {(ar.text or '')[:600]}")

    print("Falha ao anexar card por todos os endpoints testados. Retornando dashboard id (sem erro fatal).")

    print("🎉 Finalizado:", f"{METABASE_HOST}/dashboard/{dash_id}")
    return dash_id


# não roda nada na importação do módulo (evita break na DAG)
if __name__ == "__main__":
    create_dashboard_safe()
