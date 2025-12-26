import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except ImportError:
    Credentials = None
    build = None


BASE_URL = "https://utilities.confirmafacil.com.br"
LOGIN_URL = f"{BASE_URL}/login/login"
OCORR_URL = f"{BASE_URL}/filter/ocorrencia"

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
OUTPUT_NAME = "STATUS_PEDIDOS.xlsx"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.getenv("SHEET_ID_PREVENTIVOS", "1N5kJ4Q99J_yCGNRya8KPP7ZIm7GjYurq-zte5KnUCRM")
SHEET_RANGE_INPUT = os.getenv("SHEET_RANGE_INPUT_PREVENTIVOS", "PREVENTIVOS!D:D")
SHEET_RANGE_OUTPUT = os.getenv("SHEET_RANGE_OUTPUT_PREVENTIVOS", "PREVENTIVOS!B:B")
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\j.rhoden\Desktop\PREVENTIVO\nth-platform-428511-q9-e685a5723bfc.json",
)

ENTREGUE_CODES = {"1", "2", "37", "999"}
CANCELADO_CODES = {"25", "102", "203", "303", "325", "327"}
ALL_CODES = ",".join(sorted(ENTREGUE_CODES | CANCELADO_CODES, key=int))

PAGE_SIZE = 1000
SUBLOTE_SIZE = 20
TIMEOUT = (5, 60)


def has_cf_credentials() -> bool:
    if not os.getenv("CF_EMAIL") or not os.getenv("CF_SENHA"):
        print("Defina CF_EMAIL e CF_SENHA nas variaveis de ambiente.")
        return False
    return True


def make_session(max_pool: int = 20, total_retries: int = 3, backoff: float = 0.5) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=max_pool, pool_maxsize=max_pool, max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Accept-Encoding": "gzip, deflate"})
    return session


def normalize_pedido(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


class ConfirmaFacilAPI:
    def __init__(self) -> None:
        self.session = make_session(max_pool=40, total_retries=3, backoff=0.5)
        self.token = None
        self.email = os.getenv("CF_EMAIL")
        self.senha = os.getenv("CF_SENHA")
        self.authenticate()

    def authenticate(self) -> bool:
        headers = {"Content-Type": "application/json"}
        payload = {"email": self.email, "senha": self.senha, "idcliente": 206, "idproduto": 1}
        try:
            response = self.session.post(LOGIN_URL, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
            response.raise_for_status()
            self.token = response.json().get("resposta", {}).get("token")
            return bool(self.token)
        except Exception as exc:
            print(f"Erro ao autenticar: {exc}")
            return False

    def _request(self, params: dict, retries: int = 3) -> dict:
        headers = {"Authorization": self.token, "accept": "application/json"}
        for attempt in range(1, retries + 1):
            response = self.session.get(OCORR_URL, headers=headers, params=params, timeout=TIMEOUT)
            if response.status_code == 401:
                print("Token expirado. Reautenticando...")
                if not self.authenticate():
                    return {"respostas": []}
                headers["Authorization"] = self.token
                continue
            if response.status_code == 404:
                return {"respostas": []}
            try:
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                if attempt == retries:
                    print(f"Falha ao consultar ocorrencias: {exc}")
                    return {"respostas": []}
                time.sleep(2 * attempt)
        return {"respostas": []}

    def fetch_ocorrencias(self, pedidos: List[str]) -> List[dict]:
        data_final = datetime.now()
        data_inicial = data_final - timedelta(days=600)
        params = {
            "pedido": ",".join(pedidos),
            "page": 0,
            "size": PAGE_SIZE,
            "de": data_inicial.strftime("%Y/%m/%d 00:00:00"),
            "ate": data_final.strftime("%Y/%m/%d 23:59:59"),
            "codigoOcorrencia": ALL_CODES,
            "tipoData": "OCORRENCIA",
        }

        payload = self._request(params)
        respostas = payload.get("respostas", []) or []
        total_pages = int(payload.get("totalPages", 0) or 0)

        for page in range(1, total_pages):
            params["page"] = page
            page_payload = self._request(params)
            respostas.extend(page_payload.get("respostas", []) or [])

        return respostas


def extract_pedido(item: dict) -> str:
    pedido = (item.get("pedido") or {}).get("numero")
    if not pedido:
        pedido = ((item.get("embarque") or {}).get("pedido") or {}).get("numero")
    return normalize_pedido(pedido)


def extract_codigo(item: dict) -> str:
    codigo = (item.get("tipoOcorrencia") or {}).get("codigo")
    if codigo is None:
        return ""
    return str(codigo).strip()


def resolver_status(pedidos: List[str]) -> Dict[str, str]:
    api = ConfirmaFacilAPI()
    flags = {pedido: {"entregue": False, "cancelado": False} for pedido in pedidos}

    for sublote in chunked(pedidos, SUBLOTE_SIZE):
        ocorrencias = api.fetch_ocorrencias(sublote)
        for item in ocorrencias:
            pedido = extract_pedido(item)
            codigo = extract_codigo(item)
            if not pedido or pedido not in flags:
                continue
            if codigo in ENTREGUE_CODES:
                flags[pedido]["entregue"] = True
            if codigo in CANCELADO_CODES:
                flags[pedido]["cancelado"] = True

    status_map = {}
    for pedido, info in flags.items():
        if info["entregue"]:
            status_map[pedido] = "ENTREGUE"
        elif info["cancelado"]:
            status_map[pedido] = "CANCELADO"
        else:
            status_map[pedido] = "DESPACHADO"
    return status_map


def load_pedidos_from_sheet() -> Tuple[List[str], List[str], bool]:
    if Credentials is None or build is None:
        print("Dependencias do Google Sheets nao encontradas. Instale google-auth e google-api-python-client.")
        return [], [], False
    if not os.path.exists(CREDENTIALS_PATH):
        print(f"Arquivo de credenciais nao encontrado: {CREDENTIALS_PATH}")
        return [], [], False
    if SHEET_ID == "COLE_AQUI_O_ID_DA_PLANILHA":
        print("Atualize o SHEET_ID no script antes de executar.")
        return [], [], False

    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=SHEET_RANGE_INPUT).execute()
    values = result.get("values", [])
    if not values:
        return [], [], False

    first_col = [row[0] if row else "" for row in values]
    start_index = 0
    has_header = False
    if first_col:
        header = str(first_col[0]).strip().lower()
        if header in {"pedido", "pedidos"}:
            start_index = 1
            has_header = True

    pedidos = [normalize_pedido(v) for v in first_col[start_index:]]
    pedidos = [p for p in pedidos if p and str(p).strip().lower() != "nan"]
    return pedidos, first_col, has_header


def update_status_in_sheet(values_count: int, statuses: List[str], has_header: bool) -> None:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    output_values = []
    if has_header:
        output_values.append(["STATUS"])

    output_values.extend([[s] for s in statuses])

    total_rows = max(values_count, len(output_values))
    end_row = total_rows if total_rows > 0 else 1
    output_range = f"{SHEET_RANGE_OUTPUT.split('!')[0]}!B1:B{end_row}"

    body = {"values": output_values}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=output_range,
        valueInputOption="RAW",
        body=body,
    ).execute()

    print(f"Planilha atualizada em {output_range}.")


def save_excel_safely(df: pd.DataFrame, path: str) -> None:
    try:
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        print(f"Arquivo gerado com sucesso: {path}")
    except PermissionError:
        base, ext = os.path.splitext(path)
        alt = f"{base}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
        with pd.ExcelWriter(alt, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        print(f"Arquivo estava aberto. Salvei como: {alt}")


def main() -> None:
    if not has_cf_credentials():
        return
    pedidos_list, raw_values, has_header = load_pedidos_from_sheet()
    if not raw_values:
        print("Nenhum pedido encontrado no Google Sheets.")
        return

    pedidos_unicos = list(dict.fromkeys(pedidos_list))
    status_map = resolver_status(pedidos_unicos)

    statuses = []
    start_index = 1 if has_header else 0
    for value in raw_values[start_index:]:
        pedido = normalize_pedido(value)
        if not pedido:
            statuses.append("")
        else:
            statuses.append(status_map.get(pedido, "DESPACHADO"))

    update_status_in_sheet(len(raw_values), statuses, has_header)

    rows = [{"PEDIDO": p, "STATUS": status_map.get(p, "DESPACHADO")} for p in pedidos_list if p]
    df_final = pd.DataFrame(rows)
    if df_final.empty:
        print("Nada para salvar.")
        return

    output_path = os.path.join(BASE_PATH, OUTPUT_NAME)
    save_excel_safely(df_final, output_path)


if __name__ == "__main__":
    main()
