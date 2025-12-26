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
OUTPUT_NAME = "STATUS_NF_ESTAB.xlsx"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.getenv("SHEET_ID_COBRANCA", "1CrKT2UxZOuhpg0iWGY0cAthuQxgsYvVanrBBlv2c6ic")
SHEET_RANGE_INPUT = os.getenv("SHEET_RANGE_INPUT_COBRANCA", "RETORNO!A:B")
SHEET_RANGE_OUTPUT = os.getenv("SHEET_RANGE_OUTPUT_COBRANCA", "RETORNO!K:K")
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\j.rhoden\Desktop\PREVENTIVO\nth-platform-428511-q9-e685a5723bfc.json",
)

ENTREGUE_CODES = {"1", "2", "37", "999"}
CANCELADO_CODES = {"25", "102", "203", "303", "325", "327"}
ALL_CODES = ",".join(sorted(ENTREGUE_CODES | CANCELADO_CODES, key=int))

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


def normalize_nf(value: object) -> str:
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


def normalize_cnpj(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 14:
        return digits.zfill(14)
    return digits


def chunked(items: List[Tuple[str, str]], size: int) -> Iterable[List[Tuple[str, str]]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


class ConfirmaFacilAPI:
    def __init__(self) -> None:
        self.session = make_session(max_pool=20, total_retries=3, backoff=0.5)
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

    def fetch_ocorrencias(self, nf: str, cnpj: str) -> List[dict]:
        params = {
            "numero": nf,
            "cnpjEmbarcador": cnpj,
            "codigoOcorrencia": ALL_CODES,
        }
        payload = self._request(params)
        return payload.get("respostas", []) or []


def extract_codigo(item: dict) -> str:
    codigo = (item.get("tipoOcorrencia") or {}).get("codigo")
    if codigo is None:
        return ""
    return str(codigo).strip()


def resolver_status(pares: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    api = ConfirmaFacilAPI()
    status_map = {}

    for nf, cnpj in pares:
        ocorrencias = api.fetch_ocorrencias(nf, cnpj)
        entregue = False
        cancelado = False
        for item in ocorrencias:
            codigo = extract_codigo(item)
            if codigo in ENTREGUE_CODES:
                entregue = True
            if codigo in CANCELADO_CODES:
                cancelado = True
        if entregue:
            status_map[(nf, cnpj)] = "ENTREGUE"
        elif cancelado:
            status_map[(nf, cnpj)] = "CANCELADO"
        else:
            status_map[(nf, cnpj)] = "-"
    return status_map


def load_inputs_from_sheet() -> Tuple[List[Tuple[str, str]], List[List[str]], bool]:
    if Credentials is None or build is None:
        print("Dependencias do Google Sheets nao encontradas. Instale google-auth e google-api-python-client.")
        return [], [], False
    if not os.path.exists(CREDENTIALS_PATH):
        print(f"Arquivo de credenciais nao encontrado: {CREDENTIALS_PATH}")
        return [], [], False
    if SHEET_ID == "COLE_AQUI_O_ID_DA_PLANILHA":
        print("Atualize o SHEET_ID no script antes de executar.")
        return [], [], False
    if SHEET_RANGE_INPUT == "COLE_AQUI_O_RANGE_DE_NF_E_CNPJ":
        print("Atualize o SHEET_RANGE_INPUT no script antes de executar.")
        return [], [], False
    if SHEET_RANGE_OUTPUT == "COLE_AQUI_O_RANGE_DE_SAIDA":
        print("Atualize o SHEET_RANGE_OUTPUT no script antes de executar.")
        return [], [], False

    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=SHEET_RANGE_INPUT).execute()
    values = result.get("values", [])
    if not values:
        return [], [], False

    has_header = False
    if values:
        header_nf = str(values[0][0]).strip().lower() if len(values[0]) > 0 else ""
        header_cnpj = str(values[0][1]).strip().lower() if len(values[0]) > 1 else ""
        if header_nf in {"nf", "nota", "nota fiscal", "numero"} or header_cnpj in {"cnpj", "estabelecimento"}:
            has_header = True

    start_index = 1 if has_header else 0
    pares = []
    for row in values[start_index:]:
        nf_raw = row[0] if len(row) > 0 else ""
        cnpj_raw = row[1] if len(row) > 1 else ""
        nf = normalize_nf(nf_raw)
        cnpj = normalize_cnpj(cnpj_raw)
        pares.append((nf, cnpj))

    return pares, values, has_header


def build_output_range(output_range: str, total_rows: int) -> str:
    if "!" not in output_range:
        return output_range
    sheet, cols = output_range.split("!", 1)
    col = cols.split(":")[0].strip()
    col_letters = "".join(ch for ch in col if ch.isalpha())
    if not col_letters:
        return output_range
    end_row = max(total_rows, 1)
    return f"{sheet}!{col_letters}1:{col_letters}{end_row}"


def update_status_in_sheet(total_rows: int, statuses: List[str], has_header: bool) -> None:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    output_values = []
    if has_header:
        output_values.append(["STATUS"])
    output_values.extend([[s] for s in statuses])

    output_range = build_output_range(SHEET_RANGE_OUTPUT, max(total_rows, len(output_values)))
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
    pares, raw_values, has_header = load_inputs_from_sheet()
    if not raw_values:
        print("Nenhum dado encontrado no Google Sheets.")
        return

    pares_validos = [(nf, cnpj) for nf, cnpj in pares if nf and cnpj]
    pares_unicos = list(dict.fromkeys(pares_validos))
    status_map = resolver_status(pares_unicos)

    statuses = []
    for nf, cnpj in pares:
        if not nf or not cnpj:
            statuses.append("")
        else:
            statuses.append(status_map.get((nf, cnpj), "-"))

    update_status_in_sheet(len(raw_values), statuses, has_header)

    rows = []
    for nf, cnpj in pares_validos:
        rows.append({"NF": nf, "CNPJ": cnpj, "STATUS": status_map.get((nf, cnpj), "-")})

    df_final = pd.DataFrame(rows)
    if df_final.empty:
        print("Nada para salvar.")
        return

    output_path = os.path.join(BASE_PATH, OUTPUT_NAME)
    save_excel_safely(df_final, output_path)


if __name__ == "__main__":
    main()
