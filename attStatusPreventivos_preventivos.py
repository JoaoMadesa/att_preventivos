import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
from typing import Dict, List, Tuple

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
SHEET_RANGE_INPUT = os.getenv("SHEET_RANGE_INPUT_PREVENTIVOS", "PREVENTIVOS!C:C")
SHEET_RANGE_OUTPUT = os.getenv("SHEET_RANGE_OUTPUT_PREVENTIVOS", "PREVENTIVOS!B:B")
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\j.rhoden\Desktop\PREVENTIVO\nth-platform-428511-q9-e685a5723bfc.json",
)

ENTREGUE_CODES = {"1", "2", "37", "999"}
CANCELADO_CODES = {"25", "102", "203", "303", "325", "327"}
ALL_CODES = ",".join(sorted(ENTREGUE_CODES | CANCELADO_CODES, key=int))

PAGE_SIZE = 1000
TIMEOUT = (5, 60)
ENABLE_ROW_LOGS = os.getenv("LOG_LINHAS_PREVENTIVOS", "1").strip() == "1"
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS_PREVENTIVOS", "8")))


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


def normalize_chave_nfe(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isdigit())


def strip_left_zeros(value: str) -> str:
    stripped = value.lstrip("0")
    return stripped if stripped else "0"


def extract_nfe_fields(chave_nfe: str) -> Tuple[str, str, str]:
    if len(chave_nfe) < 34:
        return "", "", ""

    cnpj = chave_nfe[6:20]
    serie = chave_nfe[22:25]
    numero_nf = chave_nfe[25:34]

    if len(cnpj) != 14 or len(serie) != 3 or len(numero_nf) != 9:
        return "", "", ""

    return numero_nf, serie, cnpj


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

    def fetch_ocorrencias(self, numero_nf: str, serie: str, cnpj: str) -> List[dict]:
        params = {
            "numero": strip_left_zeros(numero_nf),
            "serie": strip_left_zeros(serie),
            "cnpjEmbarcador": cnpj,
            "codigoOcorrencia": ALL_CODES,
            "page": 0,
            "size": PAGE_SIZE,
        }

        payload = self._request(params)
        respostas = payload.get("respostas", []) or []
        total_pages = int(payload.get("totalPages", 0) or 0)

        for page in range(1, total_pages):
            params["page"] = page
            page_payload = self._request(params)
            respostas.extend(page_payload.get("respostas", []) or [])

        return respostas


def extract_codigo(item: dict) -> str:
    codigo = (item.get("tipoOcorrencia") or {}).get("codigo")
    if codigo is None:
        return ""
    return str(codigo).strip()


def resolver_status(chaves_extraidas: List[Tuple[str, str, str]]) -> Dict[Tuple[str, str, str], str]:
    status_map = {}
    total = len(chaves_extraidas)
    print(f"Iniciando consultas na API: {total} registros.", flush=True)
    if total == 0:
        return status_map

    tls = local()
    completed = 0
    completed_lock = Lock()

    def get_thread_api() -> ConfirmaFacilAPI:
        if not hasattr(tls, "api"):
            tls.api = ConfirmaFacilAPI()
        return tls.api

    def process_one(item: Tuple[int, Tuple[str, str, str]]) -> Tuple[Tuple[str, str, str], str]:
        idx, (numero_nf, serie, cnpj) = item
        if ENABLE_ROW_LOGS:
            print(
                f"[API] {idx}/{total} consultando NF={numero_nf} SERIE={serie} ESTAB={cnpj}",
                flush=True,
            )
        api = get_thread_api()
        ocorrencias = api.fetch_ocorrencias(numero_nf, serie, cnpj)
        entregue = False
        cancelado = False

        for item_resp in ocorrencias:
            codigo = extract_codigo(item_resp)
            if codigo in ENTREGUE_CODES:
                entregue = True
            if codigo in CANCELADO_CODES:
                cancelado = True

        if entregue:
            status = "ENTREGUE"
        elif cancelado:
            status = "CANCELADO"
        else:
            status = "DESPACHADO"

        if ENABLE_ROW_LOGS:
            print(f"[API] {idx}/{total} concluido -> {status}", flush=True)
        return (numero_nf, serie, cnpj), status

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_one, (idx, chave))
            for idx, chave in enumerate(chaves_extraidas, start=1)
        ]
        for future in as_completed(futures):
            chave, status = future.result()
            status_map[chave] = status
            with completed_lock:
                completed += 1
                if not ENABLE_ROW_LOGS and (completed % 25 == 0 or completed == total):
                    print(f"[API] progresso: {completed}/{total}", flush=True)

    return status_map


def load_chaves_from_sheet() -> Tuple[List[Tuple[str, str, str]], List[str]]:
    if Credentials is None or build is None:
        print("Dependencias do Google Sheets nao encontradas. Instale google-auth e google-api-python-client.")
        return [], []
    if not os.path.exists(CREDENTIALS_PATH):
        print(f"Arquivo de credenciais nao encontrado: {CREDENTIALS_PATH}")
        return [], []
    if SHEET_ID == "COLE_AQUI_O_ID_DA_PLANILHA":
        print("Atualize o SHEET_ID no script antes de executar.")
        return [], []

    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=SHEET_RANGE_INPUT).execute()
    values = result.get("values", [])
    if not values:
        return [], []

    first_col = [row[0] if row else "" for row in values]

    # A linha 1 fica reservada e nao deve ser alterada.
    input_values = first_col[1:] if len(first_col) > 1 else []
    chaves_extraidas = []
    for raw in input_values:
        chave = normalize_chave_nfe(raw)
        numero_nf, serie, cnpj = extract_nfe_fields(chave)
        chaves_extraidas.append((numero_nf, serie, cnpj))

    return chaves_extraidas, first_col


def build_output_range_from_row2(output_range: str, count: int) -> str:
    if "!" in output_range:
        sheet, cols = output_range.split("!", 1)
    else:
        sheet, cols = "", output_range
    col = cols.split(":")[0].strip()
    col_letters = "".join(ch for ch in col if ch.isalpha()) or "B"
    start_row = 2
    end_row = start_row + max(count, 1) - 1
    if sheet:
        return f"{sheet}!{col_letters}{start_row}:{col_letters}{end_row}"
    return f"{col_letters}{start_row}:{col_letters}{end_row}"


def update_status_in_sheet(statuses: List[str]) -> None:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    output_values = [[s] for s in statuses]
    if not output_values:
        print("Nenhum status para atualizar na planilha.")
        return

    output_range = build_output_range_from_row2(SHEET_RANGE_OUTPUT, len(output_values))

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

    chaves_extraidas, raw_values = load_chaves_from_sheet()
    if not raw_values or len(raw_values) <= 1:
        print("Nenhuma chave encontrada no Google Sheets.")
        return

    validas = [(nf, serie, cnpj) for nf, serie, cnpj in chaves_extraidas if nf and serie and cnpj]
    print(
        f"Leitura finalizada: {len(raw_values) - 1} linhas de entrada, {len(validas)} linhas validas para consulta.",
        flush=True,
    )
    chaves_unicas = list(dict.fromkeys(validas))
    print(f"Total de chaves unicas para API: {len(chaves_unicas)}", flush=True)
    status_map = resolver_status(chaves_unicas)

    statuses = []
    input_values = raw_values[1:]
    for idx, (raw, (nf, serie, cnpj)) in enumerate(zip(input_values, chaves_extraidas), start=2):
        chave_normalizada = normalize_chave_nfe(raw)
        if not chave_normalizada:
            statuses.append("")
            if ENABLE_ROW_LOGS:
                print(f"Linha {idx}: chave vazia -> status em branco")
        elif not nf or not serie or not cnpj:
            statuses.append("DESPACHADO")
            if ENABLE_ROW_LOGS:
                print(f"Linha {idx}: chave invalida ({chave_normalizada}) -> DESPACHADO")
        else:
            status = status_map.get((nf, serie, cnpj), "DESPACHADO")
            statuses.append(status)
            if ENABLE_ROW_LOGS:
                print(f"Linha {idx}: NF {nf} | SERIE {serie} | ESTAB {cnpj} -> {status}")

    update_status_in_sheet(statuses)

    rows = [
        {
            "NF": nf,
            "SERIE": serie,
            "ESTABELECIMENTO": cnpj,
            "STATUS": status_map.get((nf, serie, cnpj), "DESPACHADO"),
        }
        for nf, serie, cnpj in validas
    ]

    df_final = pd.DataFrame(rows)
    if df_final.empty:
        print("Nada para salvar.")
        return

    output_path = os.path.join(BASE_PATH, OUTPUT_NAME)
    save_excel_safely(df_final, output_path)


if __name__ == "__main__":
    main()
