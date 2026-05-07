# utils/sheets.py
import os
import json
import time
import random
import base64
import re
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Google / GSpread helpers
# =========================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# cache por proceso (gunicorn worker / rq worker)
# Al inicio del archivo, junto a _GSPREAD_CLIENT
_GSPREAD_CLIENT = None
_SPREADSHEET_CACHE: Dict[str, Any] = {}   # ← AGREGAR
_SPREADSHEET_CACHE_TS: Dict[str, float] = {}  # ← AGREGAR
_SHEET_CACHE_TTL = 120  # segundos         # ← AGREGAR

def with_backoff(fn, *args, retries: int = 6, base: float = 0.8, jitter: float = 0.35, **kwargs):
    """
    Ejecuta una función con reintentos y backoff exponencial.
    Útil para gspread/Google API (429, 5xx, timeouts).
    """
    last_exc = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            # backoff
            sleep_s = (base * (2 ** i)) * (1.0 + random.random() * jitter)
            time.sleep(min(sleep_s, 10.0))
    raise last_exc


def _maybe_base64(s: str) -> bool:
    s = s.strip()
    if len(s) < 40:
        return False
    # Heurística simple: base64 suele ser largo y sin llaves
    return ("{" not in s and "}" not in s and "\n" not in s and " " not in s)


def _strip_wrapping_quotes(raw: str) -> str:
    raw = raw.strip()
    if len(raw) >= 2 and ((raw[0] == raw[-1] == '"') or (raw[0] == raw[-1] == "'")):
        return raw[1:-1].strip()
    return raw


def _unescape_if_needed(raw: str) -> str:
    """
    Corrige casos típicos de Render/Copy-Paste:
    - JSON dentro de comillas: "\"{...}\""  -> { ... }
    - JSON con comillas escapadas: {\\\"type\\\":...} -> {"type":...}
    """
    raw = raw.strip()

    # Caso 1: es un JSON-string (comienza y termina con comillas) => json.loads lo “des-escapa”
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        try:
            maybe = json.loads(raw)
            if isinstance(maybe, str):
                raw = maybe.strip()
        except Exception:
            raw = _strip_wrapping_quotes(raw)

    # Caso 2: tiene \" por todos lados (típico cuando pegan JSON escapado)
    if raw.startswith("{\\\"") or '\\"type\\"' in raw or '\\"private_key\\"' in raw:
        raw = raw.replace('\\"', '"')

    return raw.strip()


def _fix_private_key_newlines(info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Google auth necesita private_key con saltos reales '\n'.
    Si viene como '\\n' lo convertimos.
    Si viene con saltos reales pero el JSON venía inválido, aquí ya quedó como dict.
    """
    pk = info.get("private_key")
    if isinstance(pk, str):
        # convierte \n literal a newline real
        if "\\n" in pk:
            pk = pk.replace("\\n", "\n")
        # limpia espacios raros
        pk = pk.strip()
        info["private_key"] = pk
    return info


def _repair_json_private_key(raw: str) -> str:
    """
    Si te pegaron el JSON con private_key en múltiples líneas reales dentro de comillas,
    json.loads falla. Esta rutina intenta “escapar” SOLO el bloque de private_key.
    """
    if '"private_key"' not in raw or "-----BEGIN PRIVATE KEY-----" not in raw:
        return raw

    # busca el valor de private_key entre comillas (aunque tenga newlines reales)
    m = re.search(r'"private_key"\s*:\s*"(.*?)"\s*,\s*"client_email"', raw, flags=re.S)
    if not m:
        return raw

    key_block = m.group(1)
    escaped = key_block.replace("\r\n", "\n").replace("\n", "\\n")
    repaired = raw[:m.start(1)] + escaped + raw[m.end(1):]
    return repaired


def _load_service_account_info() -> Dict[str, Any]:
    """
    Carga credenciales desde ENV.
    Soporta:
      - GOOGLE_CREDENTIALS_JSON (principal)
      - GOOGLE_CREDENTIALS (fallback)
      - GOOGLE_APPLICATION_CREDENTIALS (ruta a archivo, si algún día lo usas)
    """
    raw = (os.environ.get("GOOGLE_CREDENTIALS_JSON") or os.environ.get("GOOGLE_CREDENTIALS") or "").strip()

    # fallback a archivo si existiera (no es tu caso, pero lo dejamos soportado)
    file_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not raw and file_path:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()

    if not raw:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON (o GOOGLE_CREDENTIALS) en variables de entorno.")

    raw = _unescape_if_needed(raw)

    # Base64 (opcional)
    if _maybe_base64(raw):
        try:
            decoded = base64.b64decode(raw).decode("utf-8", errors="replace").strip()
            raw = _unescape_if_needed(decoded)
        except Exception:
            # si falla, seguimos intentando como texto normal
            pass

    # Primer intento: json.loads directo
    try:
        info = json.loads(raw)
        if isinstance(info, str):
            # caso raro: json devolvió string => parsear 2da vez
            info = json.loads(info)
        if not isinstance(info, dict):
            raise ValueError("Credenciales no son un objeto JSON.")
        return _fix_private_key_newlines(info)
    except Exception:
        pass

    # Segundo intento: reparar private_key multiline
    try:
        raw2 = _repair_json_private_key(raw)
        info = json.loads(raw2)
        if isinstance(info, str):
            info = json.loads(info)
        if not isinstance(info, dict):
            raise ValueError("Credenciales no son un objeto JSON.")
        return _fix_private_key_newlines(info)
    except Exception:
        pass

    # Tercer intento: si venía como dict de Python (muy raro), intentamos literal_eval con cuidado
    # (sin meter ast aquí para no recrear tu error anterior)
    # Mejor: dar error claro.
    sample = raw[:180].replace("\n", "\\n")
    raise RuntimeError(
        "GOOGLE_CREDENTIALS_JSON no se pudo parsear como JSON válido. "
        "Tip: pega el JSON completo del service account (como viene en el archivo .json), "
        "sin comillas extra. Muestra (primeros caracteres): " + sample
    )


def get_gspread_client():
    global _GSPREAD_CLIENT
    if _GSPREAD_CLIENT is not None:
        return _GSPREAD_CLIENT

    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _GSPREAD_CLIENT = gspread.authorize(creds)
    return _GSPREAD_CLIENT


def open_spreadsheet(sheet_name: str):
    if not sheet_name:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")
    gc = get_gspread_client()
    
    now = time.time()
    if sheet_name in _SPREADSHEET_CACHE:
        if now - _SPREADSHEET_CACHE_TS[sheet_name] < _SHEET_CACHE_TTL:
            return _SPREADSHEET_CACHE[sheet_name]
    
    sh = with_backoff(gc.open, sheet_name)
    _SPREADSHEET_CACHE[sheet_name] = sh
    _SPREADSHEET_CACHE_TS[sheet_name] = now
    return sh

def open_worksheet(sh, tab_name: str):
    if not tab_name:
        raise RuntimeError("Falta nombre de pestaña (TAB_...).")
    return with_backoff(sh.worksheet, tab_name)


# =========================
# Sheet data helpers
# =========================

def build_header_map(ws) -> Dict[str, int]:
    headers = with_backoff(ws.row_values, 1)
    hmap: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if key and key not in hmap:
            hmap[key] = i
    return hmap


def col_idx(hmap: Dict[str, int], name: str) -> Optional[int]:
    if not name:
        return None
    name = name.strip()
    if name in hmap:
        return hmap[name]
    # case-insensitive fallback
    low = name.lower()
    for k, v in hmap.items():
        if k.lower() == low:
            return v
    return None


def get_all_values_safe(ws) -> List[List[str]]:
    try:
        return with_backoff(ws.get_all_values)
    except Exception:
        return []


def row_to_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    out = {}
    for i, h in enumerate(headers):
        key = (h or "").strip()
        if not key:
            continue
        out[key] = (row[i] if i < len(row) else "").strip()
    return out


def find_row_by_col_value(values: List[List[str]], col_name: str, value: str) -> Optional[int]:
    """
    values: salida de get_all_values (incluye header en fila 0)
    regresa índice de fila (0-based dentro de values) o None
    """
    if not values or len(values) < 2:
        return None
    headers = values[0]
    try:
        c = headers.index(col_name)
    except ValueError:
        # fallback case-insensitive
        c = None
        for i, h in enumerate(headers):
            if (h or "").strip().lower() == (col_name or "").strip().lower():
                c = i
                break
        if c is None:
            return None

    target = (value or "").strip()
    for i in range(1, len(values)):
        row = values[i]
        cell = (row[c] if c < len(row) else "").strip()
        if cell == target:
            return i
    return None


def find_row_by_value(ws, col_name: str, value: str, hmap: Optional[Dict[str, int]] = None) -> Optional[int]:
    """
    Busca por valor exacto en una columna. Devuelve número de fila (1-based de Sheets) o None.
    """
    if hmap is None:
        hmap = build_header_map(ws)

    c = col_idx(hmap, col_name)
    if not c:
        return None

    col_vals = with_backoff(ws.col_values, c)
    target = (value or "").strip()

    # col_vals incluye header en index 0 (fila 1)
    for r in range(2, len(col_vals) + 1):  # fila real en sheets
        cell = (col_vals[r - 1] or "").strip()
        if cell == target:
            return r
    return None


def update_row_cells(ws, row_num: int, mapping: Dict[str, Any], hmap: Optional[Dict[str, int]] = None):
    """
    Actualiza varias celdas en una fila usando update_cells (batch).
    """
    if not row_num or row_num < 2:
        return

    if hmap is None:
        hmap = build_header_map(ws)

    cells = []
    for k, v in (mapping or {}).items():
        c = col_idx(hmap, k)
        if not c:
            continue
        val = "" if v is None else str(v)
        cells.append(gspread.Cell(row=row_num, col=c, value=val))

    if not cells:
        return

    with_backoff(ws.update_cells, cells, value_input_option="USER_ENTERED")
