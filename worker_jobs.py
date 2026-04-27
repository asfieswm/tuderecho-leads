# worker_jobs.py
import os
import re
import uuid
import json
from datetime import datetime, date
from zoneinfo import ZoneInfo

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from utils.sheets import (
    open_spreadsheet, open_worksheet, with_backoff,
    build_header_map, col_idx, find_row_by_value, update_row_cells,
    get_all_values_safe, row_to_dict
)

MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City").strip() or "America/Mexico_City")

GOOGLE_SHEET_NAME        = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS                = os.environ.get("TAB_LEADS",           "BD_Leads").strip()
TAB_ABOG                 = os.environ.get("TAB_ABOGADOS",        "Cat_Abogados").strip()
TAB_SYS                  = os.environ.get("TAB_SYS",             "Config_Sistema").strip()
TAB_ABOG_ADMIN           = os.environ.get("TAB_ABOGADOS_ADMIN",  "Abogados_Admin").strip()
TAB_CONOCIMIENTO_AI      = os.environ.get("TAB_CONOCIMIENTO_AI", "Conocimiento_AI").strip()

TWILIO_ACCOUNT_SID       = os.environ.get("TWILIO_ACCOUNT_SID",       "").strip()
TWILIO_AUTH_TOKEN        = os.environ.get("TWILIO_AUTH_TOKEN",        "").strip()
TWILIO_WHATSAPP_NUMBER   = os.environ.get("TWILIO_WHATSAPP_NUMBER",   "").strip()

# Plantilla aprobada: Nuevo caso asignado (abogada) — 3 variables:
# {{1}} Cliente  {{2}} Teléfono  {{3}} Detalle del caso
WA_TPL_ABOGADA_NUEVO_CASO_SID = os.environ.get("WA_TPL_ABOGADA_NUEVO_CASO_SID", "").strip()

OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY",  "").strip()
OPENAI_MODEL    = os.environ.get("OPENAI_MODEL",    "gpt-4o-mini").strip()

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# ─────────────────────────────────────────────
# Helpers generales
# ─────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")


def _wa_addr(raw: str) -> str:
    t = (raw or "").strip()
    if not t:
        return ""
    return t if t.startswith("whatsapp:") else "whatsapp:" + t


def _get_twilio_client() -> Client:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        raise RuntimeError("Faltan credenciales de Twilio (TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN).")
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def _to_e164(raw: str) -> str:
    """
    Normaliza teléfono a E.164.
    Acepta: +52..., 521..., 55..., whatsapp:+52...
    """
    s = (raw or "").strip()
    if not s:
        return ""
    s = "".join(c for c in s if c.isdigit() or c == "+")
    if not s:
        return ""
    if s.startswith("whatsapp:"):
        s = s.replace("whatsapp:", "")
    if not s.startswith("+"):
        s = s.lstrip("0")
        if not s.startswith("52"):
            s = "52" + s
        s = "+" + s
    return s


def _to_e164_no_plus(raw: str) -> str:
    """
    Igual que _to_e164 pero SIN el prefijo '+'.
    Formato requerido por AppSheet en Telefono_Normalizado.
    Ej: +5215512345678 → 5215512345678
    """
    e = _to_e164(raw)
    return e.lstrip("+") if e else ""


def send_whatsapp_safe(to_number: str, body: str) -> tuple[bool, str]:
    """Envía un mensaje WhatsApp de sesión. No lanza excepción; retorna (ok, detalle)."""
    try:
        if not TWILIO_WHATSAPP_NUMBER:
            return False, "Falta TWILIO_WHATSAPP_NUMBER."
        client = _get_twilio_client()
        to_e164 = to_number if str(to_number).startswith("whatsapp:") else _to_e164(to_number)
        msg = client.messages.create(
            from_=_wa_addr(TWILIO_WHATSAPP_NUMBER),
            to=_wa_addr(to_e164),
            body=body,
        )
        return True, f"SID={getattr(msg, 'sid', '')}"
    except TwilioRestException as e:
        return False, f"TwilioRestException {getattr(e, 'code', '')}: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _clean_var(v) -> str:
    """Limpia una variable para usar en plantillas Twilio (sin saltos de línea ni chars de control)."""
    s = "" if v is None else str(v)
    s = s.replace("\r", " ").replace("\n", " ").strip()
    s = re.sub(r"[\x00-\x1F\x7F]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def send_whatsapp_template_safe(
    to_number: str,
    template_sid: str,
    variables: dict,
) -> tuple[bool, str]:
    """Envía WhatsApp usando plantilla de Twilio Content API. Retorna (ok, detalle)."""
    try:
        if not TWILIO_WHATSAPP_NUMBER:
            return False, "Falta TWILIO_WHATSAPP_NUMBER."
        if not template_sid:
            return False, "Falta template SID (content_sid)."
        to_e164 = _to_e164(to_number)
        if not to_e164:
            return False, "Número destino inválido."
        payload = {str(k): _clean_var(v) for k, v in (variables or {}).items()}
        content_vars = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        client = _get_twilio_client()
        msg = client.messages.create(
            from_=_wa_addr(TWILIO_WHATSAPP_NUMBER),
            to=_wa_addr(to_e164),
            content_sid=template_sid,
            content_variables=content_vars,
        )
        return True, f"SID={getattr(msg, 'sid', '')}"
    except TwilioRestException as e:
        return False, f"TwilioRestException {getattr(e, 'code', '')}: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ─────────────────────────────────────────────
# Conversión de tipos
# ─────────────────────────────────────────────

def money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except Exception:
        return 0.0


def safe_int(s: str) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return 0


def safe_float(s: str) -> float:
    try:
        return float(str(s).strip())
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# Texto
# ─────────────────────────────────────────────

def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-záéíóúüñ0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clip_chars(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= max_chars else s[:max_chars].rstrip() + "…"


def _clip_words(text: str, max_words: int) -> str:
    words = (text or "").strip().split()
    if len(words) <= max_words:
        return (text or "").strip()
    return " ".join(words[:max_words]).rstrip() + "…"


# ─────────────────────────────────────────────
# Google Sheets — helpers
# ─────────────────────────────────────────────

def _safe_update(ws, row_num: int, payload: dict, hmap: dict) -> None:
    """Actualiza solo las columnas que existen en la hoja."""
    if not payload:
        return
    clean = {k: v for k, v in payload.items() if k in hmap}
    if clean:
        update_row_cells(ws, row_num, clean, hmap=hmap)


def read_sys_config(ws_sys) -> dict:
    values = get_all_values_safe(ws_sys)
    if not values or len(values) < 2:
        return {}
    hdr = values[0]
    out = {}
    for r in values[1:]:
        d = row_to_dict(hdr, r)
        k = (d.get("Clave") or "").strip()
        v = (d.get("Valor") or "").strip()
        if k:
            out[k] = v
    return out


def set_sys_value(ws_sys, key: str, value: str) -> None:
    """Escribe o actualiza una clave en Config_Sistema (columnas Clave / Valor)."""
    key = (key or "").strip()
    if not key:
        return
    values = get_all_values_safe(ws_sys)
    if not values:
        with_backoff(ws_sys.append_row, ["Clave", "Valor"], value_input_option="RAW")
        values = get_all_values_safe(ws_sys)
    hdr = values[0]
    if "Clave" not in hdr or "Valor" not in hdr:
        return
    for i in range(1, len(values)):
        d = row_to_dict(hdr, values[i])
        if (d.get("Clave") or "").strip() == key:
            hmap = build_header_map(ws_sys)
            update_row_cells(ws_sys, i + 1, {"Valor": str(value)}, hmap=hmap)
            return
    with_backoff(ws_sys.append_row, [key, str(value)], value_input_option="RAW")


# ─────────────────────────────────────────────
# Abogados
# ─────────────────────────────────────────────

def list_active_abogados(ws_abog) -> list[tuple[str, str, str]]:
    """
    Retorna lista de (ID, Nombre, Telefono) de abogados activos.
    Compatible con encabezados: Abogado | Nombre_Abogado | Telefono_Aboga | Activo
    """
    h = build_header_map(ws_abog)
    rows = with_backoff(ws_abog.get_all_values)
    if not rows or len(rows) < 2:
        return []

    def get_any(r, names):
        for name in names:
            if name in h:
                c = col_idx(h, name)
                val = (r[c - 1] if c and c - 1 < len(r) else "").strip()
                if val:
                    return val
        return ""

    def is_active(r):
        return get_any(r, ["Activo"]).upper() in ("SI", "SÍ", "TRUE", "1", "YES", "Y")

    out = []
    for r in rows[1:]:
        aid = get_any(r, ["ID_Abogado", "Abogado", "ID"])
        if not aid or not is_active(r):
            continue
        nombre = (
            get_any(r, ["Nombre_Abogado", "Nombre_Abogada", "Nombre_Abogad", "Nombre"])
            or f"Abogada {aid}"
        )
        tel = get_any(r, [
            "Telefono_Aboga", "Telefono_Abogado",
            "Teléfono_Registro", "Telefono_Registro", "Telefono",
        ])
        out.append((aid, nombre, tel))

    out.sort(key=lambda x: x[0])
    return out


def pick_abogado_secuencial(
    ws_abog, ws_sys, salario_mensual: float, syscfg: dict
) -> tuple[str, str, str]:
    """
    Asignación round-robin. VIP: A01 si salario >= 50 000.
    Persiste el último ID asignado en Config_Sistema (clave ABOGADO_ULTIMO_ID).
    """
    activos = list_active_abogados(ws_abog)
    if not activos:
        return "A01", "Abogada asignada", ""

    def by_id(aid):
        for x in activos:
            if x[0] == aid:
                return x
        return None

    if salario_mensual >= 50_000:
        a01 = by_id("A01")
        return a01 if a01 else activos[0]

    last_id = (syscfg.get("ABOGADO_ULTIMO_ID") or "").strip()
    ids = [a[0] for a in activos]

    if last_id in ids:
        nxt = activos[(ids.index(last_id) + 1) % len(activos)]
    else:
        nxt = activos[0]

    try:
        set_sys_value(ws_sys, "ABOGADO_ULTIMO_ID", nxt[0])
    except Exception:
        pass

    return nxt


# ─────────────────────────────────────────────
# Cálculo de indemnización
# ─────────────────────────────────────────────

def years_of_service(ini: date, fin: date) -> float:
    days = max((fin - ini).days, 0)
    return days / 365.0 if days else 0.0


def vacation_days_by_years(y: int) -> int:
    if y <= 0:  return 0
    if y == 1:  return 12
    if y == 2:  return 14
    if y == 3:  return 16
    if y == 4:  return 18
    if y == 5:  return 20
    extra_blocks = (y - 6) // 5 + 1
    return 20 + 2 * extra_blocks


def _last_anniversary(ini: date, fin: date) -> date:
    try:
        ann = date(fin.year, ini.month, ini.day)
    except ValueError:
        ann = date(fin.year, ini.month, min(ini.day, 28))
    if ann > fin:
        try:
            ann = date(fin.year - 1, ini.month, ini.day)
        except ValueError:
            ann = date(fin.year - 1, ini.month, min(ini.day, 28))
    return max(ann, ini)


def calc_estimacion_detallada(
    tipo_caso: str,
    salario_mensual: float,
    ini: date,
    fin: date,
    salario_min_diario: float = 0.0,
) -> tuple[str, float, dict]:
    """
    Retorna (texto_desglose, total_estimado, componentes_dict).
    tipo_caso: "1" = despido injustificado, "2" = renuncia.
    """
    sd  = salario_mensual / 30.0 if salario_mensual else 0.0
    y   = years_of_service(ini, fin)
    y_int = int(y) if y > 0 else 0

    # Aguinaldo proporcional
    start_year      = date(fin.year, 1, 1)
    days_agu        = max((fin - start_year).days + 1, 0)
    aguinaldo_prop  = sd * 15 * (days_agu / 365.0) if sd else 0.0

    # Vacaciones proporcionales
    vac_from         = _last_anniversary(ini, fin)
    days_vac_period  = max((fin - vac_from).days + 1, 0)
    vac_days_base    = vacation_days_by_years(max(y_int, 1) if y > 0 else 0)
    vacaciones_prop  = sd * vac_days_base * (days_vac_period / 365.0) if sd else 0.0
    prima_vac_prop   = vacaciones_prop * 0.25

    # Prima de antigüedad (topada a 2 salarios mínimos diarios cuando aplica)
    sd_top    = sd
    if salario_min_diario and salario_min_diario > 0:
        sd_top = min(sd, 2.0 * salario_min_diario)
    prima_ant = sd_top * 12.0 * y if (sd_top and y > 0) else 0.0

    ind_90 = 0.0
    ind_20 = 0.0

    if str(tipo_caso).strip() == "1":
        # ── Despido injustificado ──
        ind_90 = sd * 90.0
        total  = ind_90 + prima_ant + aguinaldo_prop + vacaciones_prop + prima_vac_prop

        desglose = (
            "DESGLOSE DETALLADO (REFERENCIAL)\n"
            f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
            f"- Salario diario (SD aprox): ${sd:,.2f}\n"
            f"- Antigüedad estimada: {y:.2f} años\n\n"
            "INDEMNIZACIÓN (DESPIDO)\n"
            f"- 3 meses (90 días): ${ind_90:,.2f}\n"
            f"- Prima de antigüedad (12 días/año, topada si aplica): ${prima_ant:,.2f}\n\n"
            "PRESTACIONES PROPORCIONALES\n"
            f"- Aguinaldo proporcional (desde {start_year.isoformat()}): ${aguinaldo_prop:,.2f}\n"
            f"- Vacaciones proporcionales (desde {vac_from.isoformat()} / {vac_days_base} días/año): ${vacaciones_prop:,.2f}\n"
            f"- Prima vacacional proporcional (25%): ${prima_vac_prop:,.2f}\n\n"
            f"TOTAL ESTIMADO: ${total:,.2f}\n\n"
            "Nota: el monto puede variar por salario integrado real, prestaciones adicionales, "
            "salarios caídos, topes vigentes y documentación."
        )
    else:
        # ── Renuncia ──
        prima_ant_ren = prima_ant if y >= 15 else 0.0
        total = aguinaldo_prop + vacaciones_prop + prima_vac_prop + prima_ant_ren

        desglose = (
            "DESGLOSE DETALLADO (REFERENCIAL)\n"
            f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
            f"- Salario diario (SD aprox): ${sd:,.2f}\n"
            f"- Antigüedad estimada: {y:.2f} años\n\n"
            "FINIQUITO (RENUNCIA)\n"
            f"- Aguinaldo proporcional (desde {start_year.isoformat()}): ${aguinaldo_prop:,.2f}\n"
            f"- Vacaciones proporcionales (desde {vac_from.isoformat()} / {vac_days_base} días/año): ${vacaciones_prop:,.2f}\n"
            f"- Prima vacacional proporcional (25%): ${prima_vac_prop:,.2f}\n"
            + (f"- Prima de antigüedad (≥15 años): ${prima_ant_ren:,.2f}\n" if prima_ant_ren else "")
            + f"\nTOTAL ESTIMADO: ${total:,.2f}\n\n"
            "Nota: el monto puede variar según recibos, prestaciones reales y pagos pendientes."
        )

    componentes = {
        "Indemnizacion_90": ind_90,
        "Indemnizacion_20": ind_20,
        "Prima_Antiguedad": prima_ant,
        "Aguinaldo_Prop":   aguinaldo_prop,
        "Vacaciones_Prop":  vacaciones_prop,
        "Prima_Vac_Prop":   prima_vac_prop,
        "Vac_Dias_Base":    vac_days_base,
    }
    return desglose, total, componentes


# ─────────────────────────────────────────────
# Fechas — parseo seguro con día por defecto = 1
# ─────────────────────────────────────────────

def _parse_date_parts_safe(h: dict, vals: list, prefix: str) -> date:
    """
    Lee Año y Mes de la hoja. El Día es opcional: si está vacío o ausente usa 1.
    Garantiza que el día sea válido para cualquier mes (máximo 28 en modo seguro).
    No lanza excepción por día inválido; solo por año o mes fuera de rango.
    """
    def get(name: str) -> str:
        c = col_idx(h, name)
        return (vals[c - 1] if c and c - 1 < len(vals) else "").strip()

    y = safe_int(get(f"{prefix}_Anio"))
    m = safe_int(get(f"{prefix}_Mes"))

    d_raw = get(f"{prefix}_Dia")
    d = safe_int(d_raw) if d_raw else 1

    if y < 1900 or y > 2100:
        raise ValueError(f"{prefix}: año inválido ({y}). Escribe un año como 2020 o 2024.")
    if m < 1 or m > 12:
        raise ValueError(f"{prefix}: mes inválido ({m}). Escribe un número del 1 al 12.")

    # Clamp al día seguro para todos los meses (evita date(año, 2, 29) en años no bisiestos)
    d = max(1, min(d, 28))
    return date(y, m, d)


# ─────────────────────────────────────────────
# Conocimiento_AI — carga y selección
# ─────────────────────────────────────────────

def load_conocimiento(ws_con) -> list[dict]:
    """
    Carga todas las filas activas de la hoja Conocimiento_AI.
    Columnas esperadas: ID_Tema, Titulo_Visible, Contenido_Legal,
                        Palabras_Clave, Fuente, Contexto_Uso, Prioridad, Activo
    Las columnas nuevas (Contexto_Uso, Prioridad, Activo) son opcionales:
    si no existen en la hoja se usan valores por defecto.
    """
    values = get_all_values_safe(ws_con)
    if not values or len(values) < 2:
        return []
    hdr = values[0]
    out = []
    for r in values[1:]:
        d = row_to_dict(hdr, r)

        # Columna Activo: si no existe en la hoja asumimos que está activo
        activo = (d.get("Activo") or "SI").strip().upper()
        if activo not in ("SI", "SÍ", "1", "TRUE", "YES", "Y"):
            continue

        # ✅ FIX: si no existe Contexto_Uso, usa Fuente como fallback
        contexto_uso = (d.get("Contexto_Uso") or "").strip().upper()
        if not contexto_uso:
            fuente = (d.get("Fuente") or "").strip().upper()
            if fuente in ("CONVERSACIONAL", "ANALISIS", "AMBOS"):
                contexto_uso = fuente
            else:
                contexto_uso = "AMBOS"

        out.append({
            "ID_Tema":         (d.get("ID_Tema")          or "").strip(),
            "Titulo_Visible":  (d.get("Titulo_Visible")   or "").strip(),
            "Contenido_Legal": (d.get("Contenido_Legal")  or "").strip(),
            "Palabras_Clave":  (d.get("Palabras_Clave")   or "").strip(),
            "Fuente":          (d.get("Fuente")            or "").strip(),
            "Contexto_Uso":    contexto_uso,
            "Prioridad":       safe_int(d.get("Prioridad") or "5"),
        })
    return out


def select_conocimiento(
    con_rows: list[dict],
    descripcion: str,
    tipo_caso: str,
    k: int = 3,
    contexto: str = "ANALISIS",
) -> list[dict]:
    """
    Filtra y rankea filas de Conocimiento_AI por relevancia.
    """
    desc_n = _normalize_text(descripcion)
    tokens = {t for t in desc_n.split() if len(t) >= 4}

    if str(tipo_caso).strip() == "1":
        tokens |= {"despido", "indemnizacion", "indemnización",
                   "finiquito", "rescision", "rescisión"}
    elif str(tipo_caso).strip() == "2":
        tokens |= {"renuncia", "finiquito", "prestaciones",
                   "vacaciones", "aguinaldo"}

    scored = []
    for row in con_rows:
        row_ctx = row.get("Contexto_Uso", "AMBOS")
        if contexto != "AMBOS" and row_ctx not in (contexto, "AMBOS"):
            continue

        keys = _normalize_text(row.get("Palabras_Clave", ""))
        key_list = [x.strip() for x in re.split(r"[;,]", keys) if x.strip()]

        score = 0.0
        for kw in key_list:
            if kw and (kw in desc_n or kw in tokens):
                score += 2

        title = _normalize_text(row.get("Titulo_Visible", ""))
        for t in tokens:
            if t in title:
                score += 1

        score -= row.get("Prioridad", 5) / 10.0

        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:k]]


# ─────────────────────────────────────────────
# AI — Respuesta empática inmediata
# ─────────────────────────────────────────────

def build_respuesta_empatica(
    descripcion: str,
    con_rows: list[dict],
    tipo_caso_hint: str = "",
) -> str:
    """
    Genera una respuesta conversacional empática y COMPLETAMENTE PERSONALIZADA
    basada en lo que el lead escribió. La IA extrae detalles concretos del relato
    (empresa, situación específica, emociones expresadas) para que el lead sienta
    que fue leído de verdad, no que recibió una respuesta genérica.

    Si OpenAI falla, usa un fallback con copywriting persuasivo directo.
    """
    temas_conv = select_conocimiento(
        con_rows, descripcion, tipo_caso_hint, k=2, contexto="CONVERSACIONAL"
    )

    contexto_txt = ""
    for t in temas_conv:
        contenido = _clip_chars((t.get("Contenido_Legal") or "").strip(), 300)
        if contenido:
            contexto_txt += f"- {contenido}\n"
    contexto_txt = contexto_txt.strip()

    # ── Fallback sin OpenAI ──
    def fallback() -> str:
        if str(tipo_caso_hint).strip() == "1":
            cuerpo = (
                "He leído con mucha atención lo que nos cuentas. "
                "Entiendo perfectamente la frustración y el estrés que genera un despido; "
                "es una situación que desestabiliza a cualquiera y es súper válido que te sientas así.\n\n"
                "Quiero darte tranquilidad: lo que viviste tiene solución legal y la ley está de tu lado "
                "para recuperar lo que por derecho te corresponde.\n\n"
                "Para mí es muy importante entender tu prioridad. ¿Qué te genera más inquietud ahorita: "
                "saber exactamente cuánto dinero te deben, o entender cómo es el proceso para reclamarlo?\n\n"
                "Cuéntame con confianza. Conectarte con una de nuestras abogadas especializadas "
                "puede marcar toda la diferencia para que no te vayas con las manos vacías.\n\n"
            )
        elif str(tipo_caso_hint).strip() == "2":
            cuerpo = (
                "Gracias por la confianza de detallarme tu situación. "
                "Sé que tomar la decisión de renunciar (o verse presionado a hacerlo) "
                "es muy desgastante y genera muchas dudas.\n\n"
                "Lo que muchas empresas no dicen es que, incluso al renunciar, "
                "conservas derechos irrenunciables que te tienen que pagar sí o sí.\n\n"
                "¿Qué es lo que más te preocupa en este momento: que no te quieran pagar tu finiquito completo, "
                "los tiempos que se están tomando, o alguna otra actitud de la empresa?\n\n"
                "Dime qué piensas. Que una abogada revise tu caso sin costo te ayudará a "
                "dar el siguiente paso con total seguridad.\n\n"
            )
        else:
            cuerpo = (
                "Agradezco mucho que te tomes el tiempo de contarnos los detalles. "
                "Situaciones como la que describes generan muchísima incertidumbre, pero "
                "quiero que sepas que estamos aquí para darte claridad.\n\n"
                "Lo que estás pasando es más común de lo que crees y, lo más importante, tiene solución legal.\n\n"
                "De todo esto, ¿qué es lo que más te quita el sueño en este momento: "
                "los tiempos del proceso, el tema económico, o saber si tienes las pruebas suficientes?\n\n"
                "Platícame. Recuerda que la orientación correcta de una abogada puede cambiar por completo "
                "el resultado a tu favor.\n\n"
            )
        return cuerpo + "Para poder perfilar tu caso exacto, solo confírmame:\n\n¿Fue un despido (1) o presentaste tu renuncia (2)?"

    if not (OPENAI_API_KEY and OpenAI):
        return fallback()

    desc_recortada = _clip_chars(descripcion, 800)

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)

        system_prompt = (
            "Eres Ximena, asistente legal de Tu Derecho Laboral México. "
            "Atiendes por WhatsApp a personas que acaban de vivir un problema laboral. "
            "Tu ÚNICA misión en este mensaje es que la persona sienta que REALMENTE leíste "
            "su historia y que quiera seguir hablando contigo.\n\n"

            "━━ LO MÁS IMPORTANTE ━━\n"
            "Lee con atención TODO lo que escribió el lead y EXTRAE detalles concretos:\n"
            "  • Si mencionó una empresa → úsala por su nombre\n"
            "  • Si mencionó años trabajando → menciónalos\n"
            "  • Si mencionó una situación específica (acoso, cambio de turno, presión para renunciar, "
            "    jefe que los llamó, carta que les dieron) → refierete exactamente a eso\n"
            "  • Si expresó una emoción (miedo, coraje, tristeza, desesperación) → valídala con esas mismas palabras\n"
            "Nunca respondas de forma genérica. Si el lead lo mencionó, úsalo.\n\n"

            "━━ TONO Y ESTILO ━━\n"
            "- Cálido, directo, persuasivo. Como una amiga que sabe de leyes, no un corporativo.\n"
            "- PROHIBIDO: 'Lamento mucho tu situación', 'Es importante que sepas', 'Comprendo tu situación', "
            "  'Abordar esta problemática', cualquier frase de cajón.\n"
            "- SÍ usar: 'He leído con atención', 'Eso que describes es...', 'Que te hayan hecho eso de [detalle] "
            "  es completamente injusto', 'La ley te protege en esto', 'Cuéntame', 'Dime', 'Platícame'.\n"
            "- Háblale de tú. Sin Markdown, sin asteriscos, texto plano para WhatsApp.\n"
            "- Máximo 5 párrafos cortos con doble salto de línea entre ellos.\n\n"

            "━━ ESTRUCTURA (en este orden exacto) ━━\n"
            "1. ESPEJO: Demuestra que leíste su caso. Menciona AL MENOS 2 detalles concretos "
            "   de lo que escribió. Valida la emoción que expresaron. (2-3 oraciones)\n"
            "2. VALIDACIÓN LEGAL: Una sola oración tajante: la ley los protege y lo que vivieron "
            "   tiene solución. Usa el detalle de su caso, no palabras genéricas.\n"
            "3. PREGUNTA INCISIVA: Una pregunta abierta específica para SU situación "
            "   (no genérica). Que invite a dar más detalles o a desahogarse. (1 oración)\n"
            "4. CTA SUAVE: Reforza el valor de hablar con una abogada sin costo. (1 oración)\n"
            "5. CIERRE FIJO (SIEMPRE la última línea, sin cambios):\n"
            "   ¿Fue un despido (1) o presentaste tu renuncia (2)?"
        )

        user_prompt = (
            f"El lead escribió esto sobre su situación:\n\n"
            f"\"{desc_recortada}\"\n\n"
        )
        if contexto_txt:
            user_prompt += (
                f"Contexto legal de apoyo (úsalo solo si encaja de forma muy natural, "
                f"nunca lo cites textualmente ni lo fuerces):\n{contexto_txt}\n\n"
            )
        user_prompt += (
            "Escribe la respuesta siguiendo la estructura del system prompt.\n"
            "RECUERDA: el paso 1 (ESPEJO) es el más importante. Menciona detalles CONCRETOS "
            "de lo que escribió: empresa, tiempo trabajado, situación específica, emoción expresada. "
            "Si no hay suficiente detalle en lo que escribieron, usa lo que sí hay y haz la "
            "pregunta incisiva para obtener más. Nunca suenes genérica."
        )

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.85,   # más creatividad / personalización
            max_tokens=380,     # un poco más de espacio para el detalle
        )

        txt = (resp.choices[0].message.content or "").strip()
        if not txt:
            return fallback()

        # Garantizar que la pregunta de cierre siempre esté presente
        pregunta_cierre = "¿Fue un despido (1) o presentaste tu renuncia (2)?"
        if pregunta_cierre not in txt:
            txt = txt.rstrip() + "\n\n" + pregunta_cierre

        return txt

    except Exception:
        return fallback()


# ─────────────────────────────────────────────
# AI — Análisis web final (~250 palabras)
# ─────────────────────────────────────────────

def build_resumen_whatsapp(tipo_caso: str, nombre: str) -> str:
    if str(tipo_caso).strip() == "1":
        return (
            f"{nombre}, lamento lo ocurrido. "
            "Este total es una referencia preliminar; lo afinamos con documentos."
        )
    return (
        f"{nombre}, gracias por contarnos tu caso. "
        "Este total es una referencia preliminar; lo afinamos con documentos."
    )


def build_analisis_web_gpt(
    nombre: str,
    tipo_caso: str,
    descripcion: str,
    salario_mensual: float,
    ini: date,
    fin: date,
    temas: list[dict],
    total_estimado: float = 0.0,
    componentes: dict | None = None,
    abogado_nombre: str = "",
) -> str:
    """
    Genera el análisis consultivo completo (~250 palabras) para mostrar
    en el reporte web o enviar por WhatsApp al finalizar el flujo.
    """
    tipo_h   = "Despido" if str(tipo_caso).strip() == "1" else (
               "Renuncia" if str(tipo_caso).strip() == "2" else "Caso laboral")
    desc     = (descripcion or "").strip()
    antig    = years_of_service(ini, fin)
    antig_txt = f"{antig:.2f} años" if antig > 0 else "—"
    sd       = (salario_mensual / 30.0) if salario_mensual else 0.0

    comp = componentes or {}
    n90  = float(comp.get("Indemnizacion_90") or 0.0)
    nPA  = float(comp.get("Prima_Antiguedad") or 0.0)
    nAgu = float(comp.get("Aguinaldo_Prop")   or 0.0)
    nVac = float(comp.get("Vacaciones_Prop")  or 0.0)
    nPV  = float(comp.get("Prima_Vac_Prop")   or 0.0)

    def fallback() -> str:
        intro = (
            f"{nombre}, gracias por contarnos tu caso. "
            f"Con la información disponible, lo ubicamos como {tipo_h} con una antigüedad "
            f"aproximada de {antig_txt} (del {ini.isoformat()} al {fin.isoformat()}) "
            f"y un salario mensual considerado de ${salario_mensual:,.2f} "
            f"(salario diario aproximado ${sd:,.2f})."
        )
        if total_estimado > 0:
            intro += f" El total estimado preliminar es de ${total_estimado:,.2f}."

        cuerpo = (
            "Este estimado es referencial y puede cambiar al confirmar salario integrado "
            "real, pagos previos y prestaciones adicionales. "
            "El cálculo considera prestaciones proporcionales (aguinaldo, vacaciones y prima "
            "vacacional)"
            + (" y, al tratarse de despido, incluye indemnización (90 días) y prima de antigüedad."
               if str(tipo_caso).strip() == "1"
               else " y pagos pendientes vinculados al finiquito.")
        )

        pasos = (
            "Para personalizarlo con precisión te recomendamos:\n"
            "• Reunir recibos de nómina o transferencias, contrato (si existe) e historial IMSS.\n"
            "• No firmar renuncia, finiquito o documentos en blanco sin revisión previa.\n"
            "• Guardar mensajes, correos o evidencia del motivo y fecha del evento.\n"
            "Con esa información, una abogada revisa el escenario y define la ruta más conveniente."
        )

        cierre = (
            f"Tu abogada asignada es {abogado_nombre}; en breve te contactará."
            if abogado_nombre
            else "En breve una abogada te contactará para el seguimiento."
        )

        txt = f"{intro}\n\n{cuerpo}\n\n{pasos}\n\n{cierre}"
        if len(txt.split()) > 300:
            txt = _clip_words(txt, 285)
        return txt + "\n\nOrientación informativa; no constituye asesoría legal definitiva."

    if not (OPENAI_API_KEY and OpenAI):
        return fallback()

    contexto_items = []
    for t in (temas or [])[:3]:
        titulo   = (t.get("Titulo_Visible")   or "Punto legal relevante").strip()
        contenido = _clip_chars((t.get("Contenido_Legal") or "").strip(), 420)
        contexto_items.append(f"- {titulo}: {contenido}" if contenido else f"- {titulo}")
    contexto = "\n".join(contexto_items).strip() or "(Sin entradas específicas; usa criterios generales de la LFT.)"

    comp_txt = (
        f"Montos estimados: "
        f"90 días=${n90:,.2f}; prima antigüedad=${nPA:,.2f}; "
        f"aguinaldo prop=${nAgu:,.2f}; vacaciones prop=${nVac:,.2f}; "
        f"prima vac prop=${nPV:,.2f}; total=${float(total_estimado or 0.0):,.2f}."
    )

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)

        messages = [
            {
                "role": "system",
                "content": (
                    "Eres un asistente legal en derecho laboral mexicano. "
                    "Escribe con tono humano, cálido y profesional. "
                    "Lenguaje sencillo, sin tecnicismos pesados. "
                    "NO uses Markdown. "
                    "Objetivo: 230 a 290 palabras (ideal ~250). "
                    "Incluye 3 a 5 viñetas con • solo en la sección de pasos. "
                    "Evita frases genéricas; usa los datos proporcionados. "
                    "No incluyas la leyenda final de orientación informativa; el sistema la añade."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Genera un análisis consultivo personalizado para {nombre}.\n\n"
                    f"Datos del caso:\n"
                    f"- Tipo: {tipo_h}\n"
                    f"- Periodo: {ini.isoformat()} a {fin.isoformat()} (antigüedad aprox. {antig_txt})\n"
                    f"- Salario mensual considerado: ${salario_mensual:,.2f} (SD aprox. ${sd:,.2f})\n"
                    f"- Descripción del usuario: {desc if desc else '(sin descripción)'}\n"
                    f"- {comp_txt}\n"
                    f"- Abogada asignada: {abogado_nombre or '(no especificada)'}\n\n"
                    f"Base de conocimiento (usa solo lo relevante y de forma natural):\n{contexto}\n\n"
                    "Requisitos:\n"
                    "1) Empieza con empatía real referenciando algo del relato (si hay descripción).\n"
                    "2) Explica por qué el total es preliminar y qué factores lo mueven.\n"
                    "3) Explica qué incluye el cálculo usando al menos 3 montos.\n"
                    "4) Plan de acción en viñetas (3 a 5), cada una con una razón corta.\n"
                    "5) Cierra indicando que una abogada dará seguimiento por WhatsApp.\n"
                ),
            },
        ]

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.6,
            max_tokens=800,
        )

        txt = (resp.choices[0].message.content or "").strip()
        if not txt:
            return fallback()

        txt = re.sub(r"(?is)\n*orientación informativa;.*$", "", txt).strip()

        if len(txt.split()) > 310:
            txt = _clip_words(txt, 285)
        if len(txt.split()) < 180:
            return fallback()

        return txt + "\n\nOrientación informativa; no constituye asesoría legal definitiva."

    except Exception:
        return fallback()


# ─────────────────────────────────────────────
# Abogados Admin — upsert
# ─────────────────────────────────────────────

def upsert_abogados_admin(
    sh,
    lead_id: str,
    abogado_id: str,
    nombre_cliente: str = "",
    telefono_normalizado: str = "",
    descripcion: str = "",
    estatus: str = "ASIGNADO",
) -> None:
    """
    Crea o actualiza un registro en Abogados_Admin vinculado al lead.

    Columnas AppSheet:
      ID_Admin | ID_Lead | ID_Abogado | Estatus | Nombre |
      Telefono_Normalizado | Descripcion_Situacion

    FASE 1 — Registro inicial (nombre + teléfono disponibles):
      Llamar con abogado_id="A01", estatus="No Asignado".
      Solo rellena ID_Admin, ID_Lead, Nombre, Telefono_Normalizado.
      Los demás campos quedan vacíos para AppSheet.

    FASE 2 — Proceso completo (process_lead terminó):
      Llamar con abogado_id real y estatus="ASIGNADO".
      Actualiza ID_Abogado, Estatus y los campos que lleguen con valor,
      sin pisar campos que AppSheet ya haya editado manualmente.

    Nota: Telefono_Normalizado se guarda SIN el prefijo '+' (requerido por AppSheet).
    Registros manuales sin ID_Lead nunca se tocan.
    """
    try:
        ws = open_worksheet(sh, TAB_ABOG_ADMIN)
    except Exception:
        return

    try:
        existing_row = find_row_by_value(ws, "ID_Lead", lead_id)
    except Exception:
        existing_row = None

    # Telefono siempre sin '+' para AppSheet
    tel_clean = telefono_normalizado.lstrip("+") if telefono_normalizado else ""

    if existing_row:
        # ── Actualizar fila existente (respeta datos previos de AppSheet) ──
        try:
            h = build_header_map(ws)
            upd: dict = {}
            # Siempre actualizamos abogado y estatus cuando nos llaman con datos reales
            if abogado_id:
                upd["ID_Abogado"] = abogado_id
            if estatus:
                upd["Estatus"] = estatus
            # Campos de contenido: solo si llegan con valor para no pisar ediciones manuales
            if nombre_cliente:
                upd["Nombre"] = nombre_cliente
            if tel_clean:
                upd["Telefono_Normalizado"] = tel_clean
            if descripcion:
                upd["Descripcion_Situacion"] = descripcion
            if upd:
                update_row_cells(ws, existing_row, upd, hmap=h)
        except Exception:
            pass
        return

    # ── Crear fila nueva ──
    try:
        header  = with_backoff(ws.row_values, 1)
        h       = build_header_map(ws)
        row_out = [""] * len(header)

        def set_cell(col: str, val: str) -> None:
            c = col_idx(h, col)
            if c and 1 <= c <= len(row_out):
                row_out[c - 1] = val

        set_cell("ID_Admin",              uuid.uuid4().hex[:12])
        set_cell("ID_Lead",               lead_id)
        set_cell("ID_Abogado",            abogado_id)
        set_cell("Estatus",               estatus)
        set_cell("Nombre",                nombre_cliente)
        set_cell("Telefono_Normalizado",  tel_clean)
        set_cell("Descripcion_Situacion", descripcion)

        # Campos que AppSheet gestiona después (se dejan vacíos):
        set_cell("Acepto_Asesoria",       "")
        set_cell("Enviar_Cuestionario",   "")
        set_cell("Proxima_Fecha_Evento",  "")
        set_cell("Notas",                 "")

        with_backoff(ws.append_row, row_out, value_input_option="RAW")
    except Exception:
        return


def register_lead_inicial(
    sh,
    lead_id: str,
    nombre_cliente: str,
    telefono_raw: str,
) -> None:
    """
    FASE 1: Se llama en cuanto el bot captura nombre y teléfono del lead,
    antes de que complete el flujo (salario, fechas, descripción).

    Crea la fila en Abogados_Admin con:
      - ID_Abogado  = "A01"   (placeholder hasta asignación real)
      - Estatus     = "No Asignado"
      - Nombre      = nombre_cliente
      - Telefono_Normalizado = número sin '+' en formato E.164

    Si la fila ya existe (raro, por reintentos) no hace nada.
    """
    try:
        ws = open_worksheet(sh, TAB_ABOG_ADMIN)
    except Exception:
        return

    try:
        existing_row = find_row_by_value(ws, "ID_Lead", lead_id)
        if existing_row:
            return  # Ya existe, no duplicar
    except Exception:
        pass

    upsert_abogados_admin(
        sh=sh,
        lead_id=lead_id,
        abogado_id="A01",
        nombre_cliente=nombre_cliente,
        telefono_normalizado=_to_e164_no_plus(telefono_raw),
        descripcion="",
        estatus="No Asignado",
    )


# ─────────────────────────────────────────────
# Job principal
# ─────────────────────────────────────────────

def process_lead(lead_id: str) -> dict:
    """
    Job principal invocado por el worker cuando el lead llega al paso EN_PROCESO.

    Pasos:
      1. Lee el lead de BD_Leads.
      2. Asigna abogada (round-robin o VIP).
      3. Parsea fechas (día por defecto = 1 si no viene).
      4. Calcula estimación detallada.
      5. Selecciona conocimiento ANALISIS de Conocimiento_AI.
      6. Genera análisis web con GPT (~250 palabras).
      7. Actualiza BD_Leads con todos los resultados.
      8. Upsert en Abogados_Admin (crea si no existe, actualiza si ya hay datos).
      9. Notifica a la abogada por plantilla de WhatsApp.
     10. Envía mensaje de estimación al cliente por WhatsApp.
    """
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME en variables de entorno.")

    sh       = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)
    ws_sys   = open_worksheet(sh, TAB_SYS)

    row = find_row_by_value(ws_leads, "ID_Lead", lead_id)
    if not row:
        raise RuntimeError(f"Lead no encontrado en BD_Leads: {lead_id}")

    h    = build_header_map(ws_leads)
    vals = with_backoff(ws_leads.row_values, row)

    def get(name: str) -> str:
        c = col_idx(h, name)
        return (vals[c - 1] if c and c - 1 < len(vals) else "").strip()

    # Marcar como en proceso
    update_row_cells(ws_leads, row, {
        "Procesar_AI_Status": "RUNNING",
        "Ultimo_Error":       "",
        "Ultima_Actualizacion": now_iso(),
    }, hmap=h)

    syscfg = read_sys_config(ws_sys)

    try:
        # ── Leer datos del lead ──
        telefono   = get("Telefono")
        nombre     = get("Nombre") or "Hola"
        apellido   = get("Apellido")
        cliente_full = (f"{nombre} {apellido}".strip() if apellido else nombre).strip()

        tipo_caso   = get("Tipo_Caso")
        salario     = money_to_float(get("Salario_Mensual"))
        descripcion = get("Descripcion_Situacion")

        # ── Fechas con día por defecto = 1 ──
        ini = _parse_date_parts_safe(h, vals, "Inicio")
        fin = _parse_date_parts_safe(h, vals, "Fin")
        if fin < ini:
            raise ValueError(
                f"La fecha de fin ({fin.isoformat()}) es anterior a la de inicio ({ini.isoformat()})."
            )

        # ── Asignación de abogada ──
        abogado_id, abogado_nombre, abogado_tel = pick_abogado_secuencial(
            ws_abog, ws_sys, salario, syscfg
        )

        # ── Cálculo de indemnización ──
        salario_min_diario = safe_float(syscfg.get("SALARIO_MIN_DIARIO") or "0")
        desglose_txt, total_estimado, comp = calc_estimacion_detallada(
            tipo_caso=tipo_caso,
            salario_mensual=salario,
            ini=ini,
            fin=fin,
            salario_min_diario=salario_min_diario,
        )

        # ── Conocimiento AI ──
        con_rows: list[dict] = []
        try:
            ws_con   = open_worksheet(sh, TAB_CONOCIMIENTO_AI)
            con_rows = load_conocimiento(ws_con)
        except Exception:
            con_rows = []

        temas = select_conocimiento(
            con_rows, descripcion, tipo_caso, k=3, contexto="ANALISIS"
        )

        # ── Análisis web (~250 palabras) ──
        analisis_web = build_analisis_web_gpt(
            nombre=nombre,
            tipo_caso=tipo_caso,
            descripcion=descripcion,
            salario_mensual=salario,
            ini=ini,
            fin=fin,
            temas=temas,
            total_estimado=total_estimado,
            componentes=comp,
            abogado_nombre=abogado_nombre,
        )

        resumen_wa = build_resumen_whatsapp(tipo_caso, nombre)

        # ── Token y links ──
        token    = uuid.uuid4().hex[:18]
        base_url = (syscfg.get("RUTA_REPORTE") or syscfg.get("BASE_URL_WEB") or "").strip()
        if base_url and not base_url.endswith("/") and "?" not in base_url:
            base_url += "/"
        link_reporte = f"{base_url}?token={token}" if base_url else ""

        link_abog = ""
        if abogado_tel:
            tnorm = "".join(c for c in abogado_tel if c.isdigit() or c == "+")
            if tnorm:
                link_abog = f"https://wa.me/{tnorm.replace('+', '')}"

        # ── Mensaje al cliente ──
        mensaje_final = (
            f"✅ {nombre}, ya tengo tu *estimación preliminar*.\n\n"
            f"💰 *Total estimado:* ${total_estimado:,.2f}\n\n"
            f"{resumen_wa}\n\n"
            f"👩⚖️ Tu abogada asignada es *{abogado_nombre}* "
            "y se comunicará contigo muy pronto.\n"
        )
        if link_reporte:
            mensaje_final += f"\n📄 Ver desglose en web: {link_reporte}\n"
        mensaje_final += "\n(Orientación informativa; no constituye asesoría legal.)"

        # ── Actualizar BD_Leads ──
        update_row_cells(ws_leads, row, {
            "Analisis_AI":            analisis_web,
            "Resultado_Calculo":      desglose_txt,
            "Total_Estimado":         f"{total_estimado:.2f}",

            "Abogado_Asignado_ID":    abogado_id,
            "Abogado_Asignado_Nombre":abogado_nombre,
            "Token_Reporte":          token,
            "Link_Reporte_Web":       link_reporte,
            "Link_WhatsApp":          link_abog,

            "Fecha_Inicio_Laboral":   ini.isoformat(),
            "Fecha_Fin_Laboral":      fin.isoformat(),
            "Es_Cliente":             "1",

            "Indemnizacion_90":       f"{comp['Indemnizacion_90']:.2f}",
            "Indemnizacion_20":       f"{comp['Indemnizacion_20']:.2f}",
            "Prima_Antiguedad":       f"{comp['Prima_Antiguedad']:.2f}",
            "Aguinaldo_Prop":         f"{comp['Aguinaldo_Prop']:.2f}",
            "Vacaciones_Prop":        f"{comp['Vacaciones_Prop']:.2f}",
            "Prima_Vac_Prop":         f"{comp['Prima_Vac_Prop']:.2f}",
            "Vac_Dias_Base":          str(comp["Vac_Dias_Base"]),

            "Ultimo_Error":           "",
            "Ultima_Actualizacion":   now_iso(),
        }, hmap=h)

        # ── Upsert en Abogados_Admin (FASE 2: datos completos) ──
        # Si register_lead_inicial ya creó la fila, la actualiza con la abogada
        # real y los datos completos. Si por alguna razón no existe, la crea.
        upsert_abogados_admin(
            sh,
            lead_id=lead_id,
            abogado_id=abogado_id,
            nombre_cliente=cliente_full,
            telefono_normalizado=_to_e164_no_plus(telefono),
            descripcion=descripcion,
            estatus="ASIGNADO",
        )

        # ── Notificación a la abogada (plantilla Twilio, con dedupe) ──
        already_sent = ""
        if "Notif_Abogada_NuevoCaso" in h:
            try:
                vals2       = with_backoff(ws_leads.row_values, row)
                idx         = h.get("Notif_Abogada_NuevoCaso")
                already_sent = (vals2[idx - 1] if idx and idx - 1 < len(vals2) else "").strip()
            except Exception:
                already_sent = ""

        if not abogado_tel:
            _safe_update(ws_leads, row, {
                "Notif_Abogada_NuevoCaso_Det": (
                    "NO_ENVIADO: abogado_tel vacío "
                    "(revisar Cat_Abogados: Telefono_Aboga / Teléfono_Registro)"
                ),
            }, hmap=h)

        if abogado_tel and not already_sent:
            tipo_h    = "Despido" if str(tipo_caso).strip() == "1" else (
                         "Renuncia" if str(tipo_caso).strip() == "2" else "Caso")
            total_txt = f"${total_estimado:,.2f} MXN"
            detalle   = f"Tipo: {tipo_h} Total: {total_txt} Reporte: {link_reporte or ''}"

            okA, detA = send_whatsapp_template_safe(
                to_number=abogado_tel,
                template_sid=WA_TPL_ABOGADA_NUEVO_CASO_SID,
                variables={
                    "1": cliente_full or "Cliente",
                    "2": _to_e164(telefono),
                    "3": detalle,
                },
            )
            _safe_update(ws_leads, row, {
                "Notif_Abogada_NuevoCaso":     now_iso() if okA else "",
                "Notif_Abogada_NuevoCaso_Det": (
                    f"{okA} {detA}" if okA else f"FALLO_ENVIO: {detA}"
                )[:240],
            }, hmap=h)

        # ── Mensaje al cliente ──
        ok1, det1 = send_whatsapp_safe(telefono, mensaje_final)

        update_row_cells(ws_leads, row, {
            "Procesar_AI_Status": "DONE" if ok1 else "DONE_SEND_ERROR",
            "ESTATUS":            "CLIENTE_MENU" if ok1 else "EN_PROCESO",
            "Ultimo_Error":       "" if ok1 else f"send1={ok1}({det1})"[:450],
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)

        return {"ok": True, "lead_id": lead_id, "send1": ok1}

    except Exception as e:
        update_row_cells(ws_leads, row, {
            "Procesar_AI_Status": "FAILED",
            "Ultimo_Error":       f"{type(e).__name__}: {e}",
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)
        raise


# ─────────────────────────────────────────────
# Job auxiliar — respuesta empática en tiempo real
# ─────────────────────────────────────────────

def process_caso_libre(lead_id: str) -> dict:
    """
    Job ligero invocado cuando el lead termina el paso CASO_LIBRE.

    Flujo:
      1. Lee la descripción del lead.
      2. Carga Conocimiento_AI contexto CONVERSACIONAL.
      3. Genera respuesta empática personalizada con GPT.
      4. Envía la respuesta al lead por WhatsApp.
      5. Actualiza estado a AI_EMPATIA en BD_Leads.
    """
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME en variables de entorno.")

    sh       = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)

    row = find_row_by_value(ws_leads, "ID_Lead", lead_id)
    if not row:
        raise RuntimeError(f"Lead no encontrado en BD_Leads: {lead_id}")

    h    = build_header_map(ws_leads)
    vals = with_backoff(ws_leads.row_values, row)

    def get(name: str) -> str:
        c = col_idx(h, name)
        return (vals[c - 1] if c and c - 1 < len(vals) else "").strip()

    telefono    = get("Telefono")
    descripcion = get("Descripcion_Situacion")
    tipo_hint   = get("Tipo_Caso")  # puede estar vacío en este punto

    # Cargar conocimiento conversacional
    con_rows: list[dict] = []
    try:
        ws_con   = open_worksheet(sh, TAB_CONOCIMIENTO_AI)
        con_rows = load_conocimiento(ws_con)
    except Exception:
        con_rows = []

    respuesta = build_respuesta_empatica(
        descripcion=descripcion,
        con_rows=con_rows,
        tipo_caso_hint=tipo_hint,
    )

    ok, det = send_whatsapp_safe(telefono, respuesta)

    _safe_update(ws_leads, row, {
        "ESTATUS":              "AI_EMPATIA" if ok else "CASO_LIBRE",
        "Ultimo_Error":         "" if ok else f"send_empatia={det}"[:450],
        "Ultima_Actualizacion": now_iso(),
    }, hmap=h)

    return {"ok": ok, "lead_id": lead_id, "send_empatia": ok, "detalle": det}
