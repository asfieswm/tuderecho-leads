# app.py
import os
import re
import uuid
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request, Response, jsonify, make_response
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import (
    open_spreadsheet, open_worksheet, with_backoff,
    build_header_map, col_idx, find_row_by_value, update_row_cells,
    get_all_values_safe, row_to_dict, find_row_by_col_value
)
from utils.text import normalize_option, render_text, detect_fuente

# =========================
# Timezone / ENV
# =========================
MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City").strip() or "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS   = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CONFIG = (os.environ.get("TAB_CONFIG") or os.environ.get("TAB_FLOW") or "Config_XimenaAI").strip()
TAB_SYS    = os.environ.get("TAB_SYS", "Config_Sistema").strip()

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

# =========================
# Helpers
# =========================
def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def twiml(text: str) -> Response:
    resp = MessagingResponse()
    resp.message(text)
    return Response(str(resp), mimetype="text/xml")

def get_queue():
    """Devuelve Queue si REDIS_URL existe; si no, None."""
    if not REDIS_URL:
        return None
    try:
        conn = Redis.from_url(REDIS_URL)
        return Queue(REDIS_QUEUE_NAME, connection=conn)
    except Exception as e:
        app.logger.exception(f"[REDIS] No se pudo crear Queue: {e}")
        return None

def log(ws_logs, lead_id, paso, msg_in, msg_out, telefono="", err=""):
    try:
        row = [
            uuid.uuid4().hex[:8],
            now_iso(),
            telefono,
            lead_id,
            paso,
            msg_in,
            msg_out,
            "WHATSAPP",
            "",
            "",
            err
        ]
        with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")
    except Exception:
        pass

def load_config(ws_config):
    try:
        rows = with_backoff(ws_config.get_all_records, numericise_ignore=["all"])
    except TypeError:
        rows = with_backoff(ws_config.get_all_records)

    cfg = {}
    for r in rows:
        pid = (r.get("ID_Paso") or "").strip()
        if pid:
            cfg[pid] = r
    return cfg

def step_type_raw(cfg_row) -> str:
    return (cfg_row.get("Tipo_Entrada") or "").strip().upper()

def infer_step_type(cfg_row) -> str:
    """
    Si en Sheet se olvidan Tipo_Entrada (como EN_PROCESO), inferimos:
    - si hay Opciones_Validas o Siguiente_Si_* -> OPCIONES
    - si hay Regla_Validacion o Campo_BD... -> TEXTO
    - si no -> SISTEMA
    """
    t = step_type_raw(cfg_row)
    if t:
        return t

    has_opts = bool((cfg_row.get("Opciones_Validas") or "").strip()) or bool((cfg_row.get("Siguiente_Si_1") or "").strip()) or bool((cfg_row.get("Siguiente_Si_2") or "").strip())
    has_text = bool((cfg_row.get("Regla_Validacion") or "").strip()) or bool((cfg_row.get("Campo_BD_Leads_A_Actualizar") or "").strip())

    if has_opts:
        return "OPCIONES"
    if has_text:
        return "TEXTO"
    return "SISTEMA"

def get_text(cfg_row) -> str:
    return render_text(cfg_row.get("Texto_Bot") or "")

def next_for_option(cfg_row, opt: str) -> str:
    k = f"Siguiente_Si_{opt}"
    if cfg_row.get(k):
        return (cfg_row.get(k) or "").strip()
    if opt == "1":
        return (cfg_row.get("Siguiente_Si_1") or "").strip()
    if opt == "2":
        return (cfg_row.get("Siguiente_Si_2") or "").strip()
    return ""

def _parse_opciones_validas(v) -> list:
    if v is None:
        return []
    if isinstance(v, (int, float)):
        n = int(v)
        if 1 <= n <= 20:
            return [str(i) for i in range(1, n + 1)]
        return [str(n)]
    s = str(v).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def ensure_lead(ws_leads, from_phone: str):
    phone_norm = re.sub(r"\D+", "", (from_phone or "").replace("whatsapp:", ""))
    h = build_header_map(ws_leads)

    row = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm, hmap=h)
    if row:
        vals = with_backoff(ws_leads.row_values, row)

        def get(name):
            c = col_idx(h, name)
            return (vals[c-1] if c and c-1 < len(vals) else "").strip()

        lead_id = get("ID_Lead") or ""
        if not lead_id:
            lead_id = uuid.uuid4().hex[:12]
            update_row_cells(ws_leads, row, {"ID_Lead": lead_id}, hmap=h)

        return row, lead_id, phone_norm, h

    lead_id = uuid.uuid4().hex[:12]
    new_row = [""] * len(h)

    def setv(name, val):
        c = col_idx(h, name)
        if c:
            new_row[c-1] = str(val)

    setv("ID_Lead", lead_id)
    setv("Telefono", from_phone)
    setv("Telefono_Normalizado", phone_norm)
    setv("Fuente_Lead", "DESCONOCIDA")
    setv("Fecha_Registro", now_iso())
    setv("Ultima_Actualizacion", now_iso())
    setv("ESTATUS", "INICIO")

    with_backoff(ws_leads.append_row, new_row, value_input_option="USER_ENTERED")
    row2 = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm, hmap=h)
    if not row2:
        # fallback extremo: buscar por ID_Lead recién insertado
        row2 = find_row_by_value(ws_leads, "ID_Lead", lead_id, hmap=h)
    return row2, lead_id, phone_norm, h

def read_lead_row(ws_leads, row_num: int, hmap):
    vals = with_backoff(ws_leads.row_values, row_num)
    d = {}
    for k, idx in hmap.items():
        d[k] = (vals[idx-1] if idx-1 < len(vals) else "").strip()
    return d

# =========================
# Routes
# =========================
@app.get("/")
def home():
    return {
        "ok": True,
        "service": "ximena-web",
        "ts": now_iso(),
        "hint": "Twilio debe hacer POST a /whatsapp (o / si lo configuraste así)."
    }

@app.get("/health")
def health():
    # health más útil
    q_ok = bool(get_queue())
    return {"ok": True, "ts": now_iso(), "redis_configured": bool(REDIS_URL), "queue_ok": q_ok}

@app.post("/")
def whatsapp_root_alias():
    return whatsapp_webhook()

@app.post("/whatsapp")
def whatsapp_webhook():
    msg_in_raw = (request.form.get("Body") or "").strip()
    from_phone = (request.form.get("From") or "").strip()
    msg_lower = msg_in_raw.lower().strip()

    app.logger.info(f"[TWILIO IN] path={request.path} from={from_phone} body={msg_in_raw}")

    try:
        if not GOOGLE_SHEET_NAME:
            app.logger.error("[CONFIG] Falta GOOGLE_SHEET_NAME.")
            return twiml("Estamos en mantenimiento (configuración). Intenta de nuevo en unos minutos 🙏")

        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_cfg   = open_worksheet(sh, TAB_CONFIG)

        cfg = load_config(ws_cfg)

        lead_row, lead_id, phone_norm, h = ensure_lead(ws_leads, from_phone)
        if not lead_row:
            return twiml("Perdón, tuve un problema técnico al registrar tu caso 🙏 Intenta de nuevo.")

        lead = read_lead_row(ws_leads, lead_row, h)

        estatus = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        nombre  = (lead.get("Nombre") or "").strip()
        procesar_status = (lead.get("Procesar_AI_Status") or "").strip().upper()

        # Bloqueo por no aceptar aviso
        if (lead.get("Bloqueado_Por_No_Aceptar") or "").strip():
            out = get_text(cfg.get("FIN_NO_ACEPTA", {})) or "Sin aviso de privacidad no podemos continuar."
            log(ws_logs, lead_id, "FIN_NO_ACEPTA", msg_in_raw, out, telefono=from_phone, err="blocked")
            return twiml(out)

        # Fuente (solo si aún es desconocida)
        fuente_actual = (lead.get("Fuente_Lead") or "DESCONOCIDA").strip()
        if fuente_actual == "DESCONOCIDA":
            fuente_actual = detect_fuente(msg_in_raw)

        update_row_cells(ws_leads, lead_row, {
            "Ultimo_Mensaje_Cliente": msg_in_raw,
            "Fuente_Lead": fuente_actual,
            "Ultima_Actualizacion": now_iso()
        }, hmap=h)

        # Comando "menu"
        if msg_lower == "menu":
            # si ya hay resultado, mandamos menú
            if (lead.get("Abogado_Asignado_ID") or "").strip() or procesar_status == "DONE":
                update_row_cells(ws_leads, lead_row, {"ESTATUS": "CLIENTE_MENU"}, hmap=h)
                out = get_text(cfg.get("CLIENTE_MENU", {})) or f"Hola {nombre} 👋\n1️⃣ Próximas fechas\n2️⃣ Resumen\n3️⃣ Contactar a mi abogada"
                out = out.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "CLIENTE_MENU", msg_in_raw, out, telefono=from_phone, err="")
                return twiml(out)
            # si está en proceso, no lo castigamos con invalid_option
            if estatus == "EN_PROCESO":
                out = "Sigo preparando tu estimación ✅\nEn cuanto termine te envío el resultado por aquí."
                log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err="still_processing")
                return twiml(out)

        msg_opt = normalize_option(msg_in_raw)

        # ==========================================================
        # INTERCEPCIÓN: LÓGICA DE RECUPERACIÓN (PROCESO A LA MITAD)
        # ==========================================================
        if estatus == "ESPERANDO_LLAMADA_OPCION":
            if msg_opt == "1" or "si" in msg_lower or "sí" in msg_lower:
                # SI, PREFIERO LLAMADA
                upd = {
                    "ESTATUS": "SOLICITAR_LLAMADA",
                    "Analisis_AI": "CLIENTE SOLICITÓ LLAMADA DIRECTA. El proceso de chatbot quedó incompleto.",
                    "Notas_Abogado": "PENDIENTE DE REGISTRO MANUAL - CONTACTAR URGENTE",
                    "Aviso_Privacidad_Aceptado": "SOLICITADO_EN_LLAMADA",
                    "Ultima_Actualizacion": now_iso()
                }
                update_row_cells(ws_leads, lead_row, upd, hmap=h)
                return twiml("¡Perfecto! Un asesor de Cuantarchitec te llamará pronto para ayudarte. Gracias.")

            else:
                # NO GRACIAS o cualquier otra respuesta
                update_row_cells(ws_leads, lead_row, {
                    "ESTATUS": "CONTACTO_RECHAZADO",
                    "Analisis_AI": "El usuario decidió no continuar con el registro ni recibir llamada.",
                    "Ultima_Actualizacion": now_iso()
                }, hmap=h)
                return twiml("Entendido. Si en algún momento necesitas asesoría legal, aquí estaremos. ¡Buen día!")
        # ==========================================================

        # Si está en INICIO y no manda 1/2 -> manda INICIO
        if estatus == "INICIO" and msg_opt not in ("1", "2"):
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n\n1️⃣ Sí\n2️⃣ No"
            out = out.replace("{Nombre}", nombre or "")
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="")
            return twiml(out)

        row_cfg = cfg.get(estatus)
        if not row_cfg:
            update_row_cells(ws_leads, lead_row, {"ESTATUS": "INICIO"}, hmap=h)
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n\n1️⃣ Sí\n2️⃣ No"
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="missing_step")
            return twiml(out)

        t = infer_step_type(row_cfg)
        msg_err = render_text(row_cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.")

        # ====== OPCIONES ======
        if t == "OPCIONES":
            valid = _parse_opciones_validas(row_cfg.get("Opciones_Validas"))
            if not valid:
                valid = ["1", "2"]

            if msg_opt not in valid:
                # excepción: si está en EN_PROCESO no lo castigamos
                if estatus == "EN_PROCESO":
                    out = "Sigo preparando tu estimación ✅\nEn cuanto termine te envío el resultado por aquí."
                    log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err="still_processing")
                    return twiml(out)

                log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_option")
                return twiml(msg_err)

            nxt = next_for_option(row_cfg, msg_opt) or "INICIO"
            campo = (row_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()

            upd = {"Paso_Anterior": estatus, "ESTATUS": nxt, "Ultima_Actualizacion": now_iso()}
            if campo:
                upd[campo] = msg_opt

            # Rechazó aviso
            if estatus == "AVISO_PRIVACIDAD" and msg_opt == "2":
                upd["Bloqueado_Por_No_Aceptar"] = "SI"

            update_row_cells(ws_leads, lead_row, upd, hmap=h)

            # EN_PROCESO (opcional Redis)
            if nxt == "EN_PROCESO":
                out = get_text(cfg.get("EN_PROCESO", {})) or "Estoy preparando tu estimación…"

                q = get_queue()
                if q is not None:
                    try:
                        from worker_jobs import process_lead
                        job = q.enqueue(process_lead, lead_id, job_timeout=180)
                        update_row_cells(ws_leads, lead_row, {
                            "Procesar_AI_Status": "ENQUEUED",
                            "RQ_Job_ID": job.get_id(),
                            "Ultimo_Error": ""
                        }, hmap=h)
                        app.logger.info(f"[RQ] Encolado lead_id={lead_id} job_id={job.get_id()} cola={REDIS_QUEUE_NAME}")
                    except Exception as e:
                        app.logger.exception(f"[RQ] No se pudo encolar: {e}")
                        update_row_cells(ws_leads, lead_row, {
                            "Procesar_AI_Status": "ERROR_ENQUEUE",
                            "Ultimo_Error": f"ENQUEUE_ERROR: {type(e).__name__}: {e}"
                        }, hmap=h)
                        # útil: también lo dejamos en Logs
                        log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err=f"enqueue_fail: {type(e).__name__}: {e}")
                else:
                    update_row_cells(ws_leads, lead_row, {
                        "Procesar_AI_Status": "SKIPPED_NO_REDIS",
                        "Ultimo_Error": "NO_REDIS_CONFIGURED"
                    }, hmap=h)

                out = out.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err="")
                return twiml(out)

            out = get_text(cfg.get(nxt, {})) or "Continuemos…"
            lead2 = read_lead_row(ws_leads, lead_row, h)
            out = out.replace("{Nombre}", (lead2.get("Nombre") or "").strip())
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return twiml(out)

        # ====== TEXTO ======
        if t == "TEXTO":
            regla = (row_cfg.get("Regla_Validacion") or "").strip()
            ok = True
            if regla.upper() == "MONEY":
                ok = bool(re.fullmatch(r"\d{1,12}", msg_in_raw.strip()))
            elif regla.upper().startswith("REGEX:"):
                pattern = regla.split(":", 1)[1].strip()
                try:
                    ok = bool(re.fullmatch(pattern, msg_in_raw.strip()))
                except re.error:
                    ok = True

            if not ok:
                log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_text")
                return twiml(msg_err)

            campo = (row_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
            nxt = (row_cfg.get("Siguiente_Si_1") or "").strip() or estatus

            upd = {"Paso_Anterior": estatus, "ESTATUS": nxt, "Ultima_Actualizacion": now_iso()}
            if campo:
                upd[campo] = msg_in_raw.strip()

            update_row_cells(ws_leads, lead_row, upd, hmap=h)

            out = get_text(cfg.get(nxt, {})) or "Gracias. Continuemos…"
            lead2 = read_lead_row(ws_leads, lead_row, h)
            out = out.replace("{Nombre}", (lead2.get("Nombre") or "").strip())
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return twiml(out)

        # ====== SISTEMA ======
        # En SISTEMA NO validamos opciones.
        out = get_text(row_cfg) or "Gracias."
        out = out.replace("{Nombre}", nombre or "")
        log(ws_logs, lead_id, estatus, msg_in_raw, out, telefono=from_phone, err="system_step")
        return twiml(out)

    except Exception as e:
        app.logger.exception(f"[ERROR] webhook exception: {e}")
        return twiml("Perdón, tuve un problema técnico 🙏\nIntenta de nuevo en un momento.")

# =========================
# API JSON para Divi (NUEVO - cambio mínimo)
# =========================
def _cors_json(payload, status=200):
    resp = make_response(jsonify(payload), status)
    # Puedes cambiar "*" por "https://tuderecholaboralmexico.com" si quieres restringir
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    # anti-caché fuerte (para evitar "reportes viejos")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/api/report", methods=["GET", "OPTIONS"])
def api_report():
    if request.method == "OPTIONS":
        return _cors_json({"ok": True})

    token = (request.args.get("token") or "").strip()
    if not token:
        return _cors_json({"ok": False, "error": "missing_token"}, 400)

    if not GOOGLE_SHEET_NAME:
        return _cors_json({"ok": False, "error": "missing_google_sheet_name"}, 500)

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)

        h = build_header_map(ws_leads)
        row_num = find_row_by_value(ws_leads, "Token_Reporte", token, hmap=h)
        if not row_num:
            return _cors_json({"ok": False, "error": "token_not_found"}, 404)

        lead = read_lead_row(ws_leads, row_num, h)

        # Antigüedad (para el reporte web), si no viene ya calculada
        try:
            from datetime import date as _date
            ini = None
            fin = None

            ini_str = (lead.get("Fecha_Inicio_Laboral") or "").strip()
            fin_str = (lead.get("Fecha_Fin_Laboral") or "").strip()

            if not ini_str:
                y = int((lead.get("Inicio_Anio") or "0") or "0")
                m = int((lead.get("Inicio_Mes") or "0") or "0")
                d = int((lead.get("Inicio_Dia") or "0") or "0")
                if y and m and d:
                    ini_str = f"{y:04d}-{m:02d}-{d:02d}"
                    lead["Fecha_Inicio_Laboral"] = ini_str

            if not fin_str:
                y = int((lead.get("Fin_Anio") or "0") or "0")
                m = int((lead.get("Fin_Mes") or "0") or "0")
                d = int((lead.get("Fin_Dia") or "0") or "0")
                if y and m and d:
                    fin_str = f"{y:04d}-{m:02d}-{d:02d}"
                    lead["Fecha_Fin_Laboral"] = fin_str

            if ini_str:
                ini = _date.fromisoformat(ini_str)
            if fin_str:
                fin = _date.fromisoformat(fin_str)

            if ini and fin:
                years = max((fin - ini).days, 0) / 365.0
                lead["Antiguedad"] = f"{years:.2f} años"
        except Exception:
            pass

        # Solo devolvemos campos necesarios (más estable/seguro)
        allow = [
            "ID_Lead",
            "Nombre", "Apellido",
            "Tipo_Caso",
            "Salario_Mensual",
            "Fecha_Inicio_Laboral", "Fecha_Fin_Laboral",
            "Antiguedad",
            "Analisis_AI",
            "Resultado_Calculo",
            "Total_Estimado",
            "Indemnizacion_90", "Indemnizacion_20",
            "Prima_Antiguedad",
            "Aguinaldo_Prop", "Vacaciones_Prop", "Prima_Vac_Prop",
            "Vac_Dias_Base",
            "Abogado_Asignado_Nombre",
            "Link_WhatsApp",
            "Token_Reporte",
        ]

        data = {k: (lead.get(k, "") if lead.get(k, "") is not None else "") for k in allow}
        return _cors_json({"ok": True, "data": data}, 200)

    except Exception as e:
        app.logger.exception(f"[ERROR] api_report exception: {e}")
        return _cors_json({"ok": False, "error": f"{type(e).__name__}: {e}"}, 500)

# =========================
# Reporte web (legacy: Render) - lo dejo intacto
# =========================
@app.get("/reporte")
def reporte():
    token = (request.args.get("token") or "").strip()
    lead_id = (request.args.get("lead") or "").strip()

    if not token and not lead_id:
        return ("Falta token o lead.", 400)

    if not GOOGLE_SHEET_NAME:
        return ("Falta GOOGLE_SHEET_NAME.", 500)

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    values = get_all_values_safe(ws_leads)

    idx = None
    if token:
        idx = find_row_by_col_value(values, "Token_Reporte", token)
    if idx is None and lead_id:
        idx = find_row_by_col_value(values, "ID_Lead", lead_id)

    if idx is None:
        return ("Reporte no encontrado.", 404)

    lead = row_to_dict(values[0], values[idx])

    nombre = html.escape((lead.get("Nombre") or "").strip())
    apellido = html.escape((lead.get("Apellido") or "").strip())
    tipo = html.escape((lead.get("Tipo_Caso") or "").strip())
    desc = html.escape((lead.get("Descripcion_Situacion") or "").strip())
    res = html.escape((lead.get("Resultado_Calculo") or "").strip())
    ai = html.escape((lead.get("Analisis_AI") or "").strip())

    tipo_h = "Despido" if tipo == "1" else ("Renuncia" if tipo == "2" else "Caso laboral")

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Reporte preliminar</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b0f14; color:#f2f4f7; margin:0; }}
    .wrap {{ max-width:980px; margin:0 auto; padding:22px; }}
    .card {{ background:#111827; border:1px solid #1f2937; border-radius:16px; padding:18px; margin-bottom:14px; }}
    h1 {{ margin:0 0 8px 0; font-size:22px; }}
    h2 {{ margin:0 0 8px 0; font-size:16px; color:#93c5fd; }}
    p {{ margin:0; line-height:1.45; white-space:pre-wrap; }}
    .muted {{ color:#9ca3af; font-size:12px; }}
    .btn {{ display:inline-block; margin-top:10px; background:#2563eb; color:white; padding:10px 14px; border-radius:10px; text-decoration:none; }}
    .btn2 {{ display:inline-block; margin-top:10px; background:#111827; border:1px solid #374151; color:white; padding:10px 14px; border-radius:10px; text-decoration:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Reporte preliminar</h1>
      <p class="muted">Generado: {now_iso()} · Este reporte es informativo y no constituye asesoría legal.</p>
      <a class="btn" href="#" onclick="window.print();return false;">Imprimir</a>
      <a class="btn2" href="/">Volver</a>
    </div>

    <div class="card">
      <h2>Datos del caso</h2>
      <p><b>Nombre:</b> {nombre} {apellido}</p>
      <p><b>Tipo:</b> {tipo_h}</p>
      <p><b>Descripción:</b> {desc}</p>
    </div>

    <div class="card">
      <h2>Estimación preliminar</h2>
      <p>{res}</p>
    </div>

    <div class="card">
      <h2>Orientación (informativa)</h2>
      <p>{ai}</p>
    </div>
  </div>
</body>
</html>
"""