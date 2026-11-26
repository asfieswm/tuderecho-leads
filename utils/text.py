# utils/text.py
import re
from typing import Dict

def normalize_option(msg: str) -> str:
    """Devuelve la opción elegida ('1'..'9') solo si el mensaje ES una respuesta de opción:
    empieza con el dígito ("1", "1️⃣", "2 porque...") o es corto (máx. 3 palabras) y trae un
    dígito suelto ("opción 1", "la 2"). Un relato con números ("me despidieron hace 2 semanas")
    no cuenta como opción."""
    s = (msg or "").strip()
    m = re.match(r"^\W*([1-9])(?!\d)", s)
    if m:
        return m.group(1)
    if len(s.split()) <= 3:
        m = re.search(r"(?<!\d)([1-9])(?!\d)", s)
        if m:
            return m.group(1)
    return ""

def tipo_caso_por_texto(msg: str) -> str:
    """Para la pregunta '¿despido (1) o renuncia (2)?' cuando la persona contesta con palabras."""
    t = (msg or "").lower()
    renuncia = bool(re.search(r"renunci", t))
    despido = bool(re.search(r"despid|corri[eó]|corrieron|me sacaron|liquidaron|me liquid|me echaron", t))
    if despido and not renuncia:
        return "1"
    if renuncia and not despido:
        return "2"
    return ""

MESES = {
    "enero": 1, "ene": 1, "febrero": 2, "feb": 2, "marzo": 3, "mar": 3, "abril": 4, "abr": 4,
    "mayo": 5, "may": 5, "junio": 6, "jun": 6, "julio": 7, "jul": 7, "agosto": 8, "ago": 8,
    "septiembre": 9, "setiembre": 9, "sep": 9, "sept": 9, "septt": 9, "octubre": 10, "oct": 10,
    "noviembre": 11, "nov": 11, "diciembre": 12, "dic": 12,
}
_SIGUE = re.compile(r"\b(sigo|a[uú]n|actualmente|vigente|activ[oa]|todav[ií]a|no ha terminado|no me han despedido)\b")

def _anio_valido(a: int, hoy_anio: int):
    return a if 1950 <= a <= hoy_anio else None

def parse_fecha_laboral(msg: str, hoy_anio: int, hoy_mes: int, es_fin: bool = False):
    """Interpreta respuestas libres de fecha: '2020', 'enero', '06', 'marzo 2020', '23/02/2018',
    '2015-07-15', 'año 2025', 'del 97', 'hace 3 años' y, si es_fin, 'sigo trabajando' (= hoy).
    Devuelve (anio|None, mes|None)."""
    s = (msg or "").strip().lower()
    s = re.sub(r"(?<=\d)\.0\b", "", s)                      # '6.0' -> '6'
    if es_fin and _SIGUE.search(s):
        return hoy_anio, hoy_mes
    m = re.search(r"\bhace\s+(\d{1,2})\s+a[ñn]os?\b", s)
    if m:
        a = _anio_valido(hoy_anio - int(m.group(1)), hoy_anio)
        return (a, hoy_mes) if a else (None, None)
    m = re.search(r"\b(19\d{2}|20\d{2})-(\d{1,2})-\d{1,2}", s)    # ISO
    if m and 1 <= int(m.group(2)) <= 12:
        return _anio_valido(int(m.group(1)), hoy_anio), int(m.group(2))
    m = re.search(r"\b\d{1,2}\s*[/\-.]\s*(\d{1,2})\s*[/\-.]\s*(\d{2,4})\b", s)   # dd/mm/aaaa
    if m and 1 <= int(m.group(1)) <= 12:
        a = int(m.group(2))
        if a < 100:
            a += 2000 if a <= hoy_anio % 100 else 1900
        return _anio_valido(a, hoy_anio), int(m.group(1))
    mes = None
    for w in re.findall(r"[a-záéíóú]+", s):
        if w in MESES:
            mes = MESES[w]
            break
    anio = None
    m = re.search(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", s)
    if m:
        anio = _anio_valido(int(m.group(1)), hoy_anio)
    else:
        m = re.search(r"\b(?:del|el|en|a[ñn]o)\s+(\d{2})\b", s)
        if m:
            a = int(m.group(1))
            anio = _anio_valido(a + (2000 if a <= hoy_anio % 100 else 1900), hoy_anio)
    if mes is None:
        m = re.fullmatch(r"\W*(?:mes\s*)?0?([1-9]|1[0-2])\W*", s)
        if m:
            mes = int(m.group(1))
    return anio, mes

def _int_o_none(v):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None

def fin_antes_de_inicio(ini_anio, ini_mes, fin_anio, fin_mes) -> bool:
    """True si la fecha de fin es claramente anterior a la de inicio. Con mes faltante compara años."""
    ia, im, fa, fm = map(_int_o_none, (ini_anio, ini_mes, fin_anio, fin_mes))
    if ia is None or fa is None:
        return False
    if im is None or fm is None:
        return fa < ia
    return (fa, fm) < (ia, im)

SALARIO_MIN, SALARIO_MAX = 1500, 300000

def parse_salario(msg: str):
    """'12,150 pesos', '$12000', '10 mil', '20mil', '19576.9', '408 al dia', '2600 semanales',
    '9\\'400', '14, 000' -> salario MENSUAL. Devuelve (monto|None, motivo) con motivo en
    {'ok', 'sin_numero', 'fuera_de_rango'}."""
    s = (msg or "").strip().lower()
    s = re.sub(r"(?<=\d)[,'’]\s?(?=\d{3}\b)", "", s)        # separadores de miles
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None, "sin_numero"
    v = float(m.group(0))
    resto = s[m.end():m.end() + 25]
    if re.match(r"\s*mil\b", resto) and v < 1000:
        v *= 1000
    if re.search(r"d[ií]a|diari", s):
        v *= 30
    elif re.search(r"seman", s):
        v *= 30 / 7
    elif re.search(r"quincen", s):
        v *= 2
    if not (SALARIO_MIN <= v <= SALARIO_MAX):
        return None, "fuera_de_rango"
    return round(v, 2), "ok"

def render_text(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1]
    return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t").strip()

def detect_fuente(msg: str, form=None) -> str:
    # 1) Anuncios Click-to-WhatsApp de Meta: Twilio manda los campos Referral*.
    if form is not None:
        src_url = (form.get("ReferralSourceUrl") or "").lower()
        if form.get("ReferralCtwaClid") or form.get("ReferralSourceId") or src_url:
            return "INSTAGRAM" if "instagram" in src_url else "FACEBOOK"
    # 2) Texto del mensaje (palabras completas, para no confundir "amigo" con "ig").
    t = (msg or "").lower().strip()
    if t in ("test", "prueba"):
        return "PRUEBA"
    if re.search(r"\b(facebook|fb|anuncio)\b", t):
        return "FACEBOOK"
    if re.search(r"\b(instagram|insta|ig)\b", t):
        return "INSTAGRAM"
    if re.search(r"\b(sitio|web|p[aá]gina)\b", t):
        return "WEB"
    return "DESCONOCIDA"
