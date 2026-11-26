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
