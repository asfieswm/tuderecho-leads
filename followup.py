import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from twilio.rest import Client
from utils.sheets import open_spreadsheet, open_worksheet, build_header_map, update_row_cells

MX_TZ = ZoneInfo("America/Mexico_City")

def ejecutar_seguimiento():
    # Inicializar Twilio
    client = Client(os.environ['TWILIO_ACCOUNT_SID'], os.environ['TWILIO_AUTH_TOKEN'])
    
    # Abrir Google Sheets
    sh = open_spreadsheet(os.environ["GOOGLE_SHEET_NAME"])
    ws_leads = open_worksheet(sh, os.environ["TAB_LEADS"])
    h = build_header_map(ws_leads)
    records = ws_leads.get_all_records()
    
    ahora = datetime.now(MX_TZ)

    # Revisar fila por fila (empezando en la 2 por los encabezados)
    for i, lead in enumerate(records, start=2):
        estatus = (lead.get("ESTATUS") or "").strip()
        
        # Si el usuario se quedó a la mitad del proceso inicial
        if estatus in ["INICIO", "AVISO_PRIVACIDAD", "CASO_TIPO"]:
            last_upd_str = lead.get("Ultima_Actualizacion")
            if not last_upd_str: continue
            
            try:
                # Limpiamos la fecha para poder compararla
                clean_ts = last_upd_str.split("-")[0].split("+")[0]
                last_upd = datetime.fromisoformat(clean_ts).replace(tzinfo=MX_TZ)
                
                # Si han pasado más de 12 horas desde su último mensaje
                if ahora - last_upd > timedelta(hours=12):
                    tel = lead.get("Telefono")
                    
                    # Mensaje con opciones
                    msg = (
                        "Hola, vimos que iniciaste tu registro pero no terminaste. 📋\n\n"
                        "¿Preferirías que te llamemos para completar tus datos?\n"
                        "1️⃣ Sí, prefiero que me llamen\n"
                        "2️⃣ No, gracias"
                    )
                    
                    # Enviar mensaje por Twilio
                    client.messages.create(
                        from_=f"whatsapp:{os.environ['TWILIO_NUMBER']}",
                        body=msg,
                        to=tel
                    )
                    
                    # Actualizar hoja para esperar respuesta
                    update_row_cells(ws_leads, i, {
                        "ESTATUS": "ESPERANDO_LLAMADA_OPCION",
                        "Ultima_Actualizacion": ahora.strftime("%Y-%m-%dT%H:%M:%S%z")
                    }, hmap=h)
                    
            except Exception as e:
                print(f"Error procesando el teléfono {lead.get('Telefono')}: {e}")

if __name__ == "__main__":
    ejecutar_seguimiento()