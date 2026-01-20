import os
import hashlib
import time
import requests
from flask import Flask, request, jsonify
import logging
import uuid
from urllib.parse import urlparse, urlencode, parse_qs
import json

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DO META ADS (FACEBOOK) ---
# Valores padrão fornecidos pelo usuário
DEFAULT_PIXEL_ID = '1810756586220053'
DEFAULT_ACCESS_TOKEN = 'EAAKe1WkmnOsBQlWaNSlbv3e5SRZA1yHZAN2oncAVcpTVUazNikLM0DnklfkI6ZBckBFdGtBh5W4BudBN3W2ccgl67aGxnObjQSUZAEBYsc7FnZBkzjaxr6wLZCpsIzTZCozBAstNdB7H69ZAwveBqPSZB8c1S03BNsHLJUIxTzTEPzbbSFlzDCJHZB6DxdbT2ZCxAZDZD'

PIXEL_ID = os.environ.get('META_PIXEL_ID', DEFAULT_PIXEL_ID)
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', DEFAULT_ACCESS_TOKEN)
META_API_VERSION = 'v19.0'
META_API_URL = f'https://graph.facebook.com/{META_API_VERSION}/{PIXEL_ID}/events'

def hash_data(data):
    """Função para hashear os dados do usuário com SHA-256 (requisito do Meta)."""
    if data is None or data == "":
        return None
    # Meta requer normalização: minúsculas e sem espaços extras
    return hashlib.sha256(str(data).lower().strip().encode('utf-8')).hexdigest()

def normalize_city(city):
    """Normaliza o nome da cidade (remover acentos, minúsculas, remover espaços)."""
    if not city:
        return None
    # Simplificação: apenas minúsculas e remove espaços das pontas
    # Para produção ideal, remover acentos e caracteres especiais
    return str(city).lower().strip().replace(" ", "")

def normalize_state(state):
    """Normaliza o estado (código de 2 letras minúsculas)."""
    if not state:
        return None
    return str(state).lower().strip()

def normalize_country(country):
    """Normaliza o código do país (2 letras minúsculas, ex: 'br')."""
    if not country:
        return None
    # Se vier "Brasil", tenta converter para "br". Idealmente deve vir o código ISO.
    if len(str(country)) > 2:
        return 'br' # Default fallback simples
    return str(country).lower().strip()

def send_event_to_meta(event_name, user_data, custom_data=None, event_source_url=None, action_source='website', event_id=None, event_time=None):
    """Monta e envia um evento para a API de Conversões do Meta (CAPI)."""
    if not ACCESS_TOKEN or not PIXEL_ID:
        logger.error("Credenciais do Meta não configuradas.")
        return None
    
    try:
        if not event_id:
            event_id = str(uuid.uuid4())
        
        if event_time is None:
            event_time = int(time.time())
        else:
            # Garante que seja timestamp em segundos (inteiro)
            try:
                if float(event_time) > 1e10: # Se for ms
                    event_time = int(float(event_time) / 1000)
                else:
                    event_time = int(float(event_time))
            except:
                event_time = int(time.time())

        # Estrutura do evento conforme documentação da Graph API
        event_payload = {
            'event_name': event_name,
            'event_time': event_time,
            'event_id': event_id,
            'event_source_url': event_source_url,
            'action_source': action_source,
            'user_data': user_data,
        }

        if custom_data:
            event_payload['custom_data'] = custom_data

        # Payload final (array de eventos)
        final_payload = {
            'data': [event_payload],
            'access_token': ACCESS_TOKEN
            # 'test_event_code': 'TEST12345' # Descomente para testes no Events Manager se tiver o código
        }
        
        logger.info(f"Enviando evento '{event_name}' para Meta CAPI")
        logger.debug(f"Payload completo: {json.dumps(final_payload, indent=2)}")
        
        response = requests.post(META_API_URL, json=final_payload)
        
        logger.info(f"Resposta Meta CAPI ({event_name}): {response.status_code} - {response.text}")
        
        return response
        
    except Exception as e:
        logger.error(f"Erro ao enviar evento '{event_name}': {str(e)}")
        # Não lançar exceção para não quebrar o fluxo do webhook, apenas logar
        return None

def extract_client_ip(payload, request_obj):
    """Tenta extrair o IP do cliente do payload ou do request."""
    # Tenta pegar do payload (frequentemente enviado no ip_info ou raiz)
    ip = payload.get('ip') or payload.get('ip_info', {}).get('ip')
    
    # Se não tiver no payload, tenta pegar o IP de quem chamou o webhook 
    # (CUIDADO: isso será o IP do servidor que enviou o webhook, não do usuário final.
    #  Só use se tiver certeza que o servidor repassa o IP original via header X-Forwarded-For)
    if not ip:
        # Tenta pegar de headers comuns de proxy caso o servidor de origem reenvie
        if request_obj.headers.getlist("X-Forwarded-For"):
            ip = request_obj.headers.getlist("X-Forwarded-For")[0]
        else:
            # IP remoto (geralmente do servidor de webhook, pode não ser útil para matching)
            ip = request_obj.remote_addr
            
    return ip

def extract_user_agent(payload, request_obj):
    """Extrai User Agent."""
    ua = payload.get('browser') or payload.get('user_agent')
    if not ua:
        # Se não vier no payload, não use o UA do request (pois será o UA do servidor que faz o post, ex: Python-requests)
        # Deixe None a menos que tenha certeza.
        pass
    return ua

def prepare_user_data(payload, request_obj):
    """Prepara o objeto user_data com matching avançado."""
    user_data = {}
    
    # Tenta extrair dados diretamente da raiz ou de um objeto 'user' se existir (depende da estrutura exata)
    # Baseado no exemplo: campos estão na raiz
    
    # 1. Email (em) - SHA256
    email = payload.get('email') or payload.get('user_email')
    if email:
        user_data['em'] = hash_data(email)
        
    # 2. Phone (ph) - SHA256
    phone = payload.get('phone') or payload.get('user_phone')
    if phone:
        # Limpar DDI/DDD se necessário, mas hash_data só normaliza string.
        # Para phone, Meta recomenda apenas números.
        clean_phone = ''.join(filter(str.isdigit, str(phone)))
        user_data['ph'] = hash_data(clean_phone)

    # 3. Nomes (fn, ln) - SHA256
    # O payload pode ter 'username' ou 'user_full_name' ou 'name'/'surname'
    first_name = payload.get('name')
    last_name = payload.get('surname') or payload.get('lastname')
    
    full_name = payload.get('user_full_name') or payload.get('name') # As vezes vem nome completo em 'name'
    
    if not first_name and full_name:
        parts = str(full_name).split()
        if len(parts) > 0:
            first_name = parts[0]
        if len(parts) > 1:
            last_name = ' '.join(parts[1:])
    
    if first_name:
        user_data['fn'] = hash_data(first_name)
    if last_name:
        user_data['ln'] = hash_data(last_name)
        
    # 4. Data de Nascimento (db) - SHA256 - Formato YYYYMMDD
    # O exemplo mostra: "user_birth_date": "17/08/2000"
    birth_date = payload.get('user_birth_date') or payload.get('birth_date') or payload.get('data_nascimento')
    if birth_date:
        # Tentar converter 17/08/2000 para 20000817
        try:
            parts = str(birth_date).split('/')
            if len(parts) == 3:
                # assumindo DD/MM/YYYY
                yyyymmdd = f"{parts[2]}{parts[1]}{parts[0]}"
                user_data['db'] = hash_data(yyyymmdd)
            else:
                # Tenta manter como está se já parecer YYYYMMDD
                user_data['db'] = hash_data(birth_date.replace("-", ""))
        except:
            pass
            
    # 5. Gênero (ge) - SHA256
    gender = payload.get('user_gender') or payload.get('gender')
    if gender:
        # Meta espera 'f' ou 'm' (hash)
        g_str = str(gender).lower()
        if 'f' in g_str:
            user_data['ge'] = hash_data('f')
        elif 'm' in g_str:
            user_data['ge'] = hash_data('m')
            
    # 6. Cidade (ct), Estado (st), Zip (zp), País (country) - SHA256
    # Geralmente em ip_info ou location
    ip_info = payload.get('ip_info', {})
    
    city = payload.get('city') or ip_info.get('city_normalized') or ip_info.get('city')
    if city:
        user_data['ct'] = hash_data(normalize_city(city))
        
    state = payload.get('state') or ip_info.get('region') or payload.get('region')
    if state:
        user_data['st'] = hash_data(normalize_state(state))
        
    zip_code = payload.get('zip') or ip_info.get('zip') or payload.get('postal_code')
    if zip_code:
        # Manter apenas letras e numeros ou formato específico? Documentação diz lowercase sem espaços.
        # Geralmente 5-digit zip para US, pode variar.
        user_data['zp'] = hash_data(str(zip_code).replace("-", "").replace(" ", ""))
        
    country = payload.get('country') or ip_info.get('country_code') or payload.get('country')
    if country:
        user_data['country'] = hash_data(normalize_country(country))
        
    # 7. External ID (external_id) - SHA256
    ext_id = payload.get('user_id') or payload.get('id') or payload.get('external_id')
    if ext_id:
        user_data['external_id'] = hash_data(ext_id)
        
    # --- Parâmetros que NÃO devem ser hasheados ---
    
    # 8. Client IP Address (client_ip_address)
    ip_address = extract_client_ip(payload, request_obj)
    if ip_address:
        user_data['client_ip_address'] = ip_address
        
    # 9. Client User Agent (client_user_agent)
    ua = extract_user_agent(payload, request_obj)
    if ua:
        user_data['client_user_agent'] = ua
        
    # 10. fbc e fbp (Cookie IDs)
    fbc = payload.get('fbc') or payload.get('cookie_fbc')
    if fbc:
        user_data['fbc'] = fbc
        
    fbp = payload.get('fbp') or payload.get('cookie_fbp')
    if fbp:
        user_data['fbp'] = fbp
        
    # 11. Subscription ID (subscription_id)
    # Se aplicável
    
    return user_data

# Inicializa a aplicação Flask
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def meta_webhook():
    """Endpoint principal que recebe os webhooks e envia para o Meta."""
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "No JSON payload received"}), 400
            
        logger.info(f"Webhook recebido: {payload}")
        
        event_type = payload.get('event')
        if not event_type:
            return jsonify({"error": "Missing 'event' field"}), 400

        # Dados comuns
        user_data = prepare_user_data(payload, request)
        
        # URL de origem (se não vier, tentar referer ou deixar vazio)
        event_source_url = payload.get('page_url') or payload.get('url') or request.headers.get('Referer')
        
        # Timestamp
        event_time = payload.get('created_at') or payload.get('logged_at') or payload.get('event_time')
        # Se for string ISO, pode precisar de parsing. send_event_to_meta usa time.time() se for None.
        # Tenta converter string ISO se necessário, mas o código atual espera timestamp numerico ou None.
        # Fallback para None para usar tempo atual se não for numérico simples.
        
        event_id_unique = str(uuid.uuid4()) # ID único para desduplicação

        # Roteamento de Eventos
        
        # --- 1. USER_CREATED -> CompleteRegistration ---
        if event_type == 'USER_CREATED':
            custom_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Registration'
            }
            send_event_to_meta('CompleteRegistration', user_data, custom_data, event_source_url, event_id=event_id_unique)
            
        # --- 2. USER_LOGIN -> Lead ---
        elif event_type == 'USER_LOGIN':
            custom_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Login'
            }
            send_event_to_meta('Lead', user_data, custom_data, event_source_url, event_id=event_id_unique)
            
        # --- 3. DEPOSIT_CREATED -> InitiateCheckout AND AddToCart ---
        elif event_type == 'DEPOSIT_CREATED':
            amount = payload.get('amount', 0)
            currency = payload.get('currency', 'BRL')
            deposit_id = payload.get('deposit_id')
            
            # AddToCart
            atc_data = {
                'currency': currency,
                'value': float(amount),
                'content_type': 'product',
                'content_ids': [str(deposit_id)],
                'content_name': 'Deposit Created'
            }
            # AddToCart costuma ser antes do checkout, mas aqui acontecem juntos.
            # Vamos gerar um ID diferente para o segundo evento
            send_event_to_meta('AddToCart', user_data, atc_data, event_source_url, event_id=str(uuid.uuid4()))
            
            # InitiateCheckout
            ic_data = {
                'currency': currency,
                'value': float(amount),
                'content_type': 'product',
                'content_ids': [str(deposit_id)],
                'num_items': 1
            }
            send_event_to_meta('InitiateCheckout', user_data, ic_data, event_source_url, event_id=event_id_unique)

        # --- 4. DEPOSIT_PAID -> Purchase ---
        elif event_type == 'DEPOSIT_PAID':
            amount = payload.get('amount', 0)
            currency = payload.get('currency', 'BRL')
            deposit_id = payload.get('deposit_id')
            
            purchase_data = {
                'currency': currency,
                'value': float(amount),
                'content_type': 'product',
                'content_ids': [str(deposit_id)],
                'content_name': 'Deposit Paid'
            }
            
            send_event_to_meta('Purchase', user_data, purchase_data, event_source_url, event_id=event_id_unique)
            
        else:
            logger.info(f"Evento {event_type} ignorado ou não mapeado.")
            return jsonify({"status": "ignored", "message": f"Event {event_type} not mapped"}), 200

        return jsonify({"status": "success", "message": "Event processed"}), 200

    except Exception as e:
        logger.error(f"Erro no processamento do webhook: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "Meta CAPI Tracking"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
