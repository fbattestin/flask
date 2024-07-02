import os
import logging
from flask import Flask, jsonify
from flasgger import Swagger

app = Flask(__name__)
Swagger(app)  # Adiciona Swagger ao aplicativo Flask

# Configurações
app.config['DEBUG'] = os.getenv('DEBUG', 'False') == 'True'
app.config['PORT'] = int(os.getenv('PORT', 5000))

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_kafka_connection():
    # Simulação de checagem de conexão com Kafka
    return True  # Retornar True se a conexão for bem-sucedida

def check_service_credentials():
    # Simulação de checagem de credenciais do serviço
    return True  # Retornar True se as credenciais estiverem corretas

def check_database_connection():
    # Simulação de checagem de conexão com o banco de dados
    return True  # Retornar True se a conexão for bem-sucedida

@app.route('/is_alive', methods=['GET'])
def is_alive():
    """
    Verifica o estado de saúde do serviço.
    Retorna um JSON com o status 'alive' se todas as verificações forem bem-sucedidas.
    ---
    responses:
      200:
        description: Serviço está vivo
        schema:
          type: object
          properties:
            status:
              type: string
              example: alive
      503:
        description: Falha ao verificar estado de saúde
        schema:
          type: object
          properties:
            status:
              type: string
              example: unhealthy
            error:
              type: string
    """
    try:
        # Verificação de conexão com Kafka
        if not check_kafka_connection():
            raise Exception("Kafka connection failed")

        # Verificação de credenciais do serviço
        if not check_service_credentials():
            raise Exception("Service credentials check failed")

        # Verificação de conexão com banco de dados
        if not check_database_connection():
            raise Exception("Database connection failed")

        response = {
            'status': 'alive'
        }
        return jsonify(response), 200

    except Exception as e:
        logger.error(f'Health check failed: {e}')
        response = {
            'status': 'unhealthy',
            'error': str(e)
        }
        return jsonify(response), 503

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=app.config['PORT'])
