import time
import json
from pymodbus.client import ModbusTcpClient
import paho.mqtt.client as mqtt

# --- Configuración Modbus TCP ---
#MODBUS_IP = ""
MODBUS_IP = "127.0.0.1"
MODBUS_PORT = 502  # Puerto estándar de Modbus TCP
MODBUS_SLAVE_ID = 1

# Dirección y cantidad de registros a leer
# IMPORTANTE: Dirección de inicio 1, Cantidad 4.
# Si tu dispositivo usa direccionamiento base 0 (donde el primer registro es 0),
# y te refieres al "primer" registro, deberías usar READ_ADDRESS = 0.
# Usaré 1, tal como lo solicitaste.
READ_ADDRESS = 0
READ_COUNT = 4

# --- Configuración MQTT (Thingsboard PE) ---
TB_HOST = ""
TB_PORT = 
TB_TOKEN = ""  # Este es el Access Token del dispositivo
TB_TOPIC = ""

# --- Variables de Cliente ---
modbus_client = None
mqtt_client = None

def on_connect(client, userdata, flags, rc):
    """Callback que se ejecuta al conectar al broker MQTT."""
    if rc == 0:
        print(f"¡Conectado exitosamente a Thingsboard en {TB_HOST}!")
    else:
        print(f"Fallo al conectar a MQTT, código de error: {rc}")

def setup_clients():
    """Configura los clientes Modbus y MQTT."""
    global modbus_client, mqtt_client
    
    # Configurar cliente Modbus
    modbus_client = ModbusTcpClient(MODBUS_IP, port=MODBUS_PORT)
    
    # Configurar cliente MQTT
    mqtt_client = mqtt.Client()
    # Thingsboard usa el Access Token como nombre de usuario
    mqtt_client.username_pw_set(TB_TOKEN)
    mqtt_client.on_connect = on_connect
    
def main_loop():
    """Loop principal para leer Modbus y enviar por MQTT."""
    global modbus_client, mqtt_client

    try:
        # Conectar al broker MQTT
        mqtt_client.connect(TB_HOST, TB_PORT, 60)
        mqtt_client.loop_start()  # Inicia el loop de MQTT en un hilo separado

        while True:
            print(f"Intentando leer {READ_COUNT} registros desde la dirección {READ_ADDRESS}...")
            
            # --- 1. Conectar y Leer Modbus ---
            if not modbus_client.connect():
                print(f"Error: No se pudo conectar al esclavo Modbus en {MODBUS_IP}")
                time.sleep(10)
                continue

            # Leer los Holding Registers
            read_result = modbus_client.read_holding_registers(
                address=READ_ADDRESS,
                count=READ_COUNT
            )
            
            modbus_client.close()

            if read_result.isError():
                print(f"Error al leer registros Modbus: {read_result}")
            else:
                registers = read_result.registers
                print(f"Lectura Modbus exitosa: {registers}")

                # --- 2. Formatear datos para Thingsboard ---
                # Creamos un diccionario. Thingsboard usará las "claves" 
                # (ej. "registro_1", "registro_2") como los nombres de la telemetría.
                # ¡DEBES AJUSTAR ESTAS CLAVES!
                
                payload_data = {
                    "Temperatura": registers[0],
                    "Humedad": registers[1],
                    "DI01": registers[2],
                    "DI02": registers[3]
                }
                
                '''for i, value in enumerate(registers):
                    # Crea claves como "registro_1", "registro_2", etc.
                    # Cambia esto por nombres útiles, ej: "voltaje", "corriente"
                    key_name = f"registro_{READ_ADDRESS + i}"
                    payload_data[key_name] = value'''

                # Convertir el diccionario a un string JSON
                payload_json = json.dumps(payload_data)

                # --- 3. Enviar por MQTT ---
                print(f"Enviando a Thingsboard: {payload_json}")
                mqtt_client.publish(TB_TOPIC, payload_json, qos=1)

            # Esperar antes de la próxima lectura
            print("Esperando 10 segundos para la próxima lectura...")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nScript detenido por el usuario.")
    except Exception as e:
        print(f"Ha ocurrido un error inesperado: {e}")
    finally:
        # Limpiar conexiones al salir
        if modbus_client and modbus_client.is_socket_open():
            modbus_client.close()
            print("Conexión Modbus cerrada.")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("Conexión MQTT desconectada.")

if __name__ == "__main__":
    setup_clients()
    main_loop()