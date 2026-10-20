import json
import uuid
import random
import time
from datetime import datetime
import os

EXCHANGE = "coinbase"
# RUTAJSON = "data/coinbase.json" 
RUTAJSON = "/mnt/c/Users/ramon.reina/IdeaProjects/SparkStreamingElk/src/main/resources/data/coinbase"

UBICACION = {
    "ciudad": "New York",
    "pais": "Estados Unidos",
    "latitud": 40.7142,
    "longitud": -74.0059   
}

MERCADOS = {
    "SOL/USDC": {
        "base": "SOL",
        "quote": "USDC",
        "price": 145.0
    },
    "XRP/USDC": {
        "base": "XRP",
        "quote": "USDC",
        "price": 2.85
    },
    # Cardano
    "ADA/USDC": {
        "base": "ADA",
        "quote": "USDC",
        "price": 0.75
    }
}
    
def generar_transactionId():
    return f"CB-{uuid.uuid4().hex[:8].upper()}"


def generar_precio_mercado(mercado):
    precio_mercado = MERCADOS[mercado]["price"]
    
    # para simular la variación del mercado entre +1% y -1%
    variacion = random.uniform(0.01, -0.01)
    # variacion = random.uniform(-0.005, 0.005)
    
    precio_final = (precio_mercado  * (1 + variacion))
    
    # guardar el precio final de la transaccion
    MERCADOS[mercado]["price"] = precio_final
    return round(precio_final, 2)

def generar_ubicacion_transaccion():
    ubicaciones = [
        {
            "ciudad": "Madrid",
            "pais": "España",
            "latitud": 40.4168,
            "longitud": -3.7038
        },
        {
            "ciudad": "Barcelona",
            "pais": "España",
            "latitud": 41.3874,
            "longitud": 2.1686
        },
        {
            "ciudad": "Tokyo",
            "pais": "Japon",
            "latitud": 35.6762,
            "longitud": 139.6503
        },
        {
            "ciudad": "London",
            "pais": "Reino Unido",
            "latitud": 51.5074,
            "longitud": -0.1278
        },
        {
            "ciudad": "New York",
            "pais": "Estados Unidos",
            "latitud": 40.7128,
            "longitud": -74.0060
        }
    ]
    
    return random.choice(ubicaciones)

def generar_cantidad_operacion(mercado):
    
    if mercado == "SOL/USDC":
        return round(random.uniform(0.5, 100))
    
    elif mercado == "XRP/USDC":
        return round(random.uniform(20, 300))
    
    elif mercado == "ADA/USDC":
        return round(random.uniform(100, 1000))

# generar y comprobar errores
def simular_incidencia(transaccion):
    if random.random() >= 0.03:
        return None
    
    incidencia = random.choice(["precio_invalido", "fecha_invalida", "coordenadas_invalidas"])
    
    if incidencia == "precio_invalido":
        transaccion["price"] = "mucho mucho dinero"

    elif incidencia == "fecha_invalida":
        transaccion["date"] = "coinbase octubre"

    elif incidencia == "coordenadas_invalidas":
        tipo_error = random.choice(["latitud_positiva", "latitud_negativa", "longitud_positiva", "longitud_negativa"])

        if tipo_error == "latitud_positiva":
            transaccion["transaction_ubication"]["latitud"] = random.uniform(90.0, 200.0)
        elif tipo_error == "latitud_negativa":
            transaccion["transaction_ubication"]["latitud"] = random.uniform(-200.0, -90.0)
        elif tipo_error == "longitud_positiva":
            transaccion["transaction_ubication"]["longitud"] = random.uniform(180.0, 300.0)
        elif tipo_error == "longitud_negativa":
            transaccion["transaction_ubication"]["longitud"] = random.uniform(-300.0, -180.0)
    
    transaccion["incidencia_simulada"] = incidencia
    transaccion["estado_generacion"] = "INCIDENCIA"
    
    return incidencia

def guardar_transaccion(transaccion):

    transactionId = transaccion["transactionId"]
    
    # fecha = datetime.strptime(transaccion["date"], "%d/%m/%Y %H:%M:%S")
    # fechaFormateada = fecha.strftime("%Y%m%d_%H%M%S")
    
    # nombreFichero = f"{EXCHANGE}_{fechaFormateada}_{transactionId}.json"
    
    # rutaFichero = os.path.join(RUTAJSON, nombreFichero)

    # with open(rutaFichero, "w", encoding="utf-8") as f:
    #     json.dump(transaccion, f, ensure_ascii=False, indent=4)
   
    try:
        fecha = datetime.strptime(transaccion["date"], "%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError, KeyError):
        fecha = datetime.now()

    fechaFormateada = fecha.strftime("%Y%m%d_%H%M%S")
    nombreFichero = (f"{EXCHANGE}_{fechaFormateada}_{transactionId}.json")

    os.makedirs(RUTAJSON, exist_ok=True)
    
    ruta_fichero = os.path.join(RUTAJSON, nombreFichero)

    with open(ruta_fichero, "w", encoding="utf-8") as f:
        json.dump(transaccion, f, ensure_ascii=False, indent=4)
    
   
# generamos las transacciones para Coin Base
def generar_transaccion():
    
    mercado = random.choice(list(MERCADOS))
    precio = generar_precio_mercado(mercado)
    cantidad = generar_cantidad_operacion(mercado)
    
    tipo_operacion = random.choice(["COMPRA", "VENTA"])
    
    # la fecha hay que formatearla a dd/mm/aaaa hh/mm/ss
    # ahora mismo se ve una cadena larga de números enteros
    # fecha = int(datetime.now(timezone.utc).timestamp() * 1000)
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    transaccion = {
        "transactionId": generar_transactionId(),
        "exchange_ubication": UBICACION,
        "pair": mercado,
        "operation": tipo_operacion,
        "price": f"{precio:.2f}",
        "quantity": f"{cantidad:.6f}",
        "total_price": f"{cantidad*precio:.2f}",
        "date": fecha,
        "transaction_ubication": generar_ubicacion_transaccion()
    }
    
    # aqui guardamos la transaccion en nuestro fichero json
    return transaccion
    
def main():
    print("COINBASE - INICANDO GENERADORES")    
    c = 0
    while True:
        transaccion = generar_transaccion()
        incidencia = simular_incidencia(transaccion)
        guardar_transaccion(transaccion)
        
        time.sleep(random.uniform(0.4, 2))
        c = c+1
        if incidencia:
            print(f"{c}. INCIDENCIA - {EXCHANGE} - {transaccion["transactionId"]} - {incidencia}")
        else:
            print(f"{c}. {EXCHANGE} - {transaccion["transactionId"]}")
        
if __name__ == "__main__":
    main()