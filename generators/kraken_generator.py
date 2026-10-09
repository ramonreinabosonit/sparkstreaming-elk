import json
import random
import uuid
import time
from datetime import datetime
import os

EXCHANGE = "kraken"
# RUTAJSON = "data/kraken.json"
RUTAJSON = "/mnt/c/Users/ramon.reina/IdeaProjects/SparkStreamingElk/src/main/resources/data/kraken"

UBICACION = {
    "ciudad": "Road Town",
    "pais": "Islas Vírgenes Británicas",
    "latitud": 18.4285,
    "longitud": -64.6185
}

# El par es (moneda base/cotización)
# realmetne lo que quiere decir es cúanto necesitas
# de la cotización para comprar una unidad de la moneda base
MERCADOS = {
    "ETH/USDC": {
        "name": "Ethereum",
        "base": "ETH",
        "quote": "USDC",
        "price": 2200.0
    },
    "SUI/USDC": {
        "name": "Sui",
        "base": "SUI",
        "quote": "USDC",
        "price": 2.0
    },
    "DOT/USDC": {
        "name": "Polkadot",
        "base": "DOT",
        "quote": "USDC",
        "price": 3.0
    }
}

# Para generar el id de cada operación/transacción
# Ejemplo: (Fuente - id)
def generar_transactionId():
    return f"BN-{uuid.uuid4().hex[:8].upper()}"


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

# generamos las cantidades bases de monedas compradas en la operación
def generar_cantidad_operacion(mercado):
    
    if mercado == "ETH/USDC":
        return round(random.uniform(0.01, 20), 6)
    
    elif mercado == "DOT/USDC":
        return round(random.uniform(30, 1000), 5)
    
    elif mercado == "SUI/USDC":
        return round(random.uniform(50, 1000), 4)
    

# las transacciones se guardan pero no están bien en el formato json
# hay que formatearlas bien
def guardar_transaccion(transaccion):

    transactionId = transaccion["transactionId"]
    
    fecha = datetime.strptime(transaccion["date"], "%d/%m/%Y %H:%M:%S")
    fechaFormateada = fecha.strftime("%Y%m%d_%H%M%S")
    
    nombreFichero = f"{EXCHANGE}_{fechaFormateada}_{transactionId}.json"
    
    rutaFichero = os.path.join(RUTAJSON, nombreFichero)

    with open(rutaFichero, "w", encoding="utf-8") as f:
        json.dump(transaccion, f, ensure_ascii=False, indent=4)

def generar_transaccion():
    
    mercado = random.choice(list(MERCADOS))
    precio = generar_precio_mercado(mercado)
    cantidad = generar_cantidad_operacion(mercado)
    
    tipo_operacion = random.choice(["COMPRA", "VENTA"])
    
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    transaccion = {
        "transactionId": generar_transactionId(),
        "fiscal_ubication": UBICACION,
        "values": mercado,
        "operation_type": tipo_operacion,
        "coin_value": f"{precio:.2f}",
        "coin_operation_quantity": f"{cantidad:.6f}",
        "final_price": f"{cantidad*precio:.2f}",
        "date": fecha,
        "user_ubication": generar_ubicacion_transaccion()
    }
    
    return transaccion
    
def main():
    print("KRAKEN - INICANDO GENERADORES")
    
    # c = 0
    # while c != 5:
    #     transaccion = generar_transaccion()
        
    #     # print(transaccion)
    #     # print( json.dumps(transaccion), flush=True)
    #     guardar_transaccion(transaccion)
        
    #     time.sleep(random.uniform(0.5, 2.0))
    #     c = c+1
    #     # print(c)
    #     print(f"{c}.{EXCHANGE} - {transaccion["transactionId"]}")
    #     if c == 5:
    #         break
    #     else:
    #         pass

    c = 0
    while True:
        transaccion = generar_transaccion()
        guardar_transaccion(transaccion)
        
        time.sleep(random.uniform(0.5, 7))
        c = c+1
        print(f"{c}. {EXCHANGE} - {transaccion["transactionId"]}")
        
if __name__ == "__main__":
    main()