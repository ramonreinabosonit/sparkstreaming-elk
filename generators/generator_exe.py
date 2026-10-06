import subprocess
import sys

GENERADORES = [
    "generators/binance_generator.py",
    "generators/coinbase_generator.py",
    "generators/kraken_generator.py"
]

procesos = []

for generador in GENERADORES:
    proceso = subprocess.Popen(
        [sys.executable, generador]
    )
    procesos.append(proceso)

for proceso in procesos:
    proceso.wait()