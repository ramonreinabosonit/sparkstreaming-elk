import subprocess
import sys

from generators.borrado_generator import borrado_automatico

GENERADORES = [
    "generators/binance_generator.py",
    "generators/coinbase_generator.py",
    "generators/kraken_generator.py"
]

def main():
    borrado_automatico()
    procesos = []

    for generador in GENERADORES:
        proceso = subprocess.Popen(
            [sys.executable, generador]
        )
        procesos.append(proceso)

    for proceso in procesos:
        proceso.wait()


if __name__ == "__main__":
    main()