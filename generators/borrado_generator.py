from pathlib import Path
import time

URL = Path(__file__).resolve().parent

DIRECTORIOS = [
    URL / "src/main/resources/data/binance",
    URL / "src/main/resources/data/binance",
    URL / "src/main/resources/data/binance"
]

# maxima antiguedad de ficheros
BORRADO_DIAS = 3

def borrado_automatico():
    actual = time.time()
    maximo = actual - (BORRADO_DIAS * 24 * 60 * 60)
    borrados = 0
    
    for i in DIRECTORIOS:
        if not i.is_dir():
            print(f"[ERROR] No existe el directorio: {i}")
            continue
        
        for file in i.glob("*.json"):
            try:
                if file.is_file() and file.stat().st_mtime < maximo:
                    file.unlink()
                    borrados += 1
                    print(f"[INFO] Fichero borrado {file}")
            except OSError as er:
                print(f"[ERROR] No se ha podido borrar {file}: {er}")
        
    print(f"[BORRADO] Total archivos eliminados: {borrados}")
    
if __name__ == "__main__":
    borrado_automatico()