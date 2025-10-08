# DIGIMONITOR is part of the DIGIBOOK collection.
# DIGIBOOK Copyright (C) 2024-2025 Daniel A. L.
# Repository: https://github.com/caminodelaserpiente/DigiBook

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import logging
import os


# Carpeta donde se guardarán los logs
LOG_DIR = "DigiMonitor/logs"

# Crea la carpeta "logs" si no existe (evita errores al escribir archivos de log)
os.makedirs(LOG_DIR, exist_ok=True)

# Configuración básica de logging
# - filename: ruta del archivo donde se guardan los logs
# - level: nivel mínimo de logs que se registran (INFO = muestra INFO, WARNING, ERROR, CRITICAL)
# - format: formato del mensaje de log (fecha/hora, nivel y mensaje)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "scraper.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def log(msg: str, level="info"):
    """
    Registra un mensaje tanto en consola como en el archivo de logs.

    Parámetros:
    - msg (str): mensaje a registrar.
    - level (str): nivel de log ("info", "warning", "error", "debug", etc.).
                   Por defecto "info".

    Funcionamiento:
    1. Busca en el módulo logging el método correspondiente al nivel (ej: logging.info, logging.error).
    2. Si el nivel no existe, usa logging.info por defecto.
    3. Guarda el mensaje en el archivo "logs/scraper.log".
    4. Imprime el mensaje en consola (para visibilidad inmediata).
    """
    getattr(logging, level.lower(), logging.info)(msg)
    print(msg)
