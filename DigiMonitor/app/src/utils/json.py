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


import json
import os
from datetime import datetime


def save_json(data: dict, filename: str, folder: str):
    """
    Guarda un diccionario de Python como archivo JSON en disco.

    Parámetros:
    - data (dict): diccionario con los datos a guardar.
    - filename (str): nombre base del archivo (sin extensión).
    - folder (str): carpeta de destino donde se guardará el archivo.
                    Por defecto es "output".

    Funcionamiento:
    1. Crea la carpeta de destino si no existe.
    2. Genera un nombre de archivo único con timestamp (para evitar sobrescribir).
    3. Guarda los datos en formato JSON legible (indentado y con UTF-8).
    4. Retorna la ruta completa del archivo creado.
    """

    # Generar un timestamp único en formato AAAAMMDD_HHMMSS
    # Ejemplo: 20250820_174530
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Ruta completa del archivo → carpeta + nombre base + timestamp + extensión
    full_path = os.path.join(folder, f"{filename}_{timestamp}.json")

    # Escribir el archivo JSON
    # - ensure_ascii=False → mantiene acentos y caracteres especiales tal cual
    # - indent=4 → formato legible con sangría de 4 espacios
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    # Retornar la ruta del archivo generado
    return full_path
