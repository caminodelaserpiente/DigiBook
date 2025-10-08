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


from playwright.async_api import async_playwright  


class BrowserManager:
    """
    Clase encargada de manejar la apertura y el cierre de un navegador usando Playwright.

    Implementa el protocolo de contexto asíncrono (`async with`) para garantizar 
    que los recursos (navegador, contexto y playwright) se liberen correctamente 
    sin importar si ocurre un error durante la ejecución.
    """

    def __init__(self, headless):
        # Guardamos los objetos principales que controlan el navegador.
        # Al inicio están en None, y se inicializan en __aenter__.
        self.playwright = None  # Instancia principal de Playwright (controla los navegadores instalados).
        self.browser = None     # Objeto navegador (cuando se lanza sin perfil persistente).
        self.context = None     # Contexto de navegación (como un perfil temporal o persistente).
        self.headless = headless # Booleano que indica si el navegador se ejecuta en modo headless (sin interfaz gráfica)


    async def __aenter__(self):
        """
        Método que se ejecuta automáticamente al entrar en un bloque `async with`.
        Aquí inicializamos Playwright y lanzamos el navegador.
        """

        # Iniciamos Playwright (arranca los "drivers" que permiten controlar navegadores).
        self.playwright = await async_playwright().start()

        # Lanzamos Chromium en modo headless o visible según la configuración
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless
        )

        # Creamos un "contexto nuevo" sobre ese navegador (cada contexto es como una ventana aislada).
        self.context = await self.browser.new_context()

        # Retornamos el contexto de navegación para que pueda usarse dentro del `async with`.
        return self.context


    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Método que se ejecuta automáticamente al salir de un bloque `async with`.
        Aquí cerramos todo lo que se abrió en __aenter__.
        """

        # Cerramos primero el contexto (ventanas, páginas, etc.).
        if self.context:
            await self.context.close()

        # Si se abrió un navegador directamente (sin perfil persistente), lo cerramos.
        if self.browser:
            await self.browser.close()

        # Finalmente detenemos Playwright, liberando los recursos del sistema.
        if self.playwright:
            await self.playwright.stop()
