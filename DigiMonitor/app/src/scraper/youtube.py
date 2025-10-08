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


import asyncio
import time
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
from DigiMonitor.app.src.utils import logger
from DigiMonitor.app.src.utils.json import save_json
from DigiMonitor.app.src.driver.browser_manager import BrowserManager


class YTScraper:
    """
    Implementa un scraper de YouTube usando Playwright.

    Características:
    - Abre múltiples videos de YouTube al mismo tiempo.
    - Hace scroll en el contenedor de comentarios para cargar más mensajes.
    - Extrae datos.
    - Guarda los resultados en un archivo JSON.
    """

    def __init__(self, urls, max_concurrent, output_dir, headless):
        """
        Constructor de la clase.

        Parámetros:
        - urls (list): lista de URLs de videos de YouTube.
        - max_concurrent (int): número máximo de ventanas abiertas simultáneamente 
                                (controlado por un semáforo asincrónico).
        - headless (bool): indica si el navegador debe ejecutarse en modo headless 
                            (sin interfaz gráfica). True = headless, False = modo gráfico.
        """
        self.urls = urls
        self.max_concurrent = max_concurrent
        self.headless = headless
        self.output_dir = output_dir


#ACTIONS
    async def _scrolldown(self, page, live_index, step=449, small_step=169, delay=0.3, max_attempts=3):
        """
        Realiza un scroll en el contenedor de comentarios para forzar la carga de más datos.
        
        Parámetros:
        - page: página Playwright activa.
        - live_index (int): índice del live (para logs).
        - step (int): tamaño del scroll hacia abajo.
        - small_step (int): paso pequeño hacia arriba (para "despertar" carga dinámica).
        - delay (float): tiempo de espera entre scrolls (en segundos).
        - max_attempts (int): número máximo de intentos para detectar cambios en altura.
        """

        # Altura inicial del contenedor de comentarios (#contents)
        page_init_height = await page.evaluate("""
            () => {
                const container = document.querySelector('ytd-item-section-renderer #contents');
                return container ? container.scrollHeight : 0;
            }
        """)
        logger.log(f"[URL {live_index+1}] Initial container height: {page_init_height}")

        # Primer scroll para activar carga de elementos
        await page.evaluate(f"window.scrollBy(0, {step});")
        await asyncio.sleep(delay)

        current_attempt = 0

        # Repetir scroll hasta que no haya más cambios en la altura
        while current_attempt < max_attempts:
            # Scroll hacia abajo dos veces
            for _ in range(2):
                await page.evaluate(f"window.scrollBy(0, {step});")
                await asyncio.sleep(delay)

            # Scroll pequeño hacia arriba para disparar cargas dinámicas
            await page.evaluate(f"window.scrollBy(0, -{small_step});")
            await asyncio.sleep(delay)

            # Revisar si la altura del contenedor cambió
            page_last_height = await page.evaluate("""
                () => {
                    const container = document.querySelector('ytd-item-section-renderer #contents');
                    return container ? container.scrollHeight : 0;
                }
            """)

            if page_last_height == page_init_height:
                # No hubo cambio, contamos un intento fallido
                current_attempt += 1
                #logger.log(f"[Live {live_index+1}] Attempt {current_attempt}: Comments container height has not changed.")
                await page.evaluate(f"window.scrollBy(0, {step});")
                await asyncio.sleep(delay)
            else:
                # Sí hubo cambio → reiniciamos intentos
                page_init_height = page_last_height
                current_attempt = 0

        logger.log(f"[URL {live_index+1}] Scrolling complete.")


    async def _expand_description(self, page):
        """
        Expande la descripción del video si hay un botón 'expand more/más'.
        """
        try:
            # XPath mejorado para ser compatible con inglés y español
            xpath = '//tp-yt-paper-button[@id="expand" and (contains(text(), "more") or contains(text(), "más"))]'

            # Obtenemos el locator y usamos .first para resolver la ambigüedad
            element = page.locator(xpath).first

            # Esperamos a que el elemento sea visible antes de intentar hacer clic
            await element.wait_for(state="visible", timeout=10_000)

            # Hacemos clic en el elemento
            await element.click()

        except Exception as error:
            # Capturamos cualquier error, como el timeout si el botón no aparece
            # En este caso, el error no es crítico, simplemente significa que la descripción
            # ya estaba expandida o el botón no existe.
            logger.log(f"[WARNING] An error occurred in '_expand_description': {str(error)}")
            return None


    async def _click_channel_and_expand_region(self, page, live_index) -> dict:
        """
        Da click en el enlace del canal, expande la descripción y extrae la región del canal.
        """
        data = {"channel_region": 'None',
                "channel_creation": 'None',
                "channel_total_videos": 'None',
                "channel_total_views": 'None',
                "date_scraping": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

        try:
            # 1 Hacer click en el enlace del canal
            channel_link_xpath = (
                '//yt-formatted-string[@id="text" and contains(@class, "ytd-channel-name")]'
                '/a[@class="yt-simple-endpoint style-scope yt-formatted-string"]'
            )
            channel_element = page.locator(channel_link_xpath)

            if await channel_element.count() > 0:
                await channel_element.first.click()
                logger.log(f"[URL {live_index+1}] Clicked on channel link successfully.")
                await asyncio.sleep(1)
                # 2 Esperar y hacer click en el botón para expandir la descripción
                more_button = page.locator('button.yt-truncated-text__absolute-button')
                if await more_button.count() > 0:
                    await more_button.first.click()
                    logger.log(f"[URL {live_index+1}] Clicked the description expand button successfully.")
                else:
                    logger.log(f"[URL {live_index+1}] [INFO] Expand description button not found, details already visible.")
                time.sleep(5)
                # 3 Extraer la región del canal
                data['channel_region'] = await self._extract_channel_region(page, live_index)
                data['channel_creation'] = await self._extract_channel_creation(page, live_index)
                data['channel_total_videos'] = await self._extract_channel_total_videos(page, live_index)
                data['channel_total_views'] = await self._extract_channel_total_views(page, live_index)

            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Channel link element not found.")

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] Error during click or extraction: {str(error)}")

        return data



# XPATHS
    async def _extract_id_channel(self, page, live_index) -> str | None:
        try:
            elements = page.locator('//link[@itemprop="url"]')
            if await elements.count() > 1:  # verificamos que exista el segundo elemento
                content = await elements.nth(1).get_attribute("href")
                return content or None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_id_channel' not found.")
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_id_channel': {str(error)}")
        return None


    async def _extract_full_name_channel(self, page, live_index) -> str | None:
        try:
            xpath = '//yt-formatted-string[@class="style-scope ytd-channel-name complex-string"]/a'
            element = page.locator(xpath)
            if await element.count() > 0:
                name = await element.first.inner_text()
                return name.strip() if name else None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_full_name_channel' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_full_name_channel': {str(error)}")
            return None


    async def _extract_profile_image_channel(self, page, live_index) -> str | None:
        try:
            xpath = '//yt-img-shadow[contains(@id, "avatar")]//img[contains(@id, "img")]'
            element = page.locator(xpath)
            if await element.count() > 0:
                src = await element.first.get_attribute("src")
                return src or None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_channel_profile_image' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_channel_profile_image': {str(error)}")
            return None


    async def _extract_count_subscribers_channel(self, page, live_index) -> int | None:
        xpath = '//yt-formatted-string[@class="style-scope ytd-video-owner-renderer"]'
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                subs = await element.first.inner_text()
                if not subs:
                    return None
                subs = (
                    subs.strip()
                    .replace("subscribers", "")
                    .replace("suscriptores", "")
                    .replace("\xa0", "")
                    .strip()
                )
                subs = subs.lower()

                # Manejo de K y M
                if subs.endswith("k"):
                    multiplier = 1_000
                    subs_value = float(subs[:-1].replace(",", ""))
                elif subs.endswith("m"):
                    multiplier = 1_000_000
                    subs_value = float(subs[:-1].replace(",", ""))
                else:
                    multiplier = 1
                    subs_value = int(subs.replace(",", ""))

                return int(subs_value * multiplier)
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_count_subscribers' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_count_subscribers': {str(error)}")
            return None


    async def _extract_url_post(self, page, live_index):
        try:
            element = await page.query_selector('//link[@itemprop="url"]')
            if element:
                href = await element.get_attribute('href')
                return href or 'None'
            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_url_post' not found.")
            return 'None'
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_url_post': {str(error)}")
            return 'None'


    async def _extract_upload(self, page, live_index):
        try:
            element = await page.query_selector('//meta[@itemprop="datePublished"]')
            if element:
                iso_str = await element.get_attribute('content')
                if iso_str:
                    # Parsear ISO 8601 con tzinfo
                    dt = datetime.fromisoformat(iso_str)

                    # Convertir a zona horaria CDMX
                    cdmx_tz = pytz.timezone("America/Mexico_City")
                    dt_cdmx = dt.astimezone(cdmx_tz)

                    # Formatear para ClickHouse
                    return dt_cdmx.strftime("%Y-%m-%d %H:%M:%S")

            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_upload' not found.")
            return 'None'
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_upload': {str(error)}")
            return 'None'


    async def _extract_thumbnail(self, page, live_index) -> str | None:
        try:
            xpath = '//meta[@property="og:image"]'
            element = page.locator(xpath)
            if await element.count() > 0:
                content = await element.first.get_attribute("content")
                return content.strip() if content else None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath in '_extract_thumbnail' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_thumbnail': {str(error)}")
            return None


    async def _extract_title_post(self, page, live_index) -> str | None:
        try:
            xpath = '//h1/yt-formatted-string[@class="style-scope ytd-watch-metadata"]'
            element = page.locator(xpath)
            if await element.count() > 0:
                title = await element.first.inner_text()
                return title.strip() if title else None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_title_post' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_title_post': {str(error)}")
            return None


    async def _extract_description_post(self, page, live_index) -> str | None:
        try:
            # Tomar el HTML del contenedor con la descripción expandida
            html = await page.inner_html('//ytd-text-inline-expander[@id="description-inline-expander"]//div[@id="expanded"]')

            soup = BeautifulSoup(html, "html.parser")

            # El span principal que contiene toda la descripción
            root_span = soup.find("span", class_="yt-core-attributed-string yt-core-attributed-string--white-space-pre-wrap")
            if not root_span:
                logger.log(f"[URL {live_index+1}] [WARNING] Root span for description not found.")
                return None

            description_parts = []

            # Recorremos todos los hijos del root_span
            for child in root_span.descendants:
                if child.name == "span":  # texto simple
                    text = child.get_text(strip=True)
                    if text:
                        description_parts.append(text)
                elif child.name == "a":  # links
                    text = child.get_text(strip=True)
                    href = child.get("href")
                    if text:
                        description_parts.append(f"{text} ({href})")

            description = "\n".join(description_parts).strip()
            return description if description else None

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_description_post': {str(error)}")
            return None



    async def _extract_hashtags_post(self, page, live_index) -> list[str]:
        try:
            # Localizamos todos los spans que pueden contener hashtags
            elements = page.locator('//span[contains(@class, "yt-core-attributed-string--link-inherit-color")]')
            count = await elements.count()
            hashtags = []

            for i in range(count):
                text = await elements.nth(i).inner_text()
                # Filtramos solo los que son hashtags
                if text.startswith("#"):
                    hashtags.append(text.strip())

            if not hashtags:
                logger.log(f"[URL {live_index+1}] [WARNING] No hashtags found in '_extract_hashtags_post'.")
            return hashtags

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_hashtags_post': {str(error)}")
            return []


    async def _extract_categoria_post(self, page, live_index) -> str | None:
        xpath = '//meta[@itemprop="genre"]'
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                content = await element.first.get_attribute("content")
                return content.strip() if content else None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath in '_extract_categoria' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_categoria': {str(error)}")
            return None


    async def _extract_likes_post(self, page, live_index) -> int | None:
        xpath = ('//meta[@itemprop="interactionType" and @content="https://schema.org/LikeAction"]'
                '/following-sibling::meta[@itemprop="userInteractionCount"]')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                content = await element.first.get_attribute("content")
                return int(content) if content and content.isdigit() else None
            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath in '_extract_count_likes' not found.")
            return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_count_likes': {str(error)}")
            return None


    async def _extract_count_comments(self, page, live_index) -> int | None:
        xpath = ('//yt-formatted-string[@class="count-text style-scope ytd-comments-header-renderer"]'
                '//span[@class="style-scope yt-formatted-string"]')
        try:
            elements = page.locator(xpath)
            count = await elements.count()
            if count > 0:
                texts = [await elem.inner_text() for elem in await elements.all()]
                full_text = ' '.join(t.strip() for t in texts if t).strip()
                if full_text:
                    # Filtrar solo los dígitos y convertir a int
                    num_str = ''.join(ch for ch in full_text if ch.isdigit())
                    return int(num_str) if num_str else None
                return None
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_count_comments' not found.")
                return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_count_comments': {str(error)}")
            return None


    async def _extract_count_views(self, page, live_index) -> int | None:
        xpath = ('//meta[@itemprop="interactionType" and @content="https://schema.org/WatchAction"]'
                '/following-sibling::meta[@itemprop="userInteractionCount"]')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                content = await element.first.get_attribute("content")
                return int(content) if content and content.isdigit() else None
            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath in '_extract_count_views' not found.")
            return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_count_views': {str(error)}")
            return None


    # Data comments
    async def _extract_imgs_profile_comments(self, page, live_index):
        xpath = '//div[@class=" style-scope ytd-item-section-renderer style-scope ytd-item-section-renderer"]//ytd-comment-thread-renderer//button[@id="author-thumbnail-button"]//img[contains(@id, "img")]'
        results = []
        try:
            items = page.locator(f"xpath={xpath}")
            count = await items.count()
            for i in range(count):
                src = await items.nth(i).get_attribute("src")
                if src:
                    results.append(src)
        except Exception as error:
            logger.warning(f"[URL {live_index+1}] [WARNING] Error in '_extract_imgs_profile_comments': {str(error)}")
        return results


    async def _extract_usernames(self, page, live_index) -> list[str] | None:
        xpath = '//div[@id="header-author"]'
        try:
            header_elements = page.locator(xpath)
            count = await header_elements.count()
            if count == 0:
                logger.log(f"[URL {live_index+1}] [WARNING] No header-author elements found in '_extract_usernames'.")
                return None

            usernames = []
            for i in range(count):
                element = header_elements.nth(i)
                text = None
                # Intentamos el <a>
                a_elem = element.locator('a').first
                if await a_elem.count() > 0:
                    text = await a_elem.inner_text() or await a_elem.get_attribute('href')
                else:
                    # Si no hay <a>, intentamos el <span>
                    span_elem = element.locator('span').first
                    if await span_elem.count() > 0:
                        text = await span_elem.inner_text()
                usernames.append(text.strip() if text else None)

            return usernames

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_usernames': {str(error)}")
            return None


    async def _extract_comments_emojis(self, page, live_index) -> list[str] | None:
        xpath = '//yt-attributed-string[@id="content-text"]'
        try:
            comment_blocks = page.locator(xpath)
            count = await comment_blocks.count()
            if count == 0:
                logger.log(f"[URL {live_index+1} [WARNING] No comment blocks found in '_extract_comments_emojis'.")
                return None

            results = []
            for i in range(count):
                block = comment_blocks.nth(i)
                html = await block.inner_html()
                soup = BeautifulSoup(html, 'html.parser')
                full_text = ""

                for elem in soup.recursiveChildGenerator():
                    if getattr(elem, "name", None) == "img":
                        alt = elem.get("alt")
                        if alt:
                            full_text += alt
                    elif isinstance(elem, str):
                        full_text += elem

                results.append(full_text.strip())

            return results

        except Exception as e:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_comments_emojis': {str(e)}")
            return None


    async def _extract_n_likes(self, page, live_index) -> list[str] | None:
        xpath = '//span[@id="vote-count-middle"]'
        try:
            like_elements = page.locator(xpath)
            count = await like_elements.count()
            if count == 0:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_n_likes' not found.")
                return None

            likes = []
            for i in range(count):
                item = like_elements.nth(i)
                text = await item.inner_text()
                if text:
                    likes.append(text.strip())

            return likes

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_n_likes': {str(error)}")
            return None


    async def _extract_dates(self, page, live_index) -> list[str] | None:
        xpath = '//span[@id="published-time-text"]/a'
        try:
            date_elements = page.locator(xpath)
            count = await date_elements.count()
            if count == 0:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_dates' not found.")
                return None

            dates = []
            for i in range(count):
                item = date_elements.nth(i)
                text = await item.inner_text()
                if text:
                    dates.append(text.strip())

            return dates

        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_dates': {str(error)}")
            return None


    # Metadata
    async def _extract_channel_region(self, page, live_index) -> str:
        xpath = ('//tr[@class="description-item style-scope ytd-about-channel-renderer"]'
                '/td[yt-icon[@icon="privacy_public"]]'
                '/following-sibling::td[@class="style-scope ytd-about-channel-renderer"]')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                region = await element.first.inner_text()
                return region.strip() if region else 'None'
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_channel_region' not found.")
                return 'None'
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_channel_region': {str(error)}")
            return 'None'


    async def _extract_channel_creation(self, page, live_index) -> str:
        xpath = ('//yt-attributed-string[@class="style-scope ytd-about-channel-renderer"]'
                '//span[@class="yt-core-attributed-string yt-core-attributed-string--white-space-pre-wrap" and @role="text"]//span')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                creation_date = await element.first.inner_text()
                return creation_date.strip() if creation_date else 'None'
            else:
                logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_channel_creation' not found.")
                return 'None'
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_channel_creation': {str(error)}")
            return 'None'


    async def _extract_channel_total_videos(self, page, live_index) -> int | None:
        xpath = ('//tr[@class="description-item style-scope ytd-about-channel-renderer"]'
                '/td[yt-icon[@icon="my_videos"]]/following-sibling::td[@class="style-scope ytd-about-channel-renderer"]')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                text = await element.first.inner_text()
                if text:
                    # Extraer solo los dígitos eliminando separadores y texto
                    digits_only = "".join(filter(str.isdigit, text))
                    return int(digits_only) if digits_only else None
            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_channel_total_videos' not found.")
            return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_channel_total_videos_int': {str(error)}")
            return None


    async def _extract_channel_total_views(self, page, live_index) -> int | None:
        xpath = ('//tr[@class="description-item style-scope ytd-about-channel-renderer"]'
                '/td[yt-icon[@icon="trending_up"]]/following-sibling::td[@class="style-scope ytd-about-channel-renderer"]')
        try:
            element = page.locator(xpath)
            if await element.count() > 0:
                text = await element.first.inner_text()
                if text:
                    # Extraer solo los dígitos eliminando separadores y texto
                    digits_only = "".join(filter(str.isdigit, text))
                    return int(digits_only) if digits_only else None
            logger.log(f"[URL {live_index+1}] [WARNING] Element with XPath '_extract_channel_total_views' not found.")
            return None
        except Exception as error:
            logger.log(f"[URL {live_index+1}] [WARNING] An error occurred in '_extract_channel_total_views_int': {str(error)}")
            return None



#JSON
    async def _process_url(self, sem, context, url, index):
        """
        Procesa un video de YouTube.
        """
        async with sem:
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded")
                logger.log(f"[URL {index+1}] Open URL: {url}")
                await page.wait_for_selector('//div[@id="below" and contains(@class, "style-scope ytd-watch-flexy")]')
                await page.evaluate("window.scrollTo(0, 0)")

                await self._scrolldown(page, index)

                # Validar consistencia de las listas de comentarios con reintentos
                max_attempts = 2
                wait_seconds = 1  # Espera 1 segundo antes de reintentar

                for attempt in range(max_attempts + 1):  # Primer intento + 2 reintentos
                    if attempt == 1:  # Segundo intento
                        await page.evaluate("window.scrollTo(0, 0)")  # volver al inicio
                        await self._scrolldown(page, index)           # hacer scroll de nuevo

                    comentarios = await self._extract_comments_emojis(page, index)
                    likes = await self._extract_n_likes(page, index)
                    dates = await self._extract_dates(page, index)

                    comment_lists = [
                        comentarios,
                        likes,
                        dates,
                    ]
                    same_size = all(len(lst) == len(comment_lists[0]) for lst in comment_lists)
                    comments_length = (
                        len(comment_lists[0]) if same_size else [len(lst) for lst in comment_lists]
                    )

                    if same_size:
                        logger.log(f"[URL {index+1}] [Attempt {attempt + 1}] Comment lists verified as consistent")
                        break  # Salir del bucle si las listas son consistentes
                    else:
                        logger.log(f"[URL {index+1}] [WARNING] [Attempt {attempt + 1}] Inconsistency detected in comments")
                        if attempt < max_attempts:
                            #logger.log(f"Esperando {wait_seconds}s antes de reintentar...")
                            await asyncio.sleep(wait_seconds)  # Espera antes del siguiente intento

                await self._expand_description(page)

                id_channel = await self._extract_id_channel(page, index)
                full_name_channel = await self._extract_full_name_channel(page, index)
                profile_image_channel = await self._extract_profile_image_channel(page, index)
                count_subscribers_channel = await self._extract_count_subscribers_channel(page, index)

                video_url = await self._extract_url_post(page, index)
                upload_date = await self._extract_upload(page, index)
                thumbnail = await self._extract_thumbnail(page, index)
                title = await self._extract_title_post(page, index)
                description = await self._extract_description_post(page, index)
                hashtags = await self._extract_hashtags_post(page, index)
                categoria = await self._extract_categoria_post(page, index)
                likes_count = await self._extract_likes_post(page, index)
                comentarios_count = await self._extract_count_comments(page, index)
                views = await self._extract_count_views(page, index)


                # Guardar resultados
                video_data = {
                    "date_scraping": "",
                    "original_url": url,                         # URL original del contenido
                    "channel_id": id_channel,                    # ID del canal
                    "channel_name": full_name_channel,           # Nombre completo del canal
                    "channel_profile_image": profile_image_channel, # Imagen de perfil del canal
                    "channel_subscribers_count": count_subscribers_channel, # Cantidad de suscriptores
                    "channel_region": "",
                    "channel_creation": "",
                    "channel_total_videos": "",
                    "channel_total_views": "",
                    "post_url": video_url,                       # URL del post/video
                    "post_upload_date": upload_date,             # Fecha de publicación
                    "post_thumbnail": thumbnail,                 # Miniatura del post
                    "post_title": title,                         # Título del post
                    "post_description": description,             # Descripción del post
                    "post_hashtags": hashtags,                   # Hashtags del post
                    "post_category": categoria,                  # Categoría del post
                    "post_likes_count": likes_count,             # Likes del post
                    "post_comments_count": comentarios_count,    # Comentarios del post
                    "post_views_count": views,                   # Vistas del post
                    "post_comments": {                           # Información de comentarios
                        "comments_consistent": same_size,        # True o False
                        "comments_length": comments_length,       # int o None
                        "comments_text": comentarios,                 # Texto de comentarios
                        "comment_likes": likes,                       # Likes por comentario
                        "comment_dates": dates                        # Fechas de comentarios
                    }
                }

                submetadata = await self._click_channel_and_expand_region(page, index)
                video_data.update(submetadata)

                # Guardar inmediatamente en archivo JSON
                file_path = save_json(video_data, filename=f"youtube_data_live_{index+1}", folder=self.output_dir)
                logger.log(f"[URL {index+1}] Data saved in: {file_path}")

            except Exception as e:
                logger.log(f"[URL {index+1}] Error in '_process_url': {e}", "warning")

            finally:
                if not page.is_closed():
                    await page.close()
                logger.log(f"[URL {index+1}] Page closed after scraping.")


    async def _run(self):
        """
        Método interno que orquesta el scraping de todas las URLs:
        - Crea un semáforo para limitar concurrencia.
        - Abre un navegador con BrowserManager.
        - Lanza tareas en paralelo para procesar todas las URLs.
        - Al final, guarda los resultados en un archivo JSON.
        """
        sem = asyncio.Semaphore(self.max_concurrent)

        # Abrimos navegador con el contexto de BrowserManager
        async with BrowserManager(headless=self.headless) as context:
            tasks = [
                self._process_url(sem, context, url, i)
                for i, url in enumerate(self.urls)
            ]
            await asyncio.gather(*tasks)


#RUN
    def run(self):
        """
        Método público que ejecuta el scraper de forma síncrona.
        - Internamente usa asyncio.run() para lanzar _run().
        """
        asyncio.run(self._run())
