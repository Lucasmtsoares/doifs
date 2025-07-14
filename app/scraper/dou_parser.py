import asyncio
from base_scraper import init_browser, close_browser

async def run_scraping():
    print("Oi")
    instituto = "IFAL"
    data_inicio = "20/06/2024"
    data_fim = "30/06/2024"
    base_url = "https://www.in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados"

    browser, page, playwright = await init_browser(headless=True)

    try:
        await page.goto(base_url, timeout=60000)

        # Preencher campo de busca
        await page.locator("input#search-bar").fill(instituto)

        # Clique para abrir busca avançada
        await page.locator("a#toggle-search-advanced").click()

        # Marcar opções da busca
        await page.locator("input#tipo-pesquisa-1").click()
        await page.locator("input#do2").click()
        await page.locator("input#personalizado").click()

        # Preencher datas
        await page.fill("input#data-inicio", data_inicio)
        await page.fill("input#data-fim", data_fim)

        # Clicar em "Buscar"
        await page.locator("button.button").click()

        # Esperar pelo carregamento dos resultados ou pela ausência deles
        try:
            await page.wait_for_selector("h5.title-marker a", timeout=10000)
        except:
            print("Nenhum resultado encontrado.")
            return []

        # Começar a coletar os links
        all_links = []

        while True:
            # Coletar links da página atual
            elements = await page.locator("h5.title-marker a").all()
            links = [await el.get_attribute("href") for el in elements]
            links = [f"https://www.in.gov.br{link}" for link in links if link]
            all_links.extend(links)

            # Verificar se o botão "Próximo" está visível e habilitado
            next_btn = page.locator("button#rightArrow")
            is_disabled = await next_btn.get_attribute("disabled")

            if is_disabled or not await next_btn.is_visible():
                break

            await next_btn.click()
            await page.wait_for_load_state("networkidle")


        print(f"Total de links encontrados: {len(all_links)}")
        return all_links

    except Exception as e:
        print(f"Erro no scraping: {e}")
        return []

    finally:
        await close_browser(browser, playwright)
