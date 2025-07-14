import asyncio
from playwright.async_api import async_playwright, Browser, Page
from typing import Tuple

async def init_browser(headless: bool = False) -> Tuple[Browser, Page]:
    print("base aqui")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=headless)
    context = await browser.new_context()
    page = await context.new_page()

    return browser, page, playwright


# Fecha navegador com segurança
async def close_browser(browser: Browser, playwright):
    if browser:
        await browser.close()
    if playwright:
        await playwright.stop()


#https://oxylabs.io/blog/playwright-web-scraping

