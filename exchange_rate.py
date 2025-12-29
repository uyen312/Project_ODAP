from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import requests
import asyncio

# ASYNC FUNCTION SCRAPING
async def crawl_vcb_exchange():
    url = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_selector("table.table-responsive tbody tr")
        html = await page.content()
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.table-responsive tbody tr")
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) >= 5 and cols[0].upper() == "USD":
            rate = float(cols[4].replace(",", "").strip())
            return rate
    return 24000.0  # fallback nếu không tìm thấy USD

# FUNCTION LẤY TỶ GIÁ VNĐ
def get_vnd_rate():
    """
    Lấy tỷ giá USD->VND từ API, nếu lỗi dùng Web Scraping Vietcombank
    """
    try:
        # API trước
        response = requests.get("https://api.exchangerate.host/latest?base=USD&symbols=VND", timeout=5)
        data = response.json()
        return float(data['rates']['VND'])
    except:  # noqa: E722
        # Backup: Web Scraping Vietcombank
        try:
            return asyncio.run(crawl_vcb_exchange())
        except:  # noqa: E722
            return 26000.0  # fallback mặc định