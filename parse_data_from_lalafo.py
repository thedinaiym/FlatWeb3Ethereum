#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Set, Dict, Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://lalafo.kg/kyrgyzstan/kvartiry/prodazha-kvartir"

MAX_PAGES = 150            # максимум страниц листинга
MAX_ITEMS = 5000           # максимум объявлений
PAGE_SLEEP = (0.7, 1.3)    # пауза между страницами (min,max)
ITEM_SLEEP = (0.4, 1.0)    # пауза между карточками
DEEP_SCRAPE = True         # ходить в каждую карточку
SAVE_JSON = True           # сохранять JSON-дамп
OUT_CSV = Path("lalafo_flats.csv")
OUT_JSON = Path("lalafo_flats.json")
MINT_CSV_FX_RATE = 87.0    # Курс KGS/USD для MINT CSV

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Cache-Control": "no-cache",
}

CITY_RE = re.compile(r"(Бишкек|Ош|Каракол|Нарын|Талас|Баткен|Джалал-Абад|Маевка|Кемин|Тамчы|Новониколаевка)", re.I)
NUM_ONLY_RE = re.compile(r"[^\d]")
RE_FLOAT = re.compile(r"[^\d.,]")


def _sleep_rng(a, b):
    import random
    time.sleep(random.uniform(a, b))


def normalize_url(u: str) -> str:
    parts = list(urlsplit(u))
    parts[3] = ""  # query
    parts[4] = ""  # fragment
    return urlunsplit(parts)


def to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(",", ".")
    s = RE_FLOAT.sub("", s)
    try:
        return float(s) if s else None
    except ValueError:
        return None


def extract_int_rooms(s: str | None) -> int | None:
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None


def first_image(images_csv: str | None) -> str | None:
    if not images_csv:
        return None
    return images_csv.split(",")[0].strip()


def to_usd(price: float | None, currency: str | None, fx_kgs_usd: float) -> float | None:
    if price is None:
        return None
    if (currency or "").upper() == "USD":
        return price
    if (currency or "").upper() in {"KGS", "SOM", "СОМ"}:
        return round(price / fx_kgs_usd, 2)
    # Если валюта не KGS/USD, возвращаем как есть (возможно, это уже USD)
    return price



def make_session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.mount("http://", HTTPAdapter(max_retries=retries))
    sess.headers.update(HEADERS)
    return sess



@dataclass
class ListCard:
    title: Optional[str]
    price_text: Optional[str]
    city: Optional[str]
    date: Optional[str]
    url: str


@dataclass
class FlatDetail:
    # базовые
    url: str
    title: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    city: Optional[str] = None
    created: Optional[str] = None
    updated: Optional[str] = None
    ad_id: Optional[str] = None

    # статистика/продавец
    shows: Optional[int] = None            # "Показы"
    views: Optional[int] = None            # глаз
    favorites: Optional[int] = None        # сердце
    seller_name: Optional[str] = None
    seller_is_pro: Optional[bool] = None
    phone_mask: Optional[str] = None

    # параметры квартиры
    rooms: Optional[str] = None            # "1 комната"
    area_m2: Optional[float] = None        # 42.0
    floor: Optional[int] = None            # 7
    floors_total: Optional[int] = None     # 10
    district: Optional[str] = None         # "ТРЦ Технопарк"
    series: Optional[str] = None           # "Элитка"
    documents: Optional[str] = None        # "Красная книга; ..."
    heating: Optional[str] = None          # "Центральное отопление"
    repair: Optional[str] = None           # "ПСО ..."
    features: Optional[str] = None         # "Бронированные двери; ..."
    offer_type: Optional[str] = None       # "Агентство недвижимости"
    deal_type: Optional[str] = None        # "Наличный расчет"

    # контент
    description: Optional[str] = None
    images: Optional[str] = None           # через запятую

    # JSON-LD (страховка)
    jl_title: Optional[str] = None
    jl_price: Optional[float] = None
    jl_currency: Optional[str] = None
    jl_date: Optional[str] = None
    jl_location: Optional[str] = None

    # служебное
    error: Optional[str] = None



def collect_list_links(sess: requests.Session) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()

    print(f"  [>] Начинаю сбор ссылок (макс. {MAX_PAGES} стр. / {MAX_ITEMS} шт.)")
    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
        try:
            r = sess.get(url, timeout=30)
            if r.status_code >= 400:
                print(f"  [!] Ошибка {r.status_code} на странице {page}. Завершаю сбор.")
                break
        except requests.RequestException as e:
            print(f"  [!] Ошибка сети на странице {page}: {e}. Завершаю сбор.")
            break

        soup = BeautifulSoup(r.text, "lxml")
        found = 0

        # Карточки в листинге
        for a in soup.select('article.ad-tile-horizontal a.ad-tile-horizontal-link[href]'):
            href = a.get("href")
            if not href:
                continue
            if ("/item/" in href) or ("/ads/" in href) or ("/ad/" in href):
                abs_url = normalize_url(urljoin(url, href))
                if abs_url not in seen:
                    seen.add(abs_url)
                    links.append(abs_url)
                    found += 1
                    if len(links) >= MAX_ITEMS:
                        print(f"  [+] Достигнут лимит MAX_ITEMS={MAX_ITEMS}.")
                        return links

        print(f"  [>] Стр. {page:02d}: найдено {found} новых ссылок (всего {len(links)})")

        # если на странице ничего нового — завершаем
        if found == 0:
            print(f"  [+] На странице {page} нет новых ссылок. Завершаю сбор.")
            break

        _sleep_rng(*PAGE_SLEEP)

    print(f"  [+] Сбор ссылок завершен. Всего: {len(links)}")
    return links


def extract_jsonld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            try:
                # Попытка очистить комментарии, если они есть
                data = json.loads(re.sub(r"//.*", "", raw, flags=re.MULTILINE))
            except Exception:
                continue
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            if isinstance(d, dict) and d.get("@type") in {"Product", "Offer", "ClassifiedAd", "OfferForLease"}:
                return d
    return None


def parse_flat_page(sess: requests.Session, url: str) -> FlatDetail:
    row = FlatDetail(url=normalize_url(url))
    try:
        r = sess.get(row.url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Заголовок (бывает разная комбинация классов)
        h1 = soup.select_one("h1.ad-detail-title, h1[class*='ad-detail-title']")
        row.title = h1.get_text(strip=True) if h1 else None

        # Статистика: показы / просмотры / избранное
        # Показы: внутри .details-page__statistic-bar .impressions span
        impressions = soup.select_one(".details-page__statistic-bar .impressions span")
        if impressions:
            m = re.search(r"(\d[\d\s\u00A0]*)", impressions.get_text())
            if m:
                row.shows = int(NUM_ONLY_RE.sub("", m.group(1)))

        stat_items = soup.select(".details-page__statistic-bar ul li")
        if len(stat_items) >= 1:
            vtxt = stat_items[0].get_text(" ", strip=True)
            vnum = to_float(vtxt)
            row.views = int(vnum) if vnum is not None else None
        if len(stat_items) >= 2:
            ftxt = stat_items[1].get_text(" ", strip=True)
            fnum = to_float(ftxt)
            row.favorites = int(fnum) if fnum is not None else None

        # Продавец/контакты
        sname = soup.select_one(".userName-text")
        row.seller_name = sname.get_text(strip=True) if sname else None
        row.seller_is_pro = soup.select_one(".userName .pro-label, .userName-text + .pro-label, .pro-label") is not None
        pmask = soup.select_one(".phone-wrap p")
        row.phone_mask = pmask.get_text(strip=True) if pmask else None

        # Параметры (табличка справа под заголовком)
        for li in soup.select("ul.details-page__params > li"):
            label = li.select_one("p")
            if not label:
                continue
            key = label.get_text(strip=True).lower()
            vals = [x.get_text(strip=True) for x in li.select("a, p")[1:]]
            val = "; ".join(v for v in vals if v)

            if "количество комнат" in key:
                row.rooms = val or None
            elif "площадь" in key and "м2" in key:
                try:
                    row.area_m2 = to_float(vals[0] if vals else None)
                except Exception:
                    row.area_m2 = None
            elif key.startswith("этаж:") or key == "этаж:" or "этаж:" in key:
                # на странице "Этаж" часто оформлен как ссылка <a>7</a>
                try:
                    row.floor = int(NUM_ONLY_RE.sub("", (vals[0] if vals else "") or "") or 0) or None
                except Exception:
                    row.floor = None
            elif "этажей в доме" in key:
                try:
                    row.floors_total = int(NUM_ONLY_RE.sub("", (vals[0] if vals else "") or "") or 0) or None
                except Exception:
                    row.floors_total = None
            elif "район бишкека" in key or "район" in key:
                row.district = val or None
            elif "серия" in key:
                row.series = val or None
            elif "правоустанавливающие" in key:
                row.documents = val or None
            elif "отопление" in key:
                row.heating = val or None
            elif "ремонт" in key:
                row.repair = val or None
            elif "дополнительно" in key:
                row.features = val or None
            elif "тип предложения" in key:
                row.offer_type = val or None
            elif "тип сделки" in key:
                row.deal_type = val or None

        # Описание
        descr = soup.select_one(".description .description__wrap")
        row.description = descr.get_text(" ", strip=True) if descr else None

        # Город (под картой)
        city = soup.select_one(".map-with-city-marker p.LFParagraph")
        row.city = city.get_text(strip=True) if city else None
        if not row.city and row.title:
            m = CITY_RE.search(row.title)
            if m:
                row.city = m.group(1)

        # Даты и ID
        dates = soup.select(".about-ad-info__date")
        if len(dates) >= 1:
            spans = dates[0].select("span")
            if len(spans) >= 2:
                row.created = spans[1].get_text(strip=True)
        if len(dates) >= 2:
            spans = dates[1].select("span")
            if len(spans) >= 2:
                row.updated = spans[1].get_text(strip=True)
        id_span = soup.select_one(".about-ad-info__id span")
        if id_span:
            m = re.search(r"\bID\s+(\d+)", id_span.get_text())
            if m:
                row.ad_id = m.group(1)

        # Картинки (со слайдера)
        imgs = []
        for pic in soup.select('.slider-component__item img[src]'):
            src = pic.get("src") or ""
            if src.startswith("//"):
                src = "https:" + src
            if src.startswith("http"):
                imgs.append(src)
        if imgs:
            uniq = list(dict.fromkeys(imgs))
            row.images = ", ".join(uniq)

        # Цена/валюта — через JSON-LD надёжнее
        j = extract_jsonld(soup)
        if j:
            row.jl_title = j.get("name") or j.get("headline")
            offers = j.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
                c = offers.get("priceCurrency") or (offers.get("priceSpecification") or {}).get("priceCurrency")
                row.jl_price = float(p) if p not in (None, "") else None
                row.jl_currency = c
            elif isinstance(offers, list) and offers:
                p = offers[0].get("price")
                c = offers[0].get("priceCurrency")
                row.jl_price = float(p) if p not in (None, "") else None
                row.jl_currency = c

            row.jl_date = j.get("datePosted") or j.get("datePublished")

            loc = j.get("areaServed") or j.get("address") or j.get("availableAtOrFrom")
            if isinstance(loc, dict):
                row.jl_location = ", ".join(filter(None, [
                    loc.get("addressLocality"),
                    loc.get("addressRegion"),
                    loc.get("addressCountry"),
                    loc.get("name"),
                ])) or None
            elif isinstance(loc, str):
                row.jl_location = loc

        # запасной вариант цены из DOM
        if row.jl_price is None:
            price_tag = soup.select_one('[class*="price"], .price, .ad-detail-price')
            if price_tag:
                txt = price_tag.get_text(" ", strip=True)
                num = to_float(txt)
                row.price = num
                low = txt.lower()
                if "usd" in low or "$" in low:
                    row.currency = "USD"
                elif "сом" in low or "kgs" in low:
                    row.currency = "KGS"

        # приоритет JSON-LD
        if row.jl_price is not None:
            row.price = row.jl_price
        if row.jl_currency:
            row.currency = row.jl_currency

        return row

    except Exception as e:
        row.error = str(e)
        return row


def save_mint_csv(rows: list[FlatDetail], path: Path, fx_kgs_usd: float):
    fieldnames = ["id","address","square_meters","rooms","price_usd","photo_url","legal_docs_url"]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            price_primary = r.price if r.price is not None else r.jl_price
            currency_primary = r.currency if r.currency else r.jl_currency
            w.writerow({
                "id": r.ad_id or "",
                "address": ", ".join(filter(None, [r.city, r.district])) or (r.city or ""),
                "square_meters": r.area_m2 or "",
                "rooms": extract_int_rooms(r.rooms) or "",
                "price_usd": to_usd(price_primary, currency_primary, fx_kgs_usd) or "",
                "photo_url": first_image(r.images) or "",
                "legal_docs_url": "",  # TODO: подставляйте сюда ваш URL на договор/скан
            })


def scrape():
    sess = make_session()
    print(f"Собираю ссылки из: {BASE_URL}")
    links = collect_list_links(sess)
    
    if not links:
        print("Ссылок не найдено. Завершение работы.")
        return

    rows: List[FlatDetail] = []
    if DEEP_SCRAPE:
        print(f"\n[>] Начинаю глубокий парсинг {len(links)} карточек...")
        for i, url in enumerate(links, 1):
            row = parse_flat_page(sess, url)
            rows.append(row)
            
            if row.error:
                print(f"  [!] Ошибка {i}/{len(links)}: {row.url} ({row.error})")
            
            if i % 25 == 0 or i == len(links):
                print(f"  [>] обработано {i}/{len(links)}")
            
            _sleep_rng(*ITEM_SLEEP)
        print("[+] Глубокий парсинг завершен.")
    else:
        print("[>] Глубокий парсинг (DEEP_SCRAPE=False) отключен.")
        rows = [FlatDetail(url=url) for url in links]

    # Сохранение
    print("\n[>] Начинаю сохранение результатов...")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys()) if rows else [
        # порядок колонок по умолчанию, если rows пустой
        'url','title','price','currency','city','created','updated','ad_id',
        'shows','views','favorites','seller_name','seller_is_pro','phone_mask',
        'rooms','area_m2','floor','floors_total','district','series','documents',
        'heating','repair','features','offer_type','deal_type','description',
        'images','jl_title','jl_price','jl_currency','jl_date','jl_location','error'
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))
    print(f"✅ CSV сохранён: {OUT_CSV.resolve()}")

    if SAVE_JSON:
        with OUT_JSON.open("w", encoding="utf-8") as f:
            json.dump([asdict(r) for r in rows], f, ensure_ascii=False, indent=2)
        print(f"✅ JSON сохранён: {OUT_JSON.resolve()}")

    # Сохранение CSV для минтинга
    mint_csv_path = Path("apartments_for_mint.csv")
    save_mint_csv(rows, mint_csv_path, fx_kgs_usd=MINT_CSV_FX_RATE)
    print(f"✅ CSV для минтинга сохранён: {mint_csv_path.resolve()}")


if __name__ == "__main__":
    scrape()
