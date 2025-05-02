import time
import requests
from bs4 import BeautifulSoup
import re
import lxml

# Configuration globale
REQUEST_PAUSE = 1  # Pause entre chaque requête (en secondes)
REQUEST_TIMEOUT = 10  # Timeout pour les requêtes (en secondes)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_text_or_none(element):
    """Récupère le texte d'un élément BeautifulSoup ou retourne None."""
    return element.get_text(strip=True) if element and hasattr(element, "get_text") else None

def clean_price(price_text):
    """Nettoie et convertit un texte de prix en float."""
    price_clean = re.sub(r'[^\d,\.]', '', price_text).replace(',', '.')
    match = re.search(r'(\d+(\.\d+)?)', price_clean)
    return float(match.group(1)) if match else None

def extract_product_info(url, domain):
    try:

        # Requête HTTP
        res = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()  # Lève une exception si le code HTTP >= 400
        http_status = res.status_code  # Capture du code HTTP
        soup = BeautifulSoup(res.content, 'lxml')

        name = None
        price_text = None

        # Extraction des données en fonction du domaine
        if "lepetitvapoteur.com" in domain:
            name = get_text_or_none(soup.find("h1", {"class": "product-title"}).find("span"))
            price_text = get_text_or_none(soup.find("div", {"id": "block-achat-wrap"}).find("span", class_="our_price_display"))
        elif "taklope.com" in domain:
            name = get_text_or_none(soup.find("h1", {"class": "c-pdt__title"}))
            price_tag = soup.find("div", class_="product-prices").find("span", class_="c-price--old")
            if not price_tag:
                price_tag = soup.find("div", class_="product-prices").find("span", class_="c-price--current")
            price_text = get_text_or_none(price_tag)
        elif "kumulusvape.fr" in domain:
            name = get_text_or_none(soup.find("h1", id="h1_title"))
            price_section = soup.find("div", class_="price")
            determined_price_text = None
            if price_section:
                old_price_tag = price_section.find("span", id="old_price_display")
                old_price_text_raw = get_text_or_none(old_price_tag)
                validated_original_price = clean_price(old_price_text_raw) if old_price_text_raw else None

                if validated_original_price is not None:
                    determined_price_text = old_price_text_raw
                else:
                    current_price_tag = price_section.find("span", id="our_price_display")
                    current_price_text_raw = get_text_or_none(current_price_tag)
                    if current_price_text_raw:
                        determined_price_text = current_price_text_raw
            price_text = determined_price_text
        elif "cigaretteelec.fr" in domain:
            reduction_display = soup.find("div", {"id": "reduction_display"})
            price_display = soup.find("span", {"id": "our_price_display"})
            if reduction_display and "o-0" not in reduction_display.get("class", ""):
                old_price = reduction_display.find("span", {"id": "old_price"})
                price_text = get_text_or_none(old_price)
            elif price_display:
                price_text = get_text_or_none(price_display)
            name = get_text_or_none(soup.select_one("div.notranslate span.name"))

        # Nettoyer et convertir le prix
        price = clean_price(price_text) if price_text else None

        # Retourner le résultat
        return name, price, http_status

    except requests.exceptions.HTTPError as e:
        print(f"[Erreur HTTP] {domain} / {url} : {e}")
        return None, None, e.response.status_code
    except requests.exceptions.RequestException as e:
        print(f"[Erreur Requête] {domain} / {url} : {e}")
        return None, None, "RequestError"
    except Exception as e:
        print(f"[Erreur scraping] {domain} / {url} : {e}")
        return None, None, "UnknownError"
