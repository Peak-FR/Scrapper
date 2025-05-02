import sys
import os
import time
import re
import queue # For queue.Empty exception
import traceback
import logging
import multiprocessing # For manual process and queues
import threading # For thread pool worker logging

# Third-Party Imports
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from Levenshtein import ratio as similarity_ratio
import concurrent.futures # For ThreadPoolExecutor

# GUI / Tkinter Imports (if needed directly, otherwise handled by root/callback)
from tkinter import Toplevel, messagebox

# Local Application Imports
# Assurez-vous que ces fichiers existent et sont corrects
from scraper_utils import extract_product_info # SANS time.sleep() !
from serper_utils import search_google_serper # Clé API sécurisée !
from results_viewer import ResultsViewer
from global_store import GlobalStore

# --- Setup Logging ---
logger = logging.getLogger(__name__)
# Configurez le logging de base si aucun handler n'est attaché (ex: si exécuté seul)
if not logger.hasHandlers():
    log_format = '%(asctime)s - %(levelname)s - [%(processName)s/%(threadName)s] %(name)s - %(message)s'
    # Configurer pour voir les logs des threads et processus
    logging.basicConfig(level=logging.INFO, format=log_format)


# --- Fonctions Utilitaires & Constantes ---
def get_resource_path(relative_path):
    """Obtenir le chemin absolu d'une ressource empaquetée avec PyInstaller."""
    try:
        # PyInstaller crée un dossier temporaire et stocke le chemin dans _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Configuration Google API
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
# Assurez-vous que credentials.json est inclus lors de la compilation
CREDENTIALS_FILE = get_resource_path("credentials.json")
SPREADSHEET_NAME = "ScraperData"
PRODUCTS_URL_SHEET = "products_url"
MANUAL_VERIFICATION_SHEET = "verification_manuelle"

# Paramètres Scraping
DEFAULT_COMPETITOR_DOMAINS = ['lepetitvapoteur.com', 'taklope.com', 'kumulusvape.fr', 'cigaretteelec.fr']
SIMILARITY_THRESHOLD = 0.55 # Seuil pour considérer les noms comme similaires

# Colonnes requises pour les DFs
REQUIRED_COLUMNS_VERIFICATION = ["MonNomProduit", "Concurrent", "URLConcurrent"]
REQUIRED_COLUMNS_PRODUCTS_URL = ["NomProduit", "CompetitorDomain", "URLConcurrent"]

# --- Google Sheets Client Singleton ---
GSPREAD_CLIENT = None
gs_client_lock = threading.Lock() # Sécurité si plusieurs threads accèdent en même temps

def get_gspread_client():
    """Initialise et retourne le client gspread autorisé (Singleton thread-safe)."""
    global GSPREAD_CLIENT
    # Double-checked locking pour la performance
    if GSPREAD_CLIENT is None:
        with gs_client_lock:
            if GSPREAD_CLIENT is None:
                logger.info("Initialisation du client Gspread (une seule fois)...")
                try:
                    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
                    GSPREAD_CLIENT = gspread.authorize(creds)
                    logger.info("Client Gspread initialisé avec succès.")
                except FileNotFoundError:
                    logger.critical(f"ERREUR CRITIQUE: Fichier credentials '{CREDENTIALS_FILE}' non trouvé.")
                    raise
                except Exception as e:
                    logger.critical(f"ERREUR CRITIQUE lors de l'initialisation gspread: {e}", exc_info=True)
                    raise
    return GSPREAD_CLIENT

# --- Fonctions Google Sheets (Chargement/Sauvegarde) ---
# NOTE: Ces fonctions restent un goulot d'étranglement potentiel pour de gros volumes.
# Elles chargent/sauvegardent l'intégralité des feuilles.

def load_sheets_into_memory():
    """Charge les feuilles Google Sheets dans des DataFrames dans GlobalStore."""
    try:
        logger.info("Début du chargement des feuilles Google Sheets...")
        GlobalStore.verification_df = load_verification_sheet()
        logger.info(f"Feuille verification_manuelle chargée: {GlobalStore.verification_df.shape if GlobalStore.verification_df is not None else 'None'} lignes.")
        GlobalStore.products_url_df = load_products_url_sheet()
        logger.info(f"Feuille products_url chargée: {GlobalStore.products_url_df.shape if GlobalStore.products_url_df is not None else 'None'} lignes.")

        # Initialiser à vide si le chargement a échoué
        if GlobalStore.verification_df is None:
            logger.warning("verification_df est None après chargement. Initialisation à vide.")
            GlobalStore.verification_df = pd.DataFrame(columns=REQUIRED_COLUMNS_VERIFICATION)
        if GlobalStore.products_url_df is None:
            logger.warning("products_url_df est None après chargement. Initialisation à vide.")
            GlobalStore.products_url_df = pd.DataFrame(columns=REQUIRED_COLUMNS_PRODUCTS_URL)

    except Exception as e:
        logger.error(f"Erreur lors du chargement des feuilles Google Sheets : {e}", exc_info=True)
        raise

def save_sheets_to_google():
    """Sauvegarde les DataFrames modifiés (depuis GlobalStore) dans Google Sheets."""
    # Cette fonction est appelée SYNCHRONEMENT à la fin. Peut être longue.
    sheets_saved = False
    try:
        if GlobalStore.verification_changed:
            logger.info("Sauvegarde des modifications dans verification_manuelle...")
            # Utilise le DF potentiellement mis à jour dans GlobalStore
            update_sheet_from_dataframe(MANUAL_VERIFICATION_SHEET, GlobalStore.verification_df)
            GlobalStore.verification_changed = False # Reset flag APRES succès
            sheets_saved = True
        if GlobalStore.products_url_changed:
            logger.info("Sauvegarde des modifications dans products_url...")
            update_sheet_from_dataframe(PRODUCTS_URL_SHEET, GlobalStore.products_url_df)
            GlobalStore.products_url_changed = False # Reset flag APRES succès
            sheets_saved = True

        if not sheets_saved:
             logger.info("Aucun changement détecté dans GlobalStore, sauvegarde Google Sheets ignorée.")

    except Exception as e:
       logger.error(f"Erreur lors de la sauvegarde Google Sheets via save_sheets_to_google: {e}", exc_info=True)
       raise # Remonter l'erreur pour l'afficher à l'utilisateur

def load_verification_sheet():
    """Charge la feuille 'verification_manuelle'."""
    try:
        g_client = get_gspread_client()
        logger.info(f"Chargement de la feuille : {MANUAL_VERIFICATION_SHEET}")
        sheet = g_client.open(SPREADSHEET_NAME).worksheet(MANUAL_VERIFICATION_SHEET)
        data = sheet.get_all_records() # API Call
        if not data: return pd.DataFrame(columns=REQUIRED_COLUMNS_VERIFICATION)

        df = pd.DataFrame(data)
        # Assurer présence colonnes et types
        for col in REQUIRED_COLUMNS_VERIFICATION:
            if col not in df.columns: df[col] = ""
        df = df[REQUIRED_COLUMNS_VERIFICATION].astype(str)
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la feuille '{MANUAL_VERIFICATION_SHEET}': {e}", exc_info=True)
        return None # Retourner None pour indiquer l'échec

def load_products_url_sheet():
    """Charge la feuille 'products_url'."""
    try:
        g_client = get_gspread_client()
        logger.info(f"Chargement de la feuille : {PRODUCTS_URL_SHEET}")
        sheet = g_client.open(SPREADSHEET_NAME).worksheet(PRODUCTS_URL_SHEET)
        data = sheet.get_all_records() # API Call
        if not data: return pd.DataFrame(columns=REQUIRED_COLUMNS_PRODUCTS_URL)

        df = pd.DataFrame(data)
        for col in REQUIRED_COLUMNS_PRODUCTS_URL:
             if col not in df.columns: df[col] = ""
        df = df[REQUIRED_COLUMNS_PRODUCTS_URL].astype(str)
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la feuille '{PRODUCTS_URL_SHEET}': {e}", exc_info=True)
        return None # Retourner None pour indiquer l'échec

def update_sheet_from_dataframe(sheet_name, dataframe):
    """Met à jour une feuille Google Sheets (API Call)."""
    # Attention: clear + update est inefficace pour gros volumes
    try:
        if dataframe is None:
            logger.warning(f"DataFrame pour '{sheet_name}' est None, mise à jour annulée.")
            return
        logger.info(f"Tentative de mise à jour de la feuille : {sheet_name} ({dataframe.shape[0]} lignes locales)")
        g_client = get_gspread_client()
        sheet = g_client.open(SPREADSHEET_NAME).worksheet(sheet_name)

        # Préparation des données
        if sheet_name == MANUAL_VERIFICATION_SHEET: cols = REQUIRED_COLUMNS_VERIFICATION
        elif sheet_name == PRODUCTS_URL_SHEET: cols = REQUIRED_COLUMNS_PRODUCTS_URL
        else: cols = dataframe.columns.tolist()

        df_to_update = dataframe.copy()
        for col in cols:
            if col not in df_to_update.columns: df_to_update[col] = ''
        df_to_update = df_to_update[cols] # Réordonner
        df_to_update = df_to_update.fillna('') # Remplacer NaN par str vide

        # Convertir tout en string pour éviter problèmes de type avec gspread
        data_to_update = [df_to_update.columns.tolist()] + df_to_update.astype(str).values.tolist()

        logger.info(f"Effacement et mise à jour de '{sheet_name}' avec {len(data_to_update)-1} lignes de données...")
        # --- API Calls ---
        sheet.clear()
        sheet.update("A1", data_to_update, value_input_option='USER_ENTERED')
        # -----------------
        logger.info(f"Feuille '{sheet_name}' mise à jour avec succès.")
    except gspread.exceptions.APIError as api_err:
        logger.error(f"ERREUR API Google Sheets lors de la mise à jour de '{sheet_name}': {api_err}", exc_info=True)
        raise # Remonter pour gestion erreur globale
    except Exception as e:
        logger.error(f"Erreur générique lors de la mise à jour de la feuille '{sheet_name}': {e}", exc_info=True)
        raise

# --- Fonctions de manipulation des DFs LOCAUX ---
# Ces fonctions modifient les DFs passés en argument (copies locales)
# et retournent True si une modification a eu lieu.

def add_to_manual_verification(product_name, domain, verification_df, url=None):
    """Ajoute une ligne au DF de vérification (si pas déjà présente)."""
    if verification_df is None: return False
    # Assurer que les colonnes nécessaires existent
    if not all(col in verification_df.columns for col in ["MonNomProduit", "Concurrent"]):
        logger.error("Colonnes manquantes dans verification_df pour add_to_manual.")
        # Option: recréer les colonnes si le DF est vide ?
        if verification_df.empty:
            for col in REQUIRED_COLUMNS_VERIFICATION:
                 if col not in verification_df.columns: verification_df[col] = pd.Series(dtype='str')
        else:
             return False # Ne pas ajouter si colonnes manquantes sur DF non vide

    # Vérifier si le couple existe déjà
    mask = (verification_df["MonNomProduit"] == product_name) & \
           (verification_df["Concurrent"] == domain)
    if not mask.any():
        new_row = pd.DataFrame([{
            "MonNomProduit": product_name,
            "Concurrent": domain,
            "URLConcurrent": str(url or '') # Assurer string
        }])[REQUIRED_COLUMNS_VERIFICATION] # Assurer l'ordre et présence colonnes
        # Utiliser concat est plus sûr pour ajouter une ligne
        verification_df = pd.concat([verification_df, new_row], ignore_index=True)
        logger.debug(f"Ajouté (localement) à verification: {product_name}/{domain}")
        return True # Indique qu'un ajout a été fait
    return False # Déjà présent, pas d'ajout

def remove_from_manual_verification(product_name, domain, verification_df):
    """Supprime des lignes du DF de vérification."""
    if verification_df is None or verification_df.empty: return False
    if not all(col in verification_df.columns for col in ["MonNomProduit", "Concurrent"]):
        logger.error("Colonnes manquantes dans verification_df pour remove_from_manual.")
        return False

    initial_len = len(verification_df)
    mask = (verification_df["MonNomProduit"] == product_name) & \
           (verification_df["Concurrent"] == domain)

    if mask.any():
        # Conserver uniquement les lignes qui ne correspondent PAS au masque
        verification_df = verification_df[~mask].reset_index(drop=True)
        if len(verification_df) < initial_len:
             logger.debug(f"Supprimé (localement) de verification: {product_name}/{domain}")
             return True # Indique une suppression
    return False # Rien à supprimer

def save_or_update_url(product_name, domain, url, products_url_df):
    """Met à jour ou ajoute une URL dans le DF products_url."""
    if products_url_df is None: return False
    if not all(col in products_url_df.columns for col in ["NomProduit", "CompetitorDomain", "URLConcurrent"]):
         logger.error("Colonnes manquantes dans products_url_df pour save_or_update.")
         if products_url_df.empty:
             for col in REQUIRED_COLUMNS_PRODUCTS_URL:
                  if col not in products_url_df.columns: products_url_df[col] = pd.Series(dtype='str')
         else:
              return False

    url_str = str(url or '') # Assurer string
    mask = (products_url_df["NomProduit"] == product_name) & \
           (products_url_df["CompetitorDomain"] == domain)

    if mask.any(): # Mise à jour
        # Vérifier si l'URL a vraiment changé
        current_url = products_url_df.loc[mask, "URLConcurrent"].iloc[0]
        if current_url != url_str:
            products_url_df.loc[mask, "URLConcurrent"] = url_str
            logger.debug(f"URL MàJ (localement) pour {product_name}/{domain}")
            return True # Changement effectué
        return False # URL identique
    else: # Ajout
        new_row = pd.DataFrame([{
            "NomProduit": product_name,
            "CompetitorDomain": domain,
            "URLConcurrent": url_str
        }])[REQUIRED_COLUMNS_PRODUCTS_URL]
        products_url_df = pd.concat([products_url_df, new_row], ignore_index=True)
        logger.debug(f"URL ajoutée (localement) pour {product_name}/{domain}")
        return True # Ajout effectué

# --- Fonction de calcul (inchangée) ---
def calculate_price_difference(my_price, competitor_price):
    """Calcule la différence de prix en pourcentage."""
    if competitor_price is not None and my_price is not None:
        try:
            my_p = float(my_price)
            comp_p = float(competitor_price)
            if comp_p > 0: return round(((my_p - comp_p) / comp_p) * 100, 2)
            elif comp_p == 0 and my_p > 0: return float('inf') # Concurrent gratuit, moi payant
            elif comp_p == 0 and my_p == 0: return 0.0 # Tous les deux gratuits
            else: return "N/A" # Prix concurrent négatif?
        except (ValueError, TypeError):
             logger.warning(f"Impossible de calculer diff prix: {my_price} vs {competitor_price}", exc_info=False)
             return "Erreur Type"
    return "N/A"

# --- Logique de Scraping LPV (Refactorisée) ---

def scrape_with_selenium_lpv_core(driver, url):
    """
    Logique de scraping pour une URL LPV avec une instance de driver EXISTANTE.
    Retourne (name, price, status). Status peut être 200, 404, 408, etc.
    """
    # Imports Selenium nécessaires (au cas où non importés globalement dans le worker)
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

    logger_lpv_core = logging.getLogger(f"{__name__}.lpv_core.{os.getpid()}") # Utiliser PID pour différentier

    try:
        logger_lpv_core.info(f"Chargement page LPV: {url}")
        driver.get(url)

        # Attente WebDriverWait (ajuster timeout si nécessaire)
        timeout_sec = 20 # Augmenté un peu
        wait = WebDriverWait(driver, timeout_sec)

        # Attendre que le prix OU un indicateur de page chargée soit présent
        # Le sélecteur original 'span.our_price_display' est peut-être trop spécifique
        # Essayer d'attendre le conteneur du prix ou le titre H1
        # wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#block-achat-wrap"))) # Conteneur prix
        # Ou attendre le titre principal
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product-title span")))

        logger_lpv_core.debug(f"Page chargée (attente OK) pour {url}")

        # Extraction Nom
        product_name = None
        try:
            name_element = driver.find_element(By.CSS_SELECTOR, "h1.product-title span")
            product_name = name_element.text.strip() if name_element else None
        except NoSuchElementException:
            logger_lpv_core.warning(f"Sélecteur nom non trouvé pour {url}")

        # Extraction Prix
        price = None
        price_text = None
        try:
            # Essayer le prix normal d'abord
            price_element = driver.find_element(By.CSS_SELECTOR, "span.our_price_display")
            price_text = price_element.text.strip() if price_element else None
        except NoSuchElementException:
             logger_lpv_core.warning(f"Prix normal non trouvé ('span.our_price_display') pour {url}")
             # Essayer de trouver un prix barré s'il existe (peut indiquer rupture ou autre page)
             try:
                  old_price_element = driver.find_element(By.CSS_SELECTOR, "span#old_price_display")
                  price_text = old_price_element.text.strip() if old_price_element else None
                  logger_lpv_core.info(f"Prix barré trouvé ('span#old_price_display') pour {url}")
             except NoSuchElementException:
                   logger_lpv_core.error(f"Aucun sélecteur de prix trouvé pour {url}")

        # Nettoyage Prix
        if price_text:
            try:
                price_cleaned = re.sub(r'[^\d,.]', '', price_text).replace(',', '.').strip()
                price_match = re.search(r'(\d+\.\d+|\d+)', price_cleaned) # Cherche X.Y ou juste X
                if price_match:
                    price = float(price_match.group(1))
                else:
                     logger_lpv_core.error(f"Format prix LPV non reconnu après nettoyage: '{price_cleaned}' depuis '{price_text}'")
            except ValueError:
                logger_lpv_core.error(f"Impossible de convertir le prix LPV nettoyé '{price_cleaned}' en float.")
        else:
             logger_lpv_core.error(f"Aucun texte de prix trouvé dans les éléments pour {url}")


        # Vérification finale
        if product_name and price is not None:
            logger_lpv_core.info(f"Succès LPV: Name='{product_name}', Price={price}")
            return product_name, price, 200
        else:
            logger_lpv_core.error(f"Échec extraction LPV (nom ou prix manquant): Name={product_name}, Price={price}, URL={url}")
            # Retourner 404 si on pense que les éléments n'étaient pas là
            # Ou un autre code si on suspecte un chargement partiel?
            return product_name, price, 404 # Indique que les données n'ont pas été trouvées comme attendu

    except TimeoutException:
        logger_lpv_core.error(f"Timeout ({timeout_sec}s) lors du chargement/attente pour {url}")
        return None, None, 408 # Request Timeout
    except NoSuchElementException as nse:
         logger_lpv_core.error(f"Élément Selenium non trouvé (probablement après attente OK?) pour {url}: {nse}", exc_info=False)
         return None, None, 404 # Not Found
    except WebDriverException as wde:
         # Erreurs diverses: navigateur crashé, connexion perdue, etc.
         logger_lpv_core.error(f"Erreur WebDriver LPV pour {url}: {wde}", exc_info=True)
         # Retourner un code indiquant une erreur serveur/navigateur
         return None, None, 503 # Service Unavailable (approximatif)
    except Exception as e:
        logger_lpv_core.error(f"Erreur inattendue scraping LPV Core pour {url}: {e}", exc_info=True)
        return None, None, 500 # Internal Server Error (approximatif)


def persistent_lpv_worker(url_queue, result_queue):
    """
    Worker process persistant pour LPV.
    Initialise Selenium une fois, traite les URLs de url_queue,
    met les résultats dans result_queue.
    """
    pid = os.getpid()
    worker_logger = logging.getLogger(f"{__name__}.lpv_persist.{pid}")
    # Configuration logging basique pour ce processus
    log_format = '%(asctime)s - %(levelname)s - [%(process)d] %(name)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format, force=True)

    worker_logger.info("Démarrage du worker LPV persistant.")

    # Imports nécessaires DANS ce processus
    import undetected_chromedriver as uc
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    import sys
    import traceback

    driver = None
    driver_path = None
    try:
        # --- Initialisation WebDriver ---
        worker_logger.info("Initialisation du driver Selenium LPV...")
        try:
            worker_logger.info("Recherche/Installation du chromedriver via webdriver-manager...")
            print(f"--- (LPV Worker {pid}) Juste avant ChromeDriverManager().install() ---", file=sys.stderr)
            driver_path = ChromeDriverManager().install()
            print(f"--- (LPV Worker {pid}) Juste après ChromeDriverManager().install(). Path: {driver_path} ---", file=sys.stderr)
            worker_logger.info(f"Utilisation de chromedriver trouvé/installé : {driver_path}")

            if not driver_path or not os.path.exists(driver_path):
                 raise FileNotFoundError("webdriver-manager a retourné un chemin invalide.")

            options = uc.ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920x1080")
            # Ajouter d'autres options si nécessaire (headless, proxy...)
            # options.add_argument('--headless=new') # Pour exécution sans fenêtre visible

            worker_logger.info("Lancement de uc.Chrome...")
            print(f"--- (LPV Worker {pid}) Juste avant uc.Chrome() ---", file=sys.stderr)
            driver = uc.Chrome(driver_executable_path=driver_path, options=options)
            print(f"--- (LPV Worker {pid}) Juste après uc.Chrome() ---", file=sys.stderr)
            worker_logger.info("Driver LPV initialisé avec succès.")

        except Exception as init_error:
            worker_logger.critical(f"Erreur CRITIQUE initialisation driver LPV: {init_error}", exc_info=True)
            print(f"!!! TRACEBACK LPV DRIVER INIT ERROR (PID {pid}) !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            print(f"!!! END TRACEBACK LPV DRIVER INIT ERROR (PID {pid}) !!!", file=sys.stderr)
            # Mettre une erreur sur la queue pour chaque tâche future? Non, on arrête le worker.
            # L'alternative serait de ne pas démarrer le worker et gérer ça dans le process principal.
            # Ici, on logue et on sort, le process principal timeout sur la queue.
            return # Arrête le worker si le driver ne démarre pas

        # --- Boucle de Traitement des URLs ---
        worker_logger.info("Worker LPV prêt. En attente d'URLs...")
        while True:
            task_data = None
            try:
                # Bloque jusqu'à recevoir une tâche ou None
                task_data = url_queue.get()

                # Signal de terminaison
                if task_data is None:
                    worker_logger.info("Signal de terminaison reçu.")
                    break

                # Vérifier si task_data est bien un dictionnaire attendu
                if not isinstance(task_data, dict) or 'url' not in task_data or 'my_product_name' not in task_data:
                     worker_logger.error(f"Donnée invalide reçue dans la queue LPV: {task_data}")
                     # Mettre un résultat d'erreur?
                     result_queue.put({
                         'status': 'InvalidTaskData', 'name': None, 'price': None,
                         'url': str(task_data), 'domain': 'lepetitvapoteur.com',
                         'my_product_name': 'Inconnu (Erreur Task Data)'
                     })
                     continue # Attendre la prochaine tâche

                url = task_data['url']
                my_product_name = task_data['my_product_name']
                worker_logger.info(f"Traitement URL LPV: {url} pour produit: {my_product_name}")

                # Appel de la logique de scraping principale
                name, price, status = scrape_with_selenium_lpv_core(driver, url)

                # Préparation du résultat
                result = {
                    'status': status,
                    'name': name,
                    'price': price,
                    'url': url,
                    'domain': 'lepetitvapoteur.com',
                    'my_product_name': my_product_name # Important pour corrélation
                }
                result_queue.put(result)
                worker_logger.debug(f"Résultat LPV mis dans la queue pour {my_product_name}")

            except queue.Empty:
                # Ne devrait pas arriver avec get() bloquant, mais par sécurité
                worker_logger.warning("Queue LPV vide (ne devrait pas arriver avec get bloquant)")
                time.sleep(0.1) # Petite pause
                continue
            except Exception as loop_err:
                 # Erreur inattendue dans la boucle principale du worker
                 worker_logger.error(f"Erreur boucle worker LPV: {loop_err}", exc_info=True)
                 # Essayer de mettre une erreur sur la queue pour la tâche courante
                 try:
                      result_queue.put({
                           'status': f'WorkerLoopError: {type(loop_err).__name__}',
                           'name': None, 'price': None, 'url': url if 'url' in locals() else 'Inconnue',
                           'domain': 'lepetitvapoteur.com',
                           'my_product_name': my_product_name if 'my_product_name' in locals() else 'Inconnu'
                      })
                 except Exception as q_err:
                       worker_logger.error(f"Impossible de mettre l'erreur de boucle sur la queue: {q_err}")
                 # Faut-il arrêter le worker ici? Pour l'instant, on continue.
                 # Si les erreurs persistent, le worker risque de boucler.

    finally:
        # Nettoyage: fermer le navigateur à la fin
        if driver:
            worker_logger.info("Fermeture du driver Selenium LPV...")
            try:
                driver.quit()
                worker_logger.info("Driver LPV fermé.")
            except Exception as quit_err:
                worker_logger.error(f"Erreur lors de driver.quit() LPV: {quit_err}", exc_info=True)
        worker_logger.info("Worker LPV persistant terminé.")

# --- Worker pour ThreadPoolExecutor (Non-LPV) ---
def _worker_task(my_product_name, domain, verification_df, products_url_df):
    """
    Fonction cible pour ThreadPoolExecutor (non-LPV).
    Gère recherche URL (cache, Serper) puis scraping (requests).
    Retourne un dictionnaire de résultat.
    Si domaine est LPV, retourne un statut spécial 'Requires LPV'.
    """
    # Utilise le nom du thread pour différencier les logs
    worker_logger = logging.getLogger(f"{__name__}.worker.{threading.current_thread().name}")
    worker_logger.debug(f"Début tâche pour {my_product_name} / {domain}")

    result = {'status': 'Init', 'name': None, 'price': None, 'url': None, 'domain': domain, 'my_product_name': my_product_name}
    # --- NORMALISATION pour la recherche ---
    try:
        norm_prod_name = my_product_name.strip().lower()
        norm_domain = domain.strip().lower()
        index_unique = f"{norm_prod_name}__{norm_domain}"
    except AttributeError: # Au cas où my_product_name ou domain ne sont pas des strings
         worker_logger.error(f"Impossible de normaliser les clés pour {my_product_name}/{domain}")
         result['status'] = 'KeyNormalizationError'
         return result # Retourner une erreur si on ne peut pas créer la clé
    # --- FIN NORMALISATION ---

    competitor_url = None
    found_in_verification = False
    verification_has_no_url = False

    # 1. Chercher URL dans cache local
    try:
        # Vérifier verification_manuelle
        if not verification_df.empty and "IndexUnique" in verification_df.columns and index_unique in verification_df["IndexUnique"].values:
            found_in_verification = True
            url_series = verification_df.loc[verification_df["IndexUnique"] == index_unique, "URLConcurrent"]
            competitor_url = url_series.iloc[0] if not url_series.empty else None
            if competitor_url and isinstance(competitor_url, str) and competitor_url.strip():
                worker_logger.debug(f"URL trouvée dans verification_df local: {competitor_url}")
                result['status'] = 'URL From Verification'
            else:
                worker_logger.info(f"Trouvé dans verification_manuelle mais sans URL valide: {my_product_name} / {domain}.")
                result['status'] = 'Verification No URL'
                verification_has_no_url = True

        # Vérifier products_url si pas trouvé/valide dans verification
        if not competitor_url and not verification_has_no_url and not products_url_df.empty and "IndexUnique" in products_url_df.columns and index_unique in products_url_df["IndexUnique"].values:
            worker_logger.debug(f"Vérification products_url_df pour index normalisé: '{index_unique}'") # Log modifié
            mask = products_url_df["IndexUnique"] == index_unique # Recherche avec clé normalisée
            if mask.any():    
                url_series = products_url_df.loc[mask, "URLConcurrent"]
                temp_url = url_series.iloc[0] if not url_series.empty else None
                worker_logger.debug(f"Match trouvé dans products_url_df! URL récupérée: '{temp_url}' (type: {type(temp_url)})") # LOG 2: Match trouvé et URL brute
                # Check validité
                if temp_url and isinstance(temp_url, str) and temp_url.strip():
                    competitor_url = temp_url
                    result['status'] = 'URL From Cache'
                    worker_logger.debug(f"URL depuis products_url_df considérée VALIDE.") # LOG 3a: URL valide
                else:
                    worker_logger.warning(f"Match trouvé dans products_url_df pour '{index_unique}', mais URL ('{temp_url}') considérée INVALIDE. Serper sera tenté.") # LOG 3b: URL invalide
            else:
                worker_logger.debug(f"Aucun match trouvé dans products_url_df pour index: '{index_unique}'. Serper sera tenté.") # LOG 4: Pas de match

    except Exception as e:
         worker_logger.error(f"Erreur recherche URL locale pour {index_unique}: {e}", exc_info=True)
         result['status'] = 'LocalLookupError'

    # 2. Chercher URL via Serper si nécessaire
    if not competitor_url and not verification_has_no_url:
        worker_logger.info(f"Appel à Serper car competitor_url='{competitor_url}' et verification_has_no_url={verification_has_no_url}") # LOG 5: Pourquoi on appelle Serper
        worker_logger.debug(f"Aucune URL locale valide. Recherche via Serper pour {my_product_name} / {domain}...")
        result['status'] = 'Searching Serper'
        try:
            serper_url = search_google_serper(my_product_name, domain) # Assurez-vous que cette fonction existe
            if serper_url and isinstance(serper_url, str) and serper_url.strip():
                competitor_url = serper_url
                worker_logger.info(f"URL trouvée via Serper: {competitor_url}")
                result['status'] = 'URL From Serper'
            else:
                worker_logger.info(f"Aucune URL trouvée via Serper pour {my_product_name} sur {domain}")
                result['status'] = 'Serper Not Found'
        except Exception as e:
             worker_logger.error(f"Erreur appel Serper pour {index_unique}: {e}", exc_info=True)
             result['status'] = 'Serper API Error'

    # 3. Scraper (non-LPV) ou Marquer pour LPV
    if competitor_url and isinstance(competitor_url, str) and competitor_url.strip():
        result['url'] = competitor_url

        if domain == "lepetitvapoteur.com":
            worker_logger.info(f"URL trouvée pour LPV ({competitor_url}). Marqué pour traitement Selenium.")
            result['status'] = 'Requires LPV' # Statut spécial
        else:
            worker_logger.debug(f"Scraping de {competitor_url} pour {domain}...")
            try:
                # Assurez-vous que extract_product_info existe et SANS time.sleep()
                comp_name, comp_price, http_status = extract_product_info(competitor_url, domain)
                result['status'] = http_status
                result['name'] = comp_name
                result['price'] = comp_price
                if str(http_status) == '200': worker_logger.info(f"Succès scraping {domain}: Name={comp_name}, Price={comp_price}")
                else: worker_logger.warning(f"Échec scraping {domain} avec status {http_status}")
            except Exception as e:
                 worker_logger.error(f"Erreur appel extract_product_info pour {competitor_url}: {e}", exc_info=True)
                 result['status'] = 'ScrapingException'

    elif not verification_has_no_url:
         if result['status'] not in ['Serper Not Found', 'Serper API Error', 'LocalLookupError']:
              worker_logger.warning(f"Aucune URL valide à traiter pour {my_product_name} / {domain}")
              result['status'] = 'No URL To Scrape'

    worker_logger.debug(f"Fin tâche pour {my_product_name} / {domain}. Statut final: {result.get('status')}")
    return result

# --- Fonction Principale (Refactorisée pour Worker Persistant LPV) ---
def process_products(root, competitors, input_csv, progress_callback=None):
    """
    Traitement principal des produits avec concurrence via ThreadPoolExecutor
    et un worker persistant (Process) pour LPV communiquant par Queues.
    La progression est mise à jour APRÈS traitement du résultat final.
    VERSION CORRIGÉE : Suppression de la duplication de création IndexUnique.
    """
    start_time_total = time.time()
    logger.info("=== Début de process_products (Version Worker LPV Persistant + Prog Corrigée + Fix IndexUnique) ===")

    # Initialisation
    results = []
    verification_df = None
    products_url_df = None
    verification_needs_global_update = False
    products_url_needs_global_update = False
    product_prices = {}

    # Communication avec le worker LPV
    lpv_url_queue = multiprocessing.Queue()
    lpv_result_queue = multiprocessing.Queue()
    lpv_process = None
    lpv_tasks_submitted_count = 0

    try: # Bloc try global
        # === Étape 1 : Charger les données initiales ===
        logger.info("Chargement des données initiales...")
        try:
            load_sheets_into_memory()
            verification_df = GlobalStore.verification_df.copy() if GlobalStore.verification_df is not None else pd.DataFrame(columns=REQUIRED_COLUMNS_VERIFICATION)
            products_url_df = GlobalStore.products_url_df.copy() if GlobalStore.products_url_df is not None else pd.DataFrame(columns=REQUIRED_COLUMNS_PRODUCTS_URL)
            logger.info(f"DFs locaux copiés: verification({verification_df.shape}), products_url({products_url_df.shape})")

            # --- Création IndexUnique NORMALISÉ (Bloc Corrigé) ---
            logger.info("Normalisation des clés et création des IndexUniques...")
            # Pour verification_df
            if not verification_df.empty:
                if all(col in verification_df.columns for col in ["MonNomProduit", "Concurrent"]):
                    try:
                        verification_df["_nom_norm"] = verification_df["MonNomProduit"].astype(str).str.strip().str.lower()
                        verification_df["_dom_norm"] = verification_df["Concurrent"].astype(str).str.strip().str.lower()
                        verification_df["IndexUnique"] = verification_df["_nom_norm"] + "__" + verification_df["_dom_norm"]
                        # Optionnel: supprimer colonnes temporaires
                        # verification_df = verification_df.drop(columns=["_nom_norm", "_dom_norm"])
                    except Exception as e: logger.error(f"Erreur normalisation/index verification_df: {e}")
                else: logger.warning("Colonnes manquantes pour IndexUnique dans verification_df")
            elif "IndexUnique" not in verification_df.columns: # Créer colonne vide si DF vide
                 verification_df["IndexUnique"] = pd.Series(dtype='str')

            # Pour products_url_df
            if not products_url_df.empty:
                 if all(col in products_url_df.columns for col in ["NomProduit", "CompetitorDomain"]):
                    try:
                        products_url_df["_nom_norm"] = products_url_df["NomProduit"].astype(str).str.strip().str.lower()
                        products_url_df["_dom_norm"] = products_url_df["CompetitorDomain"].astype(str).str.strip().str.lower()
                        products_url_df["IndexUnique"] = products_url_df["_nom_norm"] + "__" + products_url_df["_dom_norm"]
                        # Optionnel: supprimer colonnes temporaires
                        # products_url_df = products_url_df.drop(columns=["_nom_norm", "_dom_norm"])
                    except Exception as e: logger.error(f"Erreur normalisation/index products_url_df: {e}")
                 else: logger.warning("Colonnes manquantes pour IndexUnique dans products_url_df")
            elif "IndexUnique" not in products_url_df.columns: # Créer colonne vide si DF vide
                 products_url_df["IndexUnique"] = pd.Series(dtype='str')
            # --- FIN Création IndexUnique NORMALISÉ ---

            # Charger le fichier CSV d'entrée
            logger.info(f"Ouverture du fichier CSV : {input_csv}")
            products = pd.read_csv(input_csv, delimiter=";")
            logger.info(f"Fichier CSV chargé : {products.shape[0]} lignes.")

            # Valider colonnes CSV et prétraiter/stocker les prix
            if 'NomProduit' not in products.columns or 'MonPrix' not in products.columns:
                 msg = "Colonnes 'NomProduit' ou 'MonPrix' manquantes dans le fichier CSV! Arrêt."
                 logger.error(msg)
                 if root: messagebox.showerror("Erreur Fichier CSV", msg)
                 return
            products['_my_price_float'] = products['MonPrix'].apply(lambda x: float(str(x).replace(',', '.')) if pd.notna(x) else None)
            product_prices = products.set_index('NomProduit')['_my_price_float'].dropna().to_dict()

            total_products_csv = len(products)
            if total_products_csv == 0:
                 logger.warning("Le fichier CSV ne contient aucun produit à traiter.")
                 if root: messagebox.showinfo("Information", "Le fichier CSV est vide ou ne contient aucun produit valide.")
                 return

        except Exception as load_err:
            logger.critical(f"Erreur lors du chargement des données initiales: {load_err}", exc_info=True)
            if root: messagebox.showerror("Erreur Chargement", f"Impossible de charger les données initiales:\n{load_err}")
            raise

        # === Étape 2 : Calcul Progression et Démarrage Worker LPV ===
        total_tasks = total_products_csv * len(competitors)
        completed_final_tasks = 0
        logger.info(f"Nombre total de paires produit/concurrent à traiter: {total_tasks}")
        if progress_callback:
             try: progress_callback(0, total_tasks)
             except Exception as cb_err: logger.error(f"Erreur callback initial: {cb_err}")

        # Démarrer le worker LPV si nécessaire... (code inchangé)
        if "lepetitvapoteur.com" in competitors:
            logger.info("Démarrage du worker LPV persistant...")
            try:
                lpv_process = multiprocessing.Process(target=persistent_lpv_worker, args=(lpv_url_queue, lpv_result_queue), daemon=True, name="LPVWorker")
                lpv_process.start()
                time.sleep(0.5)
                if not lpv_process.is_alive():
                     logger.critical("Le processus worker LPV n'a pas pu démarrer correctement!")
                     lpv_process = None
                     if root: messagebox.showerror("Erreur LPV", "Le processus LPV n'a pas pu démarrer.")
                else: logger.info(f"Worker LPV démarré (PID: {lpv_process.pid}).")
            except Exception as start_err:
                 logger.critical(f"Impossible de démarrer le worker LPV: {start_err}", exc_info=True)
                 lpv_process = None
                 if root: messagebox.showerror("Erreur LPV", f"Impossible de démarrer le worker LPV:\n{start_err}")
        else: logger.info("LPV non sélectionné, le worker persistant n'est pas démarré.")

        # === Étape 3 & 4 : Soumission Tâches Initiales et Traitement Résultats / Envoi LPV ===
        MAX_WORKERS_THREADS = 10
        logger.info(f"Configuration ThreadPool: Threads={MAX_WORKERS_THREADS}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_THREADS, thread_name_prefix='WorkerThread') as thread_pool:
            initial_futures_map = {}

            logger.info(f"Soumission des tâches initiales pour {total_products_csv} produits...")
            for product_row in products.itertuples(index=False):
                my_product_name = getattr(product_row, 'NomProduit', None)
                my_price = product_prices.get(my_product_name)
                if not my_product_name or my_price is None: continue

                for domain in competitors:
                    future = thread_pool.submit(_worker_task, my_product_name, domain, verification_df, products_url_df)
                    initial_futures_map[future] = {'my_product_name': my_product_name, 'domain': domain, 'my_price': my_price}

            initial_tasks_count = len(initial_futures_map)
            logger.info(f"{initial_tasks_count} tâches initiales soumises. Traitement...")

            # Boucle de traitement des résultats initiaux
            for future in concurrent.futures.as_completed(initial_futures_map):
                task_info = initial_futures_map[future]
                task_my_product_name = task_info['my_product_name']
                task_domain = task_info['domain']
                task_my_price = task_info['my_price']
                processed_and_progress_updated = False

                try:
                    result = future.result()
                    logger.debug(f"Résultat initial reçu pour {task_my_product_name}/{task_domain}: Status={result.get('status')}")

                    if result.get('status') == 'Requires LPV':
                        url_for_lpv = result.get('url')
                        if url_for_lpv and lpv_process and lpv_process.is_alive():
                            logger.info(f"Envoi tâche LPV vers queue pour {task_info['my_product_name']} @ {url_for_lpv}")
                            lpv_url_queue.put({'url': url_for_lpv, 'my_product_name': task_info['my_product_name']})
                            lpv_tasks_submitted_count += 1
                            # Pas de mise à jour progression ici
                        else:
                            status_final_echec = 'No URL To Scrape' if not url_for_lpv else 'LPV Worker Not Ready'
                            logger.warning(f"Échec soumission LPV pour {task_my_product_name}. Statut: {status_final_echec}")
                            verif_changed, prod_url_changed = process_single_result(results, task_my_product_name, task_my_price, task_domain, url_for_lpv, status_final_echec, None, None, verification_df, products_url_df)
                            if verif_changed: verification_needs_global_update = True
                            if prod_url_changed: products_url_needs_global_update = True
                            processed_and_progress_updated = True
                            completed_final_tasks += 1
                    else:
                        verif_changed, prod_url_changed = process_single_result(results, task_my_product_name, task_my_price, task_domain, result.get('url'), result.get('status'), result.get('name'), result.get('price'), verification_df, products_url_df)
                        if verif_changed: verification_needs_global_update = True
                        if prod_url_changed: products_url_needs_global_update = True
                        processed_and_progress_updated = True
                        completed_final_tasks += 1

                except Exception as exc:
                    logger.error(f"Erreur traitement résultat initial pour {task_my_product_name}/{task_domain}: {exc}", exc_info=True)
                    # Traiter comme échec final pour la progression
                    processed_and_progress_updated = True
                    completed_final_tasks += 1
                    # Ne pas ajouter aux résultats affichés (géré par process_single_result)

                finally:
                    if processed_and_progress_updated and progress_callback:
                        try: progress_callback(completed_final_tasks, total_tasks)
                        except Exception as cb_err: logger.error(f"Erreur callback (initial): {cb_err}")

            logger.info("Toutes les tâches initiales traitées ou envoyées à LPV.")

        # === Étape 5 : Collecte des résultats LPV ===
        logger.info(f"Collecte des résultats pour {lpv_tasks_submitted_count} tâches LPV...")
        for i in range(lpv_tasks_submitted_count):
            increment_progress_lpv = False
            try:
                lpv_result = lpv_result_queue.get(timeout=180)
                res_my_product_name = lpv_result.get('my_product_name')
                res_domain = lpv_result.get('domain')
                res_my_price = product_prices.get(res_my_product_name)

                if res_my_product_name and res_domain and res_my_price is not None:
                     logger.info(f"Résultat LPV reçu pour {res_my_product_name}: Status={lpv_result.get('status')}")
                     verif_changed, prod_url_changed = process_single_result(results, res_my_product_name, res_my_price, res_domain, lpv_result.get('url'), lpv_result.get('status'), lpv_result.get('name'), lpv_result.get('price'), verification_df, products_url_df)
                     if verif_changed: verification_needs_global_update = True
                     if prod_url_changed: products_url_needs_global_update = True
                     increment_progress_lpv = True
                else:
                     logger.error(f"Résultat LPV invalide reçu: {lpv_result}")
                     increment_progress_lpv = True

            except queue.Empty:
                logger.error(f"Timeout attente résultat LPV! ({i+1}/{lpv_tasks_submitted_count})")
                increment_progress_lpv = True
            except Exception as exc:
                logger.error(f"Erreur collecte résultat LPV ({i+1}/{lpv_tasks_submitted_count}): {exc}", exc_info=True)
                increment_progress_lpv = True
            finally:
                if increment_progress_lpv:
                    completed_final_tasks += 1
                    if progress_callback:
                        try: progress_callback(completed_final_tasks, total_tasks)
                        except Exception as cb_err: logger.error(f"Erreur callback (LPV): {cb_err}")

        # === Étape 6 : Arrêter proprement le worker LPV ===
        if lpv_process and lpv_process.is_alive():
            logger.info("Envoi du signal de terminaison au worker LPV...")
            try:
                lpv_url_queue.put(None)
                lpv_process.join(timeout=15)
                if lpv_process.is_alive():
                     logger.warning("Le worker LPV n'a pas terminé après join(15). Forçage.")
                     lpv_process.terminate()
                     lpv_process.join(timeout=5)
                else: logger.info("Worker LPV terminé proprement.")
            except Exception as join_err:
                 logger.error(f"Erreur lors de l'arrêt du worker LPV: {join_err}", exc_info=True)
                 if lpv_process.is_alive():
                      try: lpv_process.terminate(); lpv_process.join(5)
                      except: pass

        logger.info("Traitement concurrent terminé.")

        # === Étape 7 : Mise à jour de GlobalStore si nécessaire ===
        if verification_needs_global_update:
             logger.info("Mise à jour de GlobalStore.verification_df avec les changements locaux.")
             GlobalStore.verification_df = verification_df
             GlobalStore.verification_changed = True
        if products_url_needs_global_update:
             logger.info("Mise à jour de GlobalStore.products_url_df avec les changements locaux.")
             GlobalStore.products_url_df = products_url_df
             GlobalStore.products_url_changed = True

        # === Étape 8 : Sauvegarde Google Sheets ===
        logger.info("\n--- Sauvegarde Finale Google Sheets ---")
        if GlobalStore.verification_changed or GlobalStore.products_url_changed:
            try: save_sheets_to_google()
            except Exception as save_err:
                 logger.error(f"!!! ERREUR Sauvegarde Google Sheets: {save_err} !!!", exc_info=True)
                 if root: messagebox.showwarning("Erreur Sauvegarde", f"Sauvegarde Google Sheets échouée:\n{save_err}")
        else: logger.info("Aucun changement dans GlobalStore marqué pour sauvegarde.")

        # === Étape 9 : Affichage Résultats ===
        logger.info("\n--- Affichage des résultats ---")
        display_results(root, results) # Assurez-vous que display_results est définie

    # === Gestion Erreur Fatale ===
    except Exception as e_fatal:
        logger.critical(f"!!! ERREUR FATALE dans process_products : {e_fatal} !!!", exc_info=True)
        if root: messagebox.showerror("Erreur Critique", f"Erreur fatale:\n{e_fatal}\nConsultez les logs.")
        if lpv_process and lpv_process.is_alive():
             logger.warning("Tentative d'arrêt forcé du worker LPV suite à une erreur fatale.")
             try: lpv_process.terminate(); lpv_process.join(5)
             except: pass
        raise

    # === Nettoyage Final et Durée ===
    finally:
        try: lpv_url_queue.close(); lpv_url_queue.join_thread()
        except: pass
        try: lpv_result_queue.close(); lpv_result_queue.join_thread()
        except: pass
        if lpv_process and lpv_process.is_alive():
            logger.warning("Worker LPV toujours actif dans finally, terminaison forcée.")
            try: lpv_process.terminate(); lpv_process.join(1)
            except: pass

        end_time_total = time.time()
        total_duration = end_time_total - start_time_total
        logger.info(f"=== Fin de process_products - Durée totale: {total_duration:.2f}s ===")



# --- process_single_result (Fonction qui traite UN résultat de worker/lpv) ---
def process_single_result(results_list, my_product_name, my_price, domain,
                          competitor_url, http_status, competitor_name, competitor_price,
                          verification_df, products_url_df):
    """
    Traite le résultat d'une tâche, met à jour les DFs LOCAUX si nécessaire,
    ajoute à results_list et retourne (bool, bool) indiquant si verification/products_url ont été modifiés localement.
    """
    verification_changed_local = False
    products_url_changed_local = False
    processed_ok = False

    status_str = str(http_status) # Pour comparaison

    # Succès si statut 200 et on a un nom et un prix
    if status_str == '200' and competitor_name and competitor_price is not None:
        processed_ok = True
        try:
            similarity = round(similarity_ratio(my_product_name.lower(), str(competitor_name).lower()), 2)
            price_difference = calculate_price_difference(my_price, competitor_price)
            est_moins_cher = False # Valeur par défaut : on suppose qu'il n'est pas moins cher
            if my_price is not None and competitor_price is not None:
                try:
                    # Comparer directement les prix pour la clarté
                    # est_moins_cher = True si le PRIX CONCURRENT est STRICTEMENT INFÉRIEUR à MON PRIX
                    if float(competitor_price) < float(my_price):
                        est_moins_cher = True
                    # else: est_moins_cher reste False (si prix égal ou concurrent plus cher)
                except (ValueError, TypeError):
                    # Si on ne peut pas comparer, on considère que le concurrent n'est pas moins cher
                    logger.warning(f"Impossible de comparer les prix pour {my_product_name}: mon_prix={my_price}, prix_concurrent={competitor_price}")
                    est_moins_cher = False
            # --- FIN CALCUL CORRIGÉ ---

            results_list.append({
                "MonNomProduit": my_product_name, "Concurrent": domain,
                "NomProduitConcurrent": str(competitor_name), "SimilaritéNom": similarity,
                "MonPrix": my_price, "PrixConcurrent": competitor_price,
                'EstMoinsCher': est_moins_cher, "DifférencePrix (%)": price_difference,
                "URLConcurrent": competitor_url,
            })
            logger.debug(f"Succès traité pour {my_product_name}/{domain}. Sim: {similarity}, Diff: {price_difference}%")

            # MAJ DFs locaux (les fonctions retournent True si changement effectif)
            # Passer les DFs en argument pour qu'elles opèrent sur la bonne copie
            if save_or_update_url(my_product_name, domain, competitor_url, products_url_df):
                 products_url_changed_local = True
            if remove_from_manual_verification(my_product_name, domain, verification_df):
                 verification_changed_local = True

        except Exception as proc_err:
             logger.error(f"Erreur traitement succès pour {my_product_name}/{domain}: {proc_err}", exc_info=True)
             processed_ok = False
             http_status = f"ProcessingError: {type(proc_err).__name__}" # Mettre à jour le statut pour log d'échec

    # Échec (si pas traité comme succès ou si erreur pendant traitement succès)
    if not processed_ok:
        # Ne pas ajouter à la vérif si c'était explicitement demandé ('Verification No URL')
        should_add_to_verification = (status_str != 'Verification No URL')

        logger.warning(f"Échec ou données incomplètes pour {my_product_name}/{domain} (Status: {http_status}). URL: {competitor_url or 'N/A'}")

        # Ajouter à la vérification manuelle locale si pertinent
        if should_add_to_verification:
             if add_to_manual_verification(my_product_name, domain, verification_df, competitor_url):
                 verification_changed_local = True

    return verification_changed_local, products_url_changed_local

# --- display_results (Affichage/Sauvegarde Résultats Finaux) ---
def display_results(root, results_data):
    """Affiche la fenêtre de résultats ou sauvegarde en cas d'erreur/absence de root."""
    if not results_data:
        logger.info("Aucun résultat à afficher.")
        if root and hasattr(root, 'winfo_exists') and root.winfo_exists():
             # Planifier l'affichage du message dans le thread Tkinter
             root.after(0, lambda: messagebox.showinfo("Information", "Traitement terminé, aucun résultat généré."))
        return

    try:
        results_df = pd.DataFrame(results_data)
        logger.info(f"Préparation affichage de {results_df.shape[0]} résultats.")
    except Exception as df_err:
        logger.error(f"Impossible de créer le DataFrame de résultats: {df_err}", exc_info=True)
        if root and hasattr(root, 'winfo_exists') and root.winfo_exists():
             root.after(0, lambda: messagebox.showerror("Erreur Résultats", f"Impossible de formater les résultats:\n{df_err}"))
        return # Ne pas continuer si le DF ne peut être créé

    # Fonction interne pour être appelée via root.after
    def _show_results_safely():
        nonlocal results_df # Utiliser le DF créé plus haut
        backup_path = os.path.join(os.path.expanduser("~"), "mindiyrest_results_fallback.csv") # Chemin plus explicite
        try:
            if root and hasattr(root, 'winfo_exists') and root.winfo_exists():
                logger.debug("Création Toplevel pour résultats depuis thread principal...")
                results_window = Toplevel(root)
                # Assurez-vous que ResultsViewer existe et prend (window, dataframe)
                ResultsViewer(results_window, results_df)
                logger.info("Fenêtre de résultats créée.")
            else:
                 logger.warning("root Tkinter non disponible, sauvegarde locale des résultats.")
                 results_df.to_csv(backup_path, index=False, sep=';', encoding='utf-8-sig')
                 logger.info(f"Résultats sauvegardés localement dans {backup_path}.")
        except Exception as viewer_err:
            logger.error(f"Erreur lors de l'affichage des résultats (ResultsViewer?): {viewer_err} !!!", exc_info=True)
            try: # Tentative de sauvegarde backup
                results_df.to_csv(backup_path, index=False, sep=';', encoding='utf-8-sig')
                logger.info(f"Résultats sauvegardés dans {backup_path} suite à erreur viewer.")
                if root and hasattr(root, 'winfo_exists') and root.winfo_exists():
                     root.after(0, lambda: messagebox.showerror("Erreur Affichage Résultats", f"Impossible d'afficher les résultats.\nIls ont été sauvegardés dans:\n{backup_path}\nErreur: {viewer_err}"))
            except Exception as backup_err:
                 logger.critical(f"ERREUR CRITIQUE sauvegarde backup résultats: {backup_err}", exc_info=True)
                 if root and hasattr(root, 'winfo_exists') and root.winfo_exists():
                      root.after(0, lambda: messagebox.showerror("Erreur Critique", "Impossible d'afficher ou sauvegarder les résultats."))

    # Planifier l'exécution dans le thread Tkinter
    if root:
        root.after(0, _show_results_safely)
    else:
        # Pas d'interface graphique (mode test?) -> Exécution directe (avec risque si Toplevel est utilisé) ou juste sauvegarde
        logger.warning("Pas de 'root' Tkinter fourni, tentative de sauvegarde locale directe.")
        backup_path = os.path.join(os.path.expanduser("~"), "mindiyrest_results_no_gui.csv")
        try:
             results_df.to_csv(backup_path, index=False, sep=';', encoding='utf-8-sig')
             logger.info(f"Résultats sauvegardés localement dans {backup_path}.")
        except Exception as save_err:
             logger.error(f"Erreur sauvegarde directe des résultats: {save_err}", exc_info=True)

# --- Protection __main__ ---
if __name__ == '__main__':
    # Important pour multiprocessing sous Windows et pour PyInstaller
    multiprocessing.freeze_support()
    # Optionnel: Définir la méthode de démarrage (peut aider à la stabilité)
    try:
        # 'spawn' est généralement plus sûr pour les applis GUI et avec certains imports
        if sys.platform in ['win32', 'darwin']: # Windows ou macOS
             if multiprocessing.get_start_method(allow_none=True) != 'spawn':
                   multiprocessing.set_start_method('spawn', force=True)
                   logger.info("Méthode de démarrage multiprocessing forcée à 'spawn'.")
    except Exception as mp_err:
        logger.warning(f"Impossible de forcer la méthode de démarrage multiprocessing: {mp_err}")

    logger.info("Ce module contient la logique de traitement principale refactorisée.")
    logger.info("Il n'est généralement pas exécuté directement, mais importé.")
    # Vous pourriez ajouter ici un petit scénario de test si nécessaire,
    # en créant une fausse racine Tkinter ou en passant root=None à process_products.
