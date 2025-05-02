__version__ = "0.9.7"

import tkinter as tk
import os
import sys
import logging
import multiprocessing
from filelock import FileLock, Timeout # Importation de filelock

# Importe ta classe d'application depuis l'autre fichier
try:
    from launch_graphique import ScraperApp
except ImportError:
    print("Erreur: Assurez-vous que le fichier 'launch_graphique.py' existe et contient la classe 'ScraperApp'.")
    sys.exit(1)

# Configurez les logs (comme avant)
logging.basicConfig(level=logging.DEBUG,
                    filename="app_debug.log",
                    filemode="w",
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Définit le chemin du fichier de verrouillage (peut être adapté)
# Utilise le répertoire personnel pour une meilleure compatibilité multiplateforme
LOCK_FILE_PATH = os.path.join(os.path.expanduser("~"), ".MinDIYrest.lock")
# Garde une référence globale au verrou pour être sûr qu'il existe dans 'finally'
app_lock = None

def acquire_lock():
    """
    Tente d'acquérir un verrou exclusif sur le fichier de verrouillage.
    Retourne l'objet FileLock si réussi, None sinon.
    """
    global app_lock
    logger.debug(f"Tentative d'acquisition du verrou : {LOCK_FILE_PATH}")
    app_lock = FileLock(LOCK_FILE_PATH, timeout=1) # timeout=1 pour filelock

    try:
        # Tente d'acquérir le verrou SANS attendre (timeout=0)
        # Si le verrou est déjà pris, une exception Timeout est levée
        app_lock.acquire(timeout=0)
        logger.debug("Verrouillage acquis avec succès.")
        return app_lock # Retourne l'objet verrou acquis
    except Timeout:
        logger.warning(f"Une autre instance semble déjà utiliser le fichier de verrouillage : {LOCK_FILE_PATH}")
        app_lock = None # Assure que la variable globale est None si échec
        return None
    except Exception as e:
        logger.error(f"Erreur inattendue lors de l'acquisition du verrou : {e}", exc_info=True)
        app_lock = None # Assure que la variable globale est None si échec
        return None

def release_lock(lock):
    """
    Libère le verrou de fichier s'il est détenu.
    """
    if lock and lock.is_locked:
        try:
            lock.release()
            logger.debug("Verrouillage libéré.")
            # Optionnel : Supprimer le fichier .lock. Généralement pas nécessaire avec filelock.
            # if os.path.exists(lock.lock_file):
            #     try:
            #         os.remove(lock.lock_file)
            #         logger.debug(f"Fichier de verrouillage supprimé : {lock.lock_file}")
            #     except OSError as e:
            #         logger.error(f"Impossible de supprimer le fichier de verrouillage : {e}")
        except Exception as e:
            logger.error(f"Erreur lors de la libération du verrou : {e}", exc_info=True)
    else:
        logger.debug("Aucun verrou à libérer ou verrou non détenu.")


def main():
    """Point d'entrée principal de l'application."""
    logger.info(f"Application démarrée avec PID : {os.getpid()}")

    # Tente d'acquérir le verrou dès le début
    lock = acquire_lock() # Utilise la nouvelle fonction

    if not lock:
        logger.warning("Impossible d'acquérir le verrou. Une autre instance est probablement en cours d'exécution. Fermeture.")
        # Optionnel: Afficher un message à l'utilisateur ici si besoin
        # import tkinter.messagebox
        # tkinter.messagebox.showwarning("Application déjà lancée", "Une autre instance de MinDIYrest est déjà en cours d'exécution.")
        sys.exit(1) # Sortir si le verrou n'est pas acquis

    # Utilisation d'un bloc try...finally pour garantir la libération du verrou
    try:
        root = tk.Tk()
        root.title("MinDIYrest Scraper") # Donner un titre à la fenêtre
        app = ScraperApp(root) # Crée l'instance de l'application graphique

        def on_close():
            """Fonction appelée lorsque l'utilisateur ferme la fenêtre."""
            logger.info("Demande de fermeture de la fenêtre reçue.")
            try:
                # 1. Demander à l'application de nettoyer ses propres ressources
                logger.debug("Appel de app.shutdown_resources()...")
                app.shutdown_resources() # Appel de la méthode de nettoyage de ScraperApp
                logger.debug("app.shutdown_resources() terminé.")
            except Exception as e:
                logger.error(f"Erreur lors de l'appel à app.shutdown_resources() : {e}", exc_info=True)
            
            # 2. Détruire la fenêtre Tkinter (ce qui terminera la mainloop)
            logger.debug("Appel de root.destroy()...")
            root.destroy()
            logger.info("Fenêtre détruite. L'application va se fermer.")

        # Associe la fonction on_close à l'événement de fermeture de la fenêtre
        root.protocol("WM_DELETE_WINDOW", on_close)

        # Démarre la boucle principale de Tkinter
        logger.debug("Démarrage de la mainloop Tkinter...")
        root.mainloop()
        logger.debug("Mainloop Tkinter terminée.")

    except Exception as e:
        logger.critical(f"Une erreur non gérée est survenue dans l'application principale : {e}", exc_info=True)
    finally:
        # Ce bloc est EXÉCUTÉ DANS TOUS LES CAS à la fin du try
        # (soit après la fin de mainloop, soit si une erreur survient dans le try)
        logger.debug("Bloc 'finally' atteint. Libération finale du verrou...")
        release_lock(lock) # Assure que le verrou est libéré
        logger.info("Application terminée.")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    # Configuration de multiprocessing (important pour la création d'exécutables)
    # 'spawn' est plus sûr sur macOS et Windows pour les applis GUI
    if sys.platform in ['darwin', 'win32']: # Vérifie si macOS ou Windows
       if multiprocessing.get_start_method(allow_none=True) != 'spawn':
            logger.debug("Définition de la méthode de démarrage multiprocessing sur 'spawn'.")
            multiprocessing.set_start_method('spawn', force=True)

    main()
