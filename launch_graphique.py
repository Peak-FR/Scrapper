import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.ttk import Progressbar
from PIL import Image, ImageTk
import threading
import multiprocessing
import os
import sys
import logging # Ajout du logging
import time  # Ajout pour simulation potentielle ou petits délais

# Importe tes fonctions et classes spécifiques
try:
    from traitement_principal import process_products, load_sheets_into_memory, get_resource_path
    from global_store import GlobalStore
except ImportError:
    print("Erreur: Assurez-vous que les fichiers 'traitement_principal.py' et 'global_store.py' existent et sont accessibles.")
    # Dans un vrai scénario, logguer cette erreur serait mieux
    logging.error("Impossible d'importer depuis traitement_principal ou global_store.")
    # Optionnel : lever une exception ou quitter si ces imports sont vitaux dès le départ
    # sys.exit(1)


# Récupère le logger configuré dans main.py
# Ou configure un logger basique si ce fichier est exécuté seul pour tests
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# --- Tes constantes ---
# Utilisation de try-except pour la robustesse au cas où get_resource_path échouerait
COMPETITOR_LOGOS = {}
_LOGO_PATHS = {
    "lepetitvapoteur.com": "logos/lpv.png",
    "taklope.com": "logos/taklope.png",
    "kumulusvape.fr": "logos/kumulus.png",
    "cigaretteelec.fr": "logos/cigaretteelec.png",
}
for name, path in _LOGO_PATHS.items():
    try:
        COMPETITOR_LOGOS[name] = get_resource_path(path)
    except Exception as e:
        logger.error(f"Impossible de trouver le chemin pour le logo {name} via get_resource_path: {e}")
        # Optionnel: définir un chemin par défaut ou None


class ScraperApp:
    def __init__(self, root):
        self.root = root
        self.logger = logging.getLogger(__name__ + ".ScraperApp") # Logger spécifique
        self.logger.info("Initialisation de ScraperApp.")
        self.root.title("Scraper de Prix Concurrents")

        # Initialise la variable pour le thread avant son utilisation
        self.scraping_thread = None

        # --- Configuration de l'interface (votre code existant) ---
        # Cadre pour les concurrents (logos + cases à cocher)
        competitors_frame = tk.Frame(root)
        competitors_frame.pack(pady=10)

        self.selected_competitors = {}
        self.logo_images = {}  # Stocke les objets ImageTk

        for competitor, logo_path in COMPETITOR_LOGOS.items():
            frame = tk.Frame(competitors_frame)
            frame.pack(side="left", padx=10)

            if logo_path: # Vérifie si le chemin a été trouvé
                try:
                    image = Image.open(logo_path)
                    image = image.resize((50, 50), Image.Resampling.LANCZOS)
                    self.logo_images[competitor] = ImageTk.PhotoImage(image)
                    tk.Label(frame, image=self.logo_images[competitor]).pack()
                except Exception as e:
                    self.logger.error(f"Erreur lors du chargement ou redimensionnement du logo pour {competitor} depuis {logo_path}: {e}")
                    # Optionnel: afficher un placeholder ou juste le texte
                    tk.Label(frame, text="Logo Err").pack()
            else:
                 tk.Label(frame, text="Logo?").pack()


            var = tk.BooleanVar(value=True)
            self.selected_competitors[competitor] = var
            # Utilisation de ttk pour un look plus moderne si disponible
            try:
                from tkinter import ttk
                ttk.Checkbutton(frame, text=competitor, variable=var).pack()
            except ImportError:
                tk.Checkbutton(frame, text=competitor, variable=var).pack()


        # Sélection fichier CSV
        file_frame = tk.Frame(root)
        file_frame.pack(pady=10, fill='x', padx=10)
        tk.Label(file_frame, text="Fichier CSV produits :").pack(side="left")
        self.file_path = tk.StringVar(value="")
        # Utilisation de ttk pour un look plus moderne
        try:
            from tkinter import ttk
            ttk.Entry(file_frame, textvariable=self.file_path, width=50).pack(side="left", expand=True, fill='x', padx=5)
            ttk.Button(file_frame, text="Parcourir", command=self.browse_file).pack(side="left")
        except ImportError:
            tk.Entry(file_frame, textvariable=self.file_path, width=50).pack(side="left", expand=True, fill='x', padx=5)
            tk.Button(file_frame, text="Parcourir", command=self.browse_file).pack(side="left")


        # Bouton GO
        # Utilisation de ttk pour un look plus moderne
        try:
            from tkinter import ttk
            self.go_button = ttk.Button(root, text="GO", command=self.start_scraping)
        except ImportError:
            self.go_button = tk.Button(root, text="GO", command=self.start_scraping)
        self.go_button.pack(pady=10)


        # Barre de progression
        self.progress = Progressbar(root, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(pady=5)
        # Utilisation de ttk pour un look plus moderne
        try:
            from tkinter import ttk
            self.progress_label = ttk.Label(root, text="")
        except ImportError:
            self.progress_label = tk.Label(root, text="")
        self.progress_label.pack(pady=5)

        # --- Fin de la configuration de l'interface ---

        # Les lignes suivantes sont SUPPRIMÉES car la fermeture est gérée par main.py:
        # self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        # self.root.bind("<Destroy>", self.on_destroy)
        self.logger.info("Interface ScraperApp configurée.")


    def browse_file(self):
        """Ouvre une boîte de dialogue pour sélectionner un fichier CSV."""
        self.logger.debug("Ouverture de la boîte de dialogue Parcourir.")
        file_path = filedialog.askopenfilename(
            title="Sélectionner un fichier CSV",
            filetypes=[("Fichiers CSV", "*.csv"), ("Tous les fichiers", "*.*")]
        )
        if file_path:
            self.file_path.set(file_path)
            self.logger.info(f"Fichier sélectionné : {file_path}")
        else:
            self.logger.debug("Aucun fichier sélectionné.")


    def start_scraping(self):
        """Valide les choix et lance le traitement dans un thread séparé."""
        self.logger.info("Tentative de démarrage du scraping.")
        selected_domains = [domain for domain, var in self.selected_competitors.items() if var.get()]
        input_file = self.file_path.get()

        # --- Validations ---
        if not input_file:
            self.logger.warning("Validation échouée : Aucun fichier CSV sélectionné.")
            messagebox.showerror("Erreur", "Veuillez sélectionner un fichier CSV.")
            return
        if not os.path.isfile(input_file):
            self.logger.error(f"Validation échouée : Fichier invalide ou introuvable : {input_file}")
            messagebox.showerror("Erreur", "Le fichier sélectionné est invalide ou introuvable.")
            return
        if not selected_domains:
            self.logger.warning("Validation échouée : Aucun concurrent sélectionné.")
            messagebox.showerror("Erreur", "Veuillez sélectionner au moins un concurrent.")
            return

        # --- Démarrage du thread ---
        self.logger.info(f"Validation réussie. Démarrage du thread de scraping pour le fichier {input_file} avec les concurrents : {selected_domains}")
        self.go_button.config(state="disabled") # Désactiver le bouton
        self.progress["value"] = 0
        self.progress_label.config(text="Démarrage...")
        # S'assurer que l'UI est mise à jour avant de lancer le long process
        self.root.update_idletasks()

        # Lancer le traitement dans un thread séparé
        # Vérifier si un thread tourne déjà (sécurité additionnelle)
        if self.scraping_thread and self.scraping_thread.is_alive():
             self.logger.warning("Tentative de démarrer un nouveau scraping alors qu'un est déjà en cours.")
             messagebox.showwarning("Attention", "Un processus de scraping est déjà en cours.")
             # Ne pas réactiver le bouton ici, car le processus précédent n'est pas fini
             return
        else:
            self.scraping_thread = threading.Thread(
                target=self.run_scraping,
                args=(selected_domains, input_file),
                daemon=True # Important pour que le thread ne bloque pas la fermeture si l'app quitte brutalement
            )
            self.scraping_thread.start()


    def run_scraping(self, selected_domains, input_file):
        """Exécute le traitement principal dans le thread séparé."""
        self.logger.info(f"=== Début de run_scraping (Thread: {threading.current_thread().name}) ===")
        start_time = time.time()
        success = False
        try:
            # Pré-chargement (si nécessaire et synchrone)
            self.logger.debug("Chargement des feuilles en mémoire...")
            self.root.after(0, lambda: self.progress_label.config(text="Chargement initial..."))
            load_sheets_into_memory() # Supposé synchrone
            self.logger.debug("Chargement initial terminé.")

            # Traitement principal
            self.logger.debug("Démarrage de process_products...")
            process_products(
                root=self.root, # Transmet la racine pour `root.after` dans process_products si besoin
                competitors=selected_domains,
                input_csv=input_file,
                progress_callback=self.update_progress, # Transmet la méthode de callback
            )
            self.logger.info("process_products terminé avec succès (supposé).")
            success = True

        except Exception as e:
            self.logger.error(f"Erreur majeure dans run_scraping : {e}", exc_info=True) # Log l'erreur complète
            # Afficher l'erreur dans l'UI via root.after
            self.root.after(0, lambda error=e: messagebox.showerror("Erreur", f"Une erreur critique s'est produite durant le scraping:\n{error}"))
        finally:
            # Ce qui se passe TOUJOURS à la fin de ce thread
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"=== Fin de run_scraping (Thread: {threading.current_thread().name}) - Durée: {duration:.2f}s ===")

            # Afficher le message de succès/échec et réactiver le bouton
            if success:
                 self.root.after(0, lambda: messagebox.showinfo("Terminé", f"Scraping terminé avec succès en {duration:.2f} secondes."))
            # else: Le message d'erreur a déjà été affiché dans le 'except'

            # Réactiver le bouton GO via root.after pour la sécurité des threads
            self.root.after(0, lambda: self.go_button.config(state="normal"))
            self.root.after(0, lambda: self.progress_label.config(text=f"Terminé. Durée: {duration:.1f}s" if success else "Terminé avec erreurs."))

            # IMPORTANT : Le nettoyage des processus enfants est déplacé dans shutdown_resources


    def update_progress(self, current, total):
        """Met à jour la barre de progression et le label (appelé depuis le thread)."""
        if total <= 0: # Éviter la division par zéro
            return
        percentage = (current / total) * 100
        # Utiliser root.after pour garantir que les mises à jour de l'UI
        # sont exécutées dans le thread principal de Tkinter
        def _update_ui():
            try:
                if self.root.winfo_exists(): # Vérifier si la fenêtre existe toujours
                    self.progress["value"] = percentage
                    self.progress_label.config(text=f"Progression : {current}/{total} ({percentage:.1f}%)")
                    # self.root.update_idletasks() # Souvent pas nécessaire avec root.after
            except tk.TclError as e:
                # Peut arriver si la fenêtre est détruite pendant la mise à jour
                self.logger.warning(f"Erreur Tcl lors de la mise à jour de la progression (fenêtre fermée?): {e}")

        self.root.after(0, _update_ui)


    # --------------------------------------------------------------------------
    # MÉTHODE DE NETTOYAGE CENTRALE - APPELÉE PAR main.py LORS DE LA FERMETURE
    # --------------------------------------------------------------------------
    def shutdown_resources(self):
        """
        Nettoie les ressources utilisées par ScraperApp avant la fermeture.
        Appelée par la fonction on_close de main.py.
        """
        self.logger.info("Appel de shutdown_resources pour nettoyer ScraperApp...")

        # 1. Nettoyage des processus enfants de multiprocessing
        # Logique déplacée depuis run_scraping.finally
        active_children = multiprocessing.active_children()
        if active_children:
            self.logger.info(f"Tentative de terminaison de {len(active_children)} processus enfants...")
            for child in active_children:
                try:
                    self.logger.debug(f"Terminaison du processus enfant PID {child.pid}...")
                    child.terminate() # Envoie SIGTERM
                    # Attendre un court instant que le processus se termine
                    child.join(timeout=1.0) # Attendre max 1 seconde
                    if child.is_alive():
                        self.logger.warning(f"Le processus enfant PID {child.pid} n'a pas terminé après terminate/join. Il sera peut-être orphelin ou tué par l'OS.")
                        # Optionnel: Forcer avec child.kill() si SIGTERM échoue (SIGKILL)
                        # child.kill()
                        # child.join(timeout=0.5)
                    else:
                         self.logger.debug(f"Processus enfant PID {child.pid} terminé.")
                except Exception as e:
                    self.logger.error(f"Erreur lors de la terminaison/join du processus enfant PID {child.pid}: {e}", exc_info=True)
            self.logger.info("Nettoyage des processus enfants terminé.")
        else:
            self.logger.info("Aucun processus enfant actif à nettoyer.")


        # 2. Gestion du thread de scraping principal
        if self.scraping_thread and self.scraping_thread.is_alive():
            self.logger.warning(f"Le thread de scraping (Thread: {self.scraping_thread.name}) est toujours en cours d'exécution lors de la fermeture.")
            # NOTE: Puisque le thread est 'daemon', il sera automatiquement terminé par Python
            # lorsque le thread principal se termine. Tenter de l'arrêter ici nécessiterait
            # une logique de coopération dans `run_scraping` / `process_products`
            # (par exemple, vérifier un flag self._stop_requested régulièrement).
            # Pour l'instant, on se contente de le logger.
            # Optionnel (si vous implémentez un flag d'arrêt):
            # self._stop_requested = True # (Il faudrait créer ce flag)
            # self.scraping_thread.join(timeout=5) # Attendre que le thread finisse
            # if self.scraping_thread.is_alive():
            #     self.logger.warning("Le thread de scraping n'a pas pu être arrêté proprement.")
        else:
            self.logger.info("Le thread de scraping n'était pas actif lors de la fermeture.")

        # 3. Autres nettoyages spécifiques ?
        # Par exemple, fermer explicitement des fichiers si GlobalStore en garde ouverts, etc.
        # try:
        #     if GlobalStore.some_resource:
        #         GlobalStore.some_resource.close()
        # except Exception as e:
        #      self.logger.error(f"Erreur lors de la fermeture des ressources GlobalStore: {e}")

        self.logger.info("Nettoyage des ressources de ScraperApp terminé.")


# --- Section pour tester launch_graphique.py indépendamment ---
if __name__ == "__main__":
    print("Lancement de launch_graphique.py en mode test autonome...")
    try:
        test_root = tk.Tk()
        test_root.title("Test ScraperApp Autonome")
        app_instance = ScraperApp(test_root)

        # Définir une fonction de fermeture pour le test autonome qui simule main.py
        def test_on_close():
            print("--- Fermeture de la fenêtre de test autonome ---")
            app_instance.shutdown_resources() # Tester la fermeture des ressources
            test_root.destroy()
            print("--- Fenêtre de test détruite ---")
            # En mode test, on peut ajouter un exit() pour terminer le script
            # sys.exit(0)

        test_root.protocol("WM_DELETE_WINDOW", test_on_close)
        test_root.mainloop()
        print("--- Mainloop de test terminée ---")
    except Exception as e:
        logging.exception("Erreur fatale lors du test autonome de ScraperApp.")
        print(f"Erreur fatale lors du test: {e}")
