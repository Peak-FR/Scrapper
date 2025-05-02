import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import webbrowser


class ResultsViewer:
    def __init__(self, root, dataframe):
        self.root = root
        self.root.title("Résultats du Scraping")
        self.dataframe = dataframe
        self.filtered_dataframe = dataframe.copy()

        # Configuration des styles pour les lignes colorées
        self.style = ttk.Style(self.root)
        self.style.configure("Treeview", rowheight=25)  # Hauteur des lignes
        self.style.map("Treeview", background=[("selected", "#D9E8FB")])  # Couleur de sélection

        # Ajout des styles pour les tags
        self.style.configure("Green.TTag", background="#CCFFCC")  # Vert clair
        self.style.configure("Red.TTag", background="#FFCCCC")    # Rouge clair

        # Création du champ de recherche et du tableau
        self.create_search_bar()
        self.create_table()
        self.create_export_button()

    def create_search_bar(self):
        """Crée un champ de recherche pour filtrer les résultats."""
        search_frame = tk.Frame(self.root)
        search_frame.pack(fill="x", pady=5)

        tk.Label(search_frame, text="Filtrer par colonne :").pack(side="left", padx=5)
        self.column_selector = ttk.Combobox(search_frame, values=self.dataframe.columns.tolist(), state="readonly")
        self.column_selector.pack(side="left", padx=5)
        self.column_selector.set(self.dataframe.columns[0])  # Colonne par défaut

        tk.Label(search_frame, text="Valeur :").pack(side="left", padx=5)
        self.search_value = tk.Entry(search_frame)
        self.search_value.pack(side="left", padx=5)

        search_button = tk.Button(search_frame, text="Rechercher", command=self.filter_table)
        search_button.pack(side="left", padx=5)

        reset_button = tk.Button(search_frame, text="Réinitialiser", command=self.reset_table)
        reset_button.pack(side="left", padx=5)

    def create_table(self):
        """Crée un tableau interactif pour afficher les résultats."""
        table_frame = tk.Frame(self.root)
        table_frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(table_frame, columns=self.dataframe.columns.tolist(), show="headings")
        self.tree.pack(fill="both", expand=True, side="left")

        # Ajout des en-têtes de colonnes
        for col in self.dataframe.columns:
            self.tree.heading(col, text=col, command=lambda _col=col: self.sort_column(_col, False))
            self.tree.column(col, width=150, anchor="center")

        # Ajout des données
        self.update_table(self.dataframe)

        # Barre de défilement
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

        # Associer un événement pour ouvrir les liens
        self.tree.bind("<Double-1>", self.open_link)

    def create_export_button(self):
        """Ajoute un bouton pour exporter les résultats filtrés."""
        export_button = tk.Button(self.root, text="Exporter au format CSV", command=self.export_to_csv)
        export_button.pack(pady=10)

    def update_table(self, dataframe):
        """Met à jour le contenu du tableau avec des couleurs conditionnelles et texte lisible."""
        # Supprime les données existantes
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Ajoute les nouvelles données avec des tags pour les couleurs
        for index, row in dataframe.iterrows():
            try:
                # Récupérer la valeur directement depuis la colonne "DifférencePrix (%)"
                diff_prix_str = str(row["DifférencePrix (%)"])  # Convertir en chaîne pour manipulations
                diff_prix = float(diff_prix_str.replace(",", "."))  # Remplace les virgules par des points et convertit en float
            except (ValueError, KeyError):
                diff_prix = 0  # En cas de valeur invalide, on considère 0

            # Appliquer le tag en fonction de la valeur
            tag = "green" if diff_prix <= 0 else "red"
            self.tree.insert("", "end", values=row.tolist(), tags=(tag,))

        # Applique les couleurs et rend le texte noir pour une meilleure lisibilité
        self.tree.tag_configure("green", background="#CCFFCC", foreground="#000000")  # Vert clair avec texte noir
        self.tree.tag_configure("red", background="#FFCCCC", foreground="#000000")    # Rouge clair avec texte noir

    def sort_column(self, column, reverse):
        """Trie les données selon une colonne."""
        self.filtered_dataframe = self.filtered_dataframe.sort_values(by=column, ascending=not reverse)
        self.update_table(self.filtered_dataframe)
        # Inverse le sens de tri pour le prochain clic
        self.tree.heading(column, command=lambda: self.sort_column(column, not reverse))

    def filter_table(self):
        """Filtre les données selon la colonne et la valeur spécifiées."""
        column = self.column_selector.get()
        value = self.search_value.get().strip()

        if not column or not value:
            messagebox.showerror("Erreur", "Veuillez sélectionner une colonne et entrer une valeur pour filtrer.")
            return

        self.filtered_dataframe = self.dataframe[self.dataframe[column].astype(str).str.contains(value, case=False, na=False)]
        self.update_table(self.filtered_dataframe)

    def reset_table(self):
        """Réinitialise les données du tableau."""
        self.filtered_dataframe = self.dataframe.copy()
        self.update_table(self.filtered_dataframe)

    def export_to_csv(self):
        """Permet d'exporter les résultats affichés dans un fichier CSV."""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("Fichiers CSV", "*.csv")],
            title="Exporter les résultats en CSV",
        )
        if file_path:
            # Exporter le DataFrame avec la colonne "Lien"
            self.dataframe.to_csv(file_path, index=False, sep=";")
            tk.messagebox.showinfo("Exportation réussie", f"Les résultats ont été exportés avec succès vers {file_path}.")

    def open_link(self, event):
        """Ouvre le lien dans le navigateur par défaut lorsqu'on double-clique sur une cellule contenant une URL."""
        selected_item = self.tree.selection()
        if not selected_item:
            return

        # Récupérer la ligne sélectionnée
        item_data = self.tree.item(selected_item)["values"]
        if len(item_data) > 0:
            try:
                # Supposons que la colonne du lien est la dernière
                url = item_data[-1]
                if url and url != "N/A":
                    webbrowser.open(url)
                else:
                    messagebox.showerror("Erreur", "Lien non valide ou absent.")
            except Exception as e:
                messagebox.showerror("Erreur", f"Impossible d'ouvrir le lien : {e}")
