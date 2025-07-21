import glob
import pandas as pd
import sqlite3
import os

def nettoyage_colonnes(df):
    cols = ["athlete_nom", "athlete_prenom", "equipe_en", "federation", "epreuve", "annee", "mois", "jour"]
    df = df[cols]
    df = df.rename(columns={"athlete_nom": "nom", "athlete_prenom": "prenom", "equipe_en": "equipe"})
    df["nom"] = df["nom"].str.upper()
    df["prenom"] = df["prenom"].str.upper()
    df["equipe"] = df["equipe"].str.upper()
    
    return df

def fillna(df):
    df = df.fillna({"nom": "NOM INCONNU", "prenom": "PRENOM INCONNU", "equipe": "EQUIPE INCONNUE"})
    return df


def load_to_sqlite(df,db_path="../db/data.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql("epreuve_sportives", conn, if_exists="replace", index=False)
    conn.close()
    print("les données sont insérées dans la table epreuve_sportives")

def main():
    files = glob.glob("../data/*.csv")
    if not files:
        raise FileNotFoundError ("pas de fichiers csv trouvés dans le dossier data")
        
    input_path = files[0]
    df = pd.read_csv(input_path)
    df = nettoyage_colonnes(df)
    df = fillna(df)
    load_to_sqlite(df)


if __name__ == "__main__":
    main()