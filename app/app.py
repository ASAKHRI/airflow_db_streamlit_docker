import streamlit as st
import pandas as pd
import sqlite3

db_path = "../db/data.db"



def get_table_names():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        return []

def query_sql(query):
    try:
        conn=sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except sqlite3.Error as e:
        return None, f"Erreur lors de la requête SQL: {e}"
    
    
    
def main():
    st.title("application de requête SQL")
    
        # Afficher la liste des tables
    tables = get_table_names()
    if tables:
        st.subheader("📦 Tables disponibles :")
        st.markdown(", ".join([f"`{table}`" for table in tables]))
    else:
        st.warning("Aucune table trouvée dans la base de données.")
    
    query = st.text_area("Entrez votre requête SQL", height=200)
    
    if st.button("Exécuter la requête"):
        if query.strip() == "":
            st.warning("Veuillez entrer une requête SQL.")
            return
        
        df,error = query_sql(query)
        if error:
            st.error(error)
        else:
            st.write("Résultats de la requête:")
            st.dataframe(df)
            
            
if __name__ == "__main__":
    main()
            
            
            