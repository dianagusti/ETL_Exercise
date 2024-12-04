import bs4
import requests
import pandas as pd
import sqlalchemy as sa


#===========extract==========
def extract_news(url):
    #mengambil data konten halaman web
    response = requests.get(url)
    response.raise_for_status() #memeriksa jika permintaan berhasil

    #menginisialisasi BeautifulSoup untuk parsing HTML
    soup = bs4.BeautifulSoup(response.content, "html.parser")

     # Mengambil semua teks dari tag <h2> yang biasanya berisi judul berita
    data = [h2.get_text() for h2 in soup.find_all("h2")]

    #membuat DataFrame dari data yang diekstraksi
    df = pd.DataFrame(data, columns=["title"])
    

    return df

#===========load==========
def load_sqlite(df, table_name, engine):
    #Load DaraFrame ke table SQLite, menggantikan tabel jika sudah ada
    with engine.begin() as conn:
        df.to_sql(table_name, conn, index=False, if_exists="replace")


#===========tranform==========
def transform_uppercase(raw_table_name, table_name, engine):
   # Membuat tabel baru dengan judul berita dalam huruf besar
   with engine.begin() as conn:
       conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
       conn.execute(sa.text(f"""
           CREATE TABLE {table_name} AS
           SELECT UPPER(title) as title
           FROM {raw_table_name}
       """))

        
#Konfigurasi
url = "https://www.bbc.com/news"
table_name = "elt_name"
table_name_raw = "elt_news_raw"
engine = sa.create_engine(f"sqlite:///dibimbing.sqlite")

#Extract dara berita
df_news = extract_news(url)
print("Extract Berhasil")

#Load data berita ke SQLite
load_sqlite(df_news, table_name_raw, engine)
print("Load Berhasil")

#Transformasi data berita menjadi huruf besar dan simpan ke table baru
transform_uppercase(table_name_raw, table_name, engine)
print("Transform Berhasil")
