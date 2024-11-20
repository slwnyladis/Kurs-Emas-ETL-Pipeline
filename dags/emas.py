from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gold_price_and_currency_rate_etl',
    default_args=default_args,
    description='ETL Pipeline to get Gold Prices and Currency Rates',
    schedule_interval='@daily',  # Schedule every day
    catchup=False,
)

# Task 1: Ambil data kurs
@task(dag=dag)
def extract_currency_rate():
    # URL dari halaman web JISDOR
    url = "https://www.bi.go.id/id/statistik/informasi-kurs/jisdor/default.aspx"

    # Mengambil halaman web
    response = requests.get(url)
    response.raise_for_status()  # Cek apakah permintaan berhasil
    soup = BeautifulSoup(response.text, 'html.parser')

    # Menemukan tabel data kurs JISDOR
    table = soup.find('div', {'id': 'tableData'})
    rows = table.find_all('tr')

    # Menyimpan data ke list
    data = []
    for row in rows[1:2]:  # Mengambil 1 data teratas
        columns = row.find_all('td')
        tanggal = columns[0].text.strip()
        kurs = columns[1].text.strip()
        data.append([tanggal, kurs])

    # Membuat DataFrame untuk kurs mata uang
    df_kurs = pd.DataFrame(data, columns=['Tanggal', 'Kurs'])

    
    return df_kurs  # Kembalikan DataFrame kurs

# Fungsi untuk mengekstrak harga emas
@task(dag=dag)
def extract_gold_price():
    url = "https://logam-mulia-api.vercel.app/prices/indogold"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Filter data untuk 'antam'
        antam_data = next((item for item in data['data'] if item['type'] == 'antam'), None)
        if antam_data:
            sell_price = antam_data['sell']
            date_now = datetime.now().strftime("%Y-%m-%d")

            # Membuat DataFrame untuk harga emas
            df_gold = pd.DataFrame([{
                'tanggal': date_now,
                'harga_jual_emas': sell_price
            }])
            df_gold['tanggal'] = pd.to_datetime(df_gold['tanggal'], format='%Y-%m-%d')  # Format tanggal
            return df_gold
        else:
            raise ValueError("Data untuk tipe 'antam' tidak ditemukan.")
    else:
        raise Exception("Gagal mengambil data dari API. Status code: ", response.status_c)

# Fungsi untuk menggabungkan dan transformasi data kurs dan harga emas
@task(dag=dag)
def transform_data(df_kurs, df_gold):

    print("Tipe data awal df_kurs:")
    print(df_kurs.dtypes)
    print("Tipe data awal df_gold:")
    print(df_gold.dtypes)

    print("Data sebelum transformasi df_kurs:")
    print(df_kurs.head())  # Memeriksa data df_kurs
    print("Data sebelum transformasi df_gold:")
    print(df_gold.head())  # Memeriksa data df_gold

    # Pastikan kolom 'Tanggal' dan 'tanggal' bertipe string sebelum menggunakan .str.strip() jika perlu
    if df_kurs['Tanggal'].dtype == 'object':  # Cek apakah kolom bertipe string
        df_kurs['Tanggal'] = df_kurs['Tanggal'].str.strip()  # Menghapus spasi tambahan
    if df_gold['tanggal'].dtype == 'object':  # Cek apakah kolom bertipe string
        df_gold['tanggal'] = df_gold['tanggal'].str.strip()  # Menghapus spasi tambahan

    # Mengubah kolom Tanggal ke format datetime dari 'day month year' (e.g., '19 November 2024')
    df_kurs['Tanggal'] = pd.to_datetime(df_kurs['Tanggal'], format='%d %B %Y', errors='coerce')
    df_gold['tanggal'] = pd.to_datetime(df_gold['tanggal'], format='%Y-%m-%d', errors='coerce')

    # Pastikan kolom tanggal hanya berisi tanggal tanpa waktu
    df_kurs['Tanggal'] = df_kurs['Tanggal'].dt.date
    df_gold['tanggal'] = df_gold['tanggal'].dt.date

    # Drop any rows with NaT or NaN in tanggal columns
    df_kurs = df_kurs.dropna(subset=['Tanggal'])
    df_gold = df_gold.dropna(subset=['tanggal'])

    print("Data setelah pembersihan df_kurs:")
    print(df_kurs.head())  # Memeriksa data df_kurs setelah pembersihan
    print("Data setelah pembersihan df_gold:")
    print(df_gold.head())  # Memeriksa data df_gold setelah pembersihan

    # Mengubah kolom Kurs menjadi integer
    df_kurs['Kurs'] = df_kurs['Kurs'].str.replace('Rp', '')  # Hapus 'Rp'
    df_kurs['Kurs'] = df_kurs['Kurs'].str.replace('.', '')  # Hapus titik pemisah ribuan
    df_kurs['Kurs'] = df_kurs['Kurs'].str.replace(',', '.')  # Ganti koma dengan titik untuk desimal
    df_kurs['Kurs'] = df_kurs['Kurs'].astype(float).astype(int)  # Konversi ke integer

    print("Data df_kurs sebelum merge:")
    print(df_kurs.head())  # Memeriksa df_kurs sebelum merge
    print("Data df_gold sebelum merge:")
    print(df_gold.head())  # Memeriksa df_gold sebelum merge

     # Menentukan panjang data terpendek antara df_kurs dan df_gold
    min_len = min(len(df_kurs), len(df_gold))

    # Membuat df_transform dengan mengambil data dari kedua DataFrame
    df_transform = pd.DataFrame({
        'Tanggal': df_kurs['Tanggal'][:min_len],
        'Kurs': df_kurs['Kurs'][:min_len],
        'Harga_Emas': df_gold['harga_jual_emas'][:min_len]
    })

    # Pastikan kolom 'Tanggal' dikonversi ke datetime.date
    df_transform['Tanggal'] = pd.to_datetime(df_transform['Tanggal']).dt.date

    print("Tipe data setelah pembuatan df_transform dan konversi Tanggal:")
    print(df_transform.dtypes)

    print("Data setelah pembuatan df_transform:")
    print(df_transform.head())

    return df_transform


# Fungsi untuk menyimpan data ke PostgreSQL
@task(dag=dag)
def load_to_postgres(df_combined):
    # Setup koneksi PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Membuat tabel jika belum ada
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Kurs_Emas (
            tanggal DATE PRIMARY KEY,
            kurs INT NOT NULL,
            harga_emas INT NOT NULL
        );
    """)
    conn.commit()

    # Menyimpan DataFrame ke dalam tabel PostgreSQL
    for _, row in df_combined.iterrows():
        cursor.execute(
            """
            INSERT INTO Kurs_Emas (tanggal, kurs, harga_emas)
            VALUES (%s, %s, %s)
            ON CONFLICT (tanggal) DO UPDATE 
            SET kurs = EXCLUDED.kurs, harga_emas = EXCLUDED.harga_emas;
            """,
            (row['Tanggal'], row['Kurs'], row['Harga_Emas'])
        )
    conn.commit()
    cursor.close()
    conn.close()


# Eksekusi DAG
df_kurs = extract_currency_rate()
df_gold = extract_gold_price()
df_transform = transform_data(df_kurs, df_gold)
load_to_postgres(df_transform)

