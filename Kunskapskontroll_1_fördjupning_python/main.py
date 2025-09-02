import sqlite3
import csv
import logging

logging.basicConfig( 
    filename = "etl.log", 
    level = logging.INFO, 
    format = "%(asctime)s - %(levelname)s - %(message)s"
    )

def load_csv(file_path):
    data = []
    try:
        with open(file_path, encoding = "utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "date": row["Date"],
                    "close": float(row["Close/Last"]),
                    "volume": int(row["Volume"]),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                })

        logging.info(f"Läste {len(data)} rader från {file_path}")
    except FileNotFoundError:
        logging.error(f"CSV-filen hittades inte: {file_path}")
    except Exception as e:
        logging.error(f"Kunde inte läsa CSV: {e}")
    return data

def update_db(data, db_path = "tesla.db"):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for row in data:
            try:
                cursor.execute("""
                    iNSERT INTO tesla_stock (date, close, volume, open, high, low)
                     VALUES (?, ?, ?, ?, ?, ?)
                """, (row["date"], row["close"], row["volume"],
                      row["open"], row["high"], row["low"]))
            except sqlite3.IntegrityError:
                logging.warning(f"Datum {row["date"]} finns redan - hoppar över denna rad och fortsätter med nästa.")
        conn.commit()
        conn.close()
        logging.info("Databasen har uppdaterats med nya rader.")
    except Exception as e:
        logging.error(f"Fel upptäcktes vid databasuppdatering: {e}")

if __name__ == "__main__":
    file_path = "tesla.csv"
    data = load_csv(file_path)
    if data:
        update_db(data)


