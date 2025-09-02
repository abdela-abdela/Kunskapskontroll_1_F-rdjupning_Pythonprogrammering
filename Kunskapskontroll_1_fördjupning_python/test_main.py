import sqlite3
import main

def test_load_csv():
    data = main.load_csv("tesla.csv")
    assert isinstance(data, list)
    assert len(data) > 0
    assert "date" in data[0]
    assert "close" in data[0]
    assert "volume" in data [0]
    assert "open" in data [0]
    assert "high" in data [0]
    assert "low" in data [0]
    
def test_update_db(tmp_path):
    test_db = tmp_path / "test_tesla.db"
    
# Vi Skapar vår tabell (tesla_stock) i databasen
    conn = sqlite3.connect(test_db)
    conn.execute("""
    CREATE TABLE tesla_stock(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        close REAL,
        volume INTEGER,
        open REAL,
        high REAL,
        low REAL)
    """)
    conn.commit()
    conn.close()

# Vi gör ett test exempel
    sample_data = [{
        "date": "2025-06-20",
        "close": 322.0,
        "volume": 108688000,
        "open": 328.0,
        "high": 332.0,
        "low": 318.0}]
    main.update_db(sample_data, db_path=test_db)

# Vi kan exempelvis kolla om olika funktioner i databasen fungerar genom att kontrollera att raden finns
    conn = sqlite3.connect(test_db)
    result = conn.execute("SELECT * FROM tesla_stock").fetchall()
    conn.close()
    assert len(result) == 1