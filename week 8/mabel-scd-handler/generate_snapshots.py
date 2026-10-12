"""
Generates three daily snapshots of source customer data as CSVs.

Day 1 (baseline): 5 customers.
Day 2: customer 42 moves city (Lagos -> Abuja), customer 43 changes email.
        No new customers yet.
Day 3: customer 44 moves city again (Abuja -> Kano is a distractor - actually
        we change customer 41's email), and a brand-new customer (46) is added.

Run:
    python3 generate_snapshots.py
Produces data/day1.csv, data/day2.csv, data/day3.csv
"""
import csv
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

FIELDS = ["customer_id", "name", "email", "city"]

day1 = [
    {"customer_id": 40, "name": "Chidi Okafor", "email": "chidi.okafor@example.com", "city": "Lagos"},
    {"customer_id": 41, "name": "Amina Bello", "email": "amina.bello@example.com", "city": "Kano"},
    {"customer_id": 42, "name": "Ada Lovelace", "email": "ada.lovelace@example.com", "city": "Lagos"},
    {"customer_id": 43, "name": "Tunde Bakare", "email": "tunde.bakare@example.com", "city": "Ibadan"},
    {"customer_id": 44, "name": "Ngozi Eze", "email": "ngozi.eze@example.com", "city": "Enugu"},
]

# Day 2: customer 42 moves city, customer 43 changes email. Everyone else unchanged.
day2 = [dict(row) for row in day1]
_by_id = {row["customer_id"]: row for row in day2}
_by_id[42]["city"] = "Abuja"
_by_id[43]["email"] = "tunde.bakare2@example.com"

# Day 3: customer 41 changes email, customer 44 moves city, and a new
# customer (46) shows up. Everyone else carries forward unchanged.
day3 = [dict(row) for row in day2]
_by_id3 = {row["customer_id"]: row for row in day3}
_by_id3[41]["email"] = "amina.bello.new@example.com"
_by_id3[44]["city"] = "Port Harcourt"
day3.append({"customer_id": 46, "name": "Femi Adigun", "email": "femi.adigun@example.com", "city": "Abuja"})


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    write_csv(os.path.join(DATA_DIR, "day1.csv"), day1)
    write_csv(os.path.join(DATA_DIR, "day2.csv"), day2)
    write_csv(os.path.join(DATA_DIR, "day3.csv"), day3)
    print("Wrote day1.csv, day2.csv, day3.csv to", DATA_DIR)
