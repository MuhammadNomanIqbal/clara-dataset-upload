import csv
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from bson import ObjectId
from pymongo import MongoClient


# ---------- Load .env ----------
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)

# ---------- CONFIG ----------
INPUT_CSV = Path("/home/asim/Desktop/clara-dataset-upload/logs/profile_upload_success.csv")
OUTPUT_CSV = Path("/home/asim/Desktop/clara-dataset-upload/logs/profile_results.csv")

MONGO_URI = (os.getenv("MONGO_URI") or "").strip()
MONGO_DB = (os.getenv("MONGO_DB") or "").strip()
MONGO_COLLECTION = (os.getenv("MONGO_COLLECTION") or "").strip()
# --------------------------------

HEX24 = re.compile(r"[a-fA-F0-9]{24}")


def to_oid(value: str):
    if not value:
        return None
    m = HEX24.search(str(value).strip())
    if not m:
        return None
    try:
        return ObjectId(m.group(0))
    except Exception:
        return None


def main():
    if not MONGO_URI or not MONGO_DB or not MONGO_COLLECTION:
        raise ValueError("Missing MONGO_URI / MONGO_DB / MONGO_COLLECTION in .env")

    # Fail fast on bad DNS/port/auth instead of hanging
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=15000,
    )

    # Force connection now (so errors show immediately)
    client.admin.command("ping")

    col = client[MONGO_DB][MONGO_COLLECTION]
    print(f"Connected: DB={MONGO_DB} | Collection={MONGO_COLLECTION}")

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []

    if "fit_score" not in headers:
        headers.append("fit_score")

    matched = 0
    failed = 0

    for i, row in enumerate(rows, start=1):
        app_raw = (row.get("application_obj_id") or "").strip()
        oid = to_oid(app_raw)

        if not oid:
            row["fit_score"] = ""
            continue

        print(f"#{i} Processing application_obj_id: {app_raw} -> {oid}")

        try:
            doc = col.find_one(
                {"_id": oid},
                {"profile.fit_score": 1}  # projection
            )
        except Exception as e:
            # Print the REAL error and continue
            print(f"   ❌ Query failed for {oid}: {type(e).__name__}: {e}")
            row["fit_score"] = ""
            failed += 1
            continue

        if not doc:
            row["fit_score"] = ""
            continue

        row["fit_score"] = ((doc.get("profile") or {}).get("fit_score")) or ""
        matched += 1

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print("DONE")
    print("Output:", OUTPUT_CSV)
    print("Matched:", matched)
    print("Query failures:", failed)


if __name__ == "__main__":
    main()
