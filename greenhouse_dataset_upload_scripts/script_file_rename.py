import csv
import shutil
from pathlib import Path

CSV_PATH = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/Clara - Candidate Matching - 2026-01-20 - Applications.csv")

SRC_DIR = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/resume_dataset/profile_resumes")  # where profile_id.pdf files exist
OUT_DIR = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/resume_dataset/external_id_resumes")# where external_id.pdf copies will be saved

PROFILE_COL = "profile_id"
EXTERNAL_COL = "external_id"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing = 0

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        # quick validation
        if not reader.fieldnames:
            raise ValueError("CSV header not found.")
        if PROFILE_COL not in reader.fieldnames or EXTERNAL_COL not in reader.fieldnames:
            raise ValueError(
                f"CSV must contain columns: '{PROFILE_COL}' and '{EXTERNAL_COL}'. "
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            profile_id = (row.get(PROFILE_COL) or "").strip()
            external_id = (row.get(EXTERNAL_COL) or "").strip()

            if not profile_id or not external_id:
                continue

            src_pdf = SRC_DIR / f"{profile_id}.pdf"
            dst_pdf = OUT_DIR / f"{external_id}.pdf"

            if not src_pdf.exists():
                missing += 1
                print(f"Missing: {src_pdf}")
                continue

            # copy + rename (original remains untouched)
            shutil.copy2(src_pdf, dst_pdf)
            copied += 1

    print("\nDone.")
    print(f"Copied: {copied}")
    print(f"Missing originals: {missing}")
    print(f"Output folder: {OUT_DIR}")


if __name__ == "__main__":
    main()
