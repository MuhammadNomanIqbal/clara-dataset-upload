import csv
import shutil
from pathlib import Path

CSV_PATH = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/Clara - Candidate Matching - 2026-01-20 - Applications.csv")  # your clara-candidate Matching.csv

RENAMED_DIR = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/resume_dataset/external_id_resumes")          # where external_id.pdf files exist
JOBS_OUT_DIR = Path("/home/asim/Downloads/job_wise_resumes")            # output: job_258/app_....pdf

JOB_COL = "job_id"
EXTERNAL_COL = "external_id"

# Change this to False if you want to COPY instead of MOVE
MOVE_FILES = True


def normalize_job_folder(job_id: str, external_id: str) -> str | None:
    """
    Returns folder name like 'job_258'.
    Uses CSV job_id if possible; otherwise extracts from external_id like app_pcf_258_72261_0.
    """
    job_id = (job_id or "").strip()
    if job_id:
        # If job_id is "258" make it "job_258"
        if job_id.isdigit():
            return f"job_{job_id}"
        # If job_id already like "job_258", keep it
        if job_id.startswith("job_"):
            return job_id
        # Otherwise, still store as-is
        return job_id

    # fallback: parse from external_id: app_pcf_258_72261_0 => 258
    parts = (external_id or "").strip().split("_")
    if len(parts) >= 4 and parts[0] == "app":
        maybe_job = parts[2]
        if maybe_job.isdigit():
            return f"job_{maybe_job}"

    return None


def main():
    JOBS_OUT_DIR.mkdir(parents=True, exist_ok=True)

    moved_or_copied = 0
    missing = 0
    bad_rows = 0

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("CSV header not found.")
        if EXTERNAL_COL not in reader.fieldnames:
            raise ValueError(f"CSV must contain '{EXTERNAL_COL}'. Found: {reader.fieldnames}")

        for row in reader:
            external_id = (row.get(EXTERNAL_COL) or "").strip()
            job_id = (row.get(JOB_COL) or "").strip()

            if not external_id:
                bad_rows += 1
                continue

            folder_name = normalize_job_folder(job_id, external_id)
            if not folder_name:
                print(f"BAD (cannot determine job folder): external_id={external_id}, job_id={job_id}")
                bad_rows += 1
                continue

            src_pdf = RENAMED_DIR / f"{external_id}.pdf"
            if not src_pdf.exists():
                print(f"MISSING: {src_pdf}")
                missing += 1
                continue

            dest_dir = JOBS_OUT_DIR / folder_name
            dest_dir.mkdir(parents=True, exist_ok=True)

            dest_pdf = dest_dir / src_pdf.name

            if MOVE_FILES:
                shutil.move(str(src_pdf), str(dest_pdf))
            else:
                shutil.copy2(src_pdf, dest_pdf)

            moved_or_copied += 1

    print("\nDone.")
    print(f"{'Moved' if MOVE_FILES else 'Copied'}: {moved_or_copied}")
    print(f"Missing renamed PDFs: {missing}")
    print(f"Bad/empty rows: {bad_rows}")
    print(f"Output root: {JOBS_OUT_DIR}")


if __name__ == "__main__":
    main()
