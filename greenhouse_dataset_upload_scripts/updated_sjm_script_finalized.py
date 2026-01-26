import re
import csv
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
import pdfplumber

# ============== DEFAULT CONFIG ==============
DEFAULT_BASE_DIR = Path("/home/asim/Desktop/clara-dataset-upload/clara_dataset/resume_dataset/job_wise_resumes")

DEFAULT_JOB_MAP_CSV_PATH = Path(
    "/home/asim/Desktop/clara-dataset-upload/clara_dataset/resume_dataset/Clara - Candidate Matching - 2026-01-20 - Applications.csv"
)

API_BASE = "https://deindev.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job"
VALIDATE_EMAIL_URL = f"{API_BASE}/validate-email/"
UPLOAD_URL_TEMPLATE = f"{API_BASE}/upload-candidate-resume/{{job_obj_id}}"

HEADERS = {"Accept": "application/json"}

EMAIL_DOMAIN = "fake-domain.com"
EMAIL_PREFIX = "fake-for-warden"
UPLOAD_FILE_FIELD = "file"

SKIP_ON_VALIDATE_FAIL = True

OUT_DIR = Path("/home/asim/Desktop/clara-dataset-upload/logs")
SUCCESS_CSV_PATH = OUT_DIR / "profile_upload_success.csv"
FAILURES_CSV_PATH = OUT_DIR / "profile_upload_failures.csv"
PROGRESS_LOG_PATH = OUT_DIR / "progress.log"

REQUEST_TIMEOUT_VALIDATE = 60
REQUEST_TIMEOUT_UPLOAD = 120
# ====================================

FILENAME_RE = re.compile(
    r"^(?P<full>app_(?P<prefix>[A-Za-z]+)_(?P<job>\d+)_(?P<resume>\d+)_(?P<idx>\d+))\.pdf$"
)

BAD_KEYWORDS = {
    "summary", "experience", "education", "skills", "certifications", "projects",
    "profile", "objective", "contact", "portfolio", "linkedin",
    "phone", "email", "address", "curriculum", "vitae", "resume", "cv"
}

SUCCESS_HEADERS = [
    "timestamp",
    "job_obj_id",
    "job_id",
    "job_title",
    "profile_id",
    "external_id",
    "email",
    "status_code",
    "message",
    "candidate_obj_id",
    "application_obj_id",
]

FAIL_HEADERS = [
    "timestamp",
    "job_obj_id",
    "job_id",
    "job_title",
    "profile_id",
    "external_id",
    "email",
    "status_code",
    "message",
]


# ---------------- progress log ----------------
def ensure_progress_log_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def log_progress(line: str):
    ensure_progress_log_dir()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"{ts} | {line}"
    print(msg)
    with open(PROGRESS_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


# ---------------- CSV helpers ----------------
def ensure_csv_header(path: Path, headers: list[str]):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(headers)


def write_success_row(**kwargs):
    ensure_csv_header(SUCCESS_CSV_PATH, SUCCESS_HEADERS)
    with open(SUCCESS_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([kwargs.get(h, "") for h in SUCCESS_HEADERS])


def write_fail_row(**kwargs):
    ensure_csv_header(FAILURES_CSV_PATH, FAIL_HEADERS)
    with open(FAILURES_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([kwargs.get(h, "") for h in FAIL_HEADERS])


# ---------------- Job map CSV ----------------
def _norm_key(k: str) -> str:
    k = (k or "").strip().lower()
    k = re.sub(r"[^a-z0-9]+", "_", k)
    k = re.sub(r"_+", "_", k).strip("_")
    return k


def normalize_job_id(job_id: str) -> str:
    s = (job_id or "").strip()
    m = re.search(r"(\d+)", s)
    return m.group(1) if m else ""


def normalize_job_folder(job_id: str) -> str:
    num = normalize_job_id(job_id)
    return f"job_{num}" if num else ""


def load_job_map(csv_path: Path) -> list[dict]:
    """
    Returns list of dict:
      {"job_id": "1393", "job_obj_id": "...", "job_title": "..."}
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Job map CSV not found: {csv_path}")

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return rows

        for raw in reader:
            row = {_norm_key(k): (v or "").strip() for k, v in raw.items()}

            raw_job_id = row.get("job_id") or row.get("job") or ""
            job_id = normalize_job_id(raw_job_id)

            job_obj_id = (
                row.get("job_obj_id")
                or row.get("job_objid")
                or row.get("job_obj")
                or row.get("jobobjid")
            )

            job_title = (
                row.get("job_title")
                or row.get("jobtitle")
                or row.get("title")
                or row.get("job_name")
                or row.get("jobname")
                or ""
            )

            if not job_id or not job_obj_id:
                continue

            rows.append({"job_id": job_id, "job_obj_id": job_obj_id, "job_title": job_title})

    return rows


# ---------------- resume helpers ----------------
def parse_filename(pdf_name: str) -> Optional[dict]:
    m = FILENAME_RE.match(pdf_name)
    if not m:
        return None
    prefix = m.group("prefix")
    job_id = m.group("job")
    resume = m.group("resume")
    return {
        "full_stem": m.group("full"),
        "prefix": prefix,
        "job_id": job_id,
        "resume": resume,
        "idx": m.group("idx"),
        "profile_id": f"{prefix}_{resume}",
    }


def build_fake_email(full_resume_stem: str) -> str:
    return f"{EMAIL_PREFIX}-{full_resume_stem}@{EMAIL_DOMAIN}"


def extract_text_first_page(pdf_path: Path) -> str:
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            if not pdf.pages:
                return ""
            return (pdf.pages[0].extract_text() or "").strip()
    except Exception:
        return ""


def normalize_line(line: str) -> str:
    line = re.sub(r"[^A-Za-z\s\-\']", " ", line)
    line = re.sub(r"\s+", " ", line).strip()
    return line


def is_reasonable_name(line: str) -> bool:
    if not line or len(line) > 60:
        return False
    if "@" in line or "|" in line:
        return False
    if any(ch.isdigit() for ch in line):
        return False
    low = line.lower()
    if any(k in low for k in BAD_KEYWORDS):
        return False
    cleaned = normalize_line(line)
    parts = cleaned.split()
    if not (2 <= len(parts) <= 4):
        return False
    if any(len(p) < 2 for p in parts):
        return False
    return True


def extract_first_last_name(pdf_path: Path) -> Tuple[str, str]:
    text = extract_text_first_page(pdf_path)
    if not text:
        return ("Unknown", "Candidate")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    if lines and is_reasonable_name(lines[0]):
        cleaned = normalize_line(lines[0])
        parts = cleaned.split()
        return (parts[0].title(), " ".join(parts[1:]).title())

    for ln in lines[:20]:
        if is_reasonable_name(ln):
            cleaned = normalize_line(ln)
            parts = cleaned.split()
            return (parts[0].title(), " ".join(parts[1:]).title())

    return ("Unknown", "Candidate")


def safe_json(resp: requests.Response) -> dict | None:
    try:
        return resp.json()
    except Exception:
        return None


def get_message_candidate_app(resp_json: dict | None) -> tuple[str, str, str]:
    if not isinstance(resp_json, dict):
        return "", "", ""
    msg = resp_json.get("message") or resp_json.get("error") or resp_json.get("msg") or ""
    candidate_obj_id = ""
    application_obj_id = ""
    data = resp_json.get("data")
    if isinstance(data, dict):
        candidate_obj_id = data.get("candidate_obj_id") or ""
        application_obj_id = data.get("application_obj_id") or ""
    return msg, candidate_obj_id, application_obj_id


# ---------------- API calls ----------------
def validate_email(session: requests.Session, email: str, job_obj_id: str):
    payload = {"email": email, "job_obj_id": job_obj_id}
    resp = session.post(
        VALIDATE_EMAIL_URL,
        json=payload,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_VALIDATE,
    )
    js = safe_json(resp)
    ok = 200 <= resp.status_code < 300
    return ok, resp.status_code, js


def upload_resume(session: requests.Session, job_obj_id: str, first_name: str, last_name: str, email: str, pdf_path: Path):
    url = UPLOAD_URL_TEMPLATE.format(job_obj_id=job_obj_id)
    data = {"first_name": first_name, "last_name": last_name, "email": email}

    with open(pdf_path, "rb") as f:
        files = {UPLOAD_FILE_FIELD: (pdf_path.name, f, "application/pdf")}
        resp = session.post(
            url,
            data=data,
            files=files,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT_UPLOAD,
        )

    js = safe_json(resp)
    ok = 200 <= resp.status_code < 300
    return ok, resp.status_code, js


# ---------------- Job runner ----------------
def run_one_job(base_dir: Path, job_id: str, job_obj_id: str, job_title: str, session: requests.Session):
    external_folder = normalize_job_folder(job_id)
    folder_path = base_dir / external_folder

    if not folder_path.exists():
        log_progress(f"[FOLDER MISSING] job_id=job_{job_id} | job_obj_id={job_obj_id} | path={folder_path}")
        return (0, 0, 0, 0, 0)  # totals

    pdfs = sorted(folder_path.glob("*.pdf"))

    job_total = 0
    job_upload_ok = 0
    job_upload_fail = 0
    job_validate_fail = 0
    job_parse_fail = 0

    log_progress(
        f"=== START JOB {external_folder} | job_id={job_id} | job_title={job_title} -> job_obj_id={job_obj_id} | files={len(pdfs)} ==="
    )

    for pdf_path in pdfs:
        job_total += 1

        info = parse_filename(pdf_path.name)
        if not info:
            job_parse_fail += 1
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id="",
                external_id="",
                email="",
                status_code="",
                message="parse: Bad filename format",
            )
            log_progress(f"[{external_folder}] #{job_total} PARSE_FAIL")
            continue

        profile_id = info["profile_id"]
        external_id = info["full_stem"]
        email = build_fake_email(external_id)

        first_name, last_name = extract_first_last_name(pdf_path)

        # VALIDATE
        try:
            v_ok, v_status, v_json = validate_email(session, email, job_obj_id)
        except Exception as e:
            job_validate_fail += 1
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id=profile_id,
                external_id=external_id,
                email=email,
                status_code="",
                message=f"validate: Exception: {e}",
            )
            log_progress(f"[{external_folder}] #{job_total} VALIDATE_EXCEPTION")
            continue

        v_msg, v_candidate, v_app = get_message_candidate_app(v_json)

        if not v_ok:
            job_validate_fail += 1
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id=profile_id,
                external_id=external_id,
                email=email,
                status_code=str(v_status),
                message=f"validate: {v_msg or 'validate_failed'}",
            )
            log_progress(f"[{external_folder}] #{job_total} VALIDATE_FAIL({v_status})")
            if SKIP_ON_VALIDATE_FAIL:
                continue

        # UPLOAD
        try:
            u_ok, u_status, u_json = upload_resume(session, job_obj_id, first_name, last_name, email, pdf_path)
        except Exception as e:
            job_upload_fail += 1
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id=profile_id,
                external_id=external_id,
                email=email,
                status_code="",
                message=f"upload: Exception: {e}",
            )
            log_progress(f"[{external_folder}] #{job_total} UPLOAD_EXCEPTION")
            continue

        u_msg, u_candidate, u_app = get_message_candidate_app(u_json)

        if u_ok:
            job_upload_ok += 1
            write_success_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id=profile_id,
                external_id=external_id,
                email=email,
                status_code=str(u_status),
                message=f"upload: {u_msg or 'upload_ok'}",
                candidate_obj_id=u_candidate or "",
                application_obj_id=u_app or "",
            )
            log_progress(f"[{external_folder}] #{job_total} UPLOAD_OK")
        else:
            job_upload_fail += 1
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                job_obj_id=job_obj_id,
                job_id=job_id,
                job_title=job_title,
                profile_id=profile_id,
                external_id=external_id,
                email=email,
                status_code=str(u_status),
                message=f"upload: {u_msg or 'upload_failed'}",
            )
            log_progress(f"[{external_folder}] #{job_total} UPLOAD_FAIL({u_status})")

    log_progress(
        f"=== DONE JOB {external_folder} | total={job_total} ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail} ==="
    )
    return (job_total, job_upload_ok, job_upload_fail, job_validate_fail, job_parse_fail)


# ---------------- Main ----------------
def main():
    parser = argparse.ArgumentParser(description="Upload resumes for one job or all jobs from CSV.")
    parser.add_argument("--job_obj_id", help="Run only this job_obj_id", default=None)
    parser.add_argument("--job_id", help="Run only this numeric job_id (e.g., 1393)", default=None)
    parser.add_argument("--job_map_csv", help="Job map CSV path", default=str(DEFAULT_JOB_MAP_CSV_PATH))
    parser.add_argument("--base_dir", help="Base resume folder", default=str(DEFAULT_BASE_DIR))
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    job_map_csv_path = Path(args.job_map_csv)

    ensure_csv_header(SUCCESS_CSV_PATH, SUCCESS_HEADERS)
    ensure_csv_header(FAILURES_CSV_PATH, FAIL_HEADERS)
    ensure_progress_log_dir()

    session = requests.Session()

    job_rows = load_job_map(job_map_csv_path)
    if not job_rows:
        log_progress(f"RUN START | ERROR: No valid rows loaded from {job_map_csv_path}")
        log_progress("Tip: Ensure your CSV has job_id and job_obj_id columns.")
        log_progress("RUN END")
        return

    # Filter: one job only
    if args.job_obj_id:
        job_rows = [r for r in job_rows if (r.get("job_obj_id") or "") == args.job_obj_id.strip()]
    if args.job_id:
        jid = normalize_job_id(args.job_id)
        job_rows = [r for r in job_rows if (r.get("job_id") or "") == jid]

    if not job_rows:
        log_progress("RUN START | ERROR: No job matched your filter (--job_obj_id/--job_id).")
        log_progress("RUN END")
        return

    log_progress(f"RUN START | BASE_DIR={base_dir} | API_BASE={API_BASE} | jobs_to_run={len(job_rows)}")

    grand_total = grand_ok = grand_fail = grand_validate_fail = grand_parse_fail = 0

    for r in job_rows:
        t, ok, fail, vfail, pফail = run_one_job(
            base_dir=base_dir,
            job_id=r["job_id"],
            job_obj_id=r["job_obj_id"],
            job_title=r.get("job_title", ""),
            session=session,
        )
        grand_total += t
        grand_ok += ok
        grand_fail += fail
        grand_validate_fail += vfail
        grand_parse_fail += pফail

    log_progress("====== GRAND SUMMARY ======")
    log_progress(f"Total processed: {grand_total}")
    log_progress(f"Uploaded OK:     {grand_ok}")
    log_progress(f"Upload Failed:   {grand_fail}")
    log_progress(f"Validate Failed: {grand_validate_fail}")
    log_progress(f"Parse Failed:    {grand_parse_fail}")
    log_progress(f"Success CSV: {SUCCESS_CSV_PATH}")
    log_progress(f"Failures CSV: {FAILURES_CSV_PATH}")
    log_progress(f"Progress log: {PROGRESS_LOG_PATH}")
    log_progress("RUN END")


if __name__ == "__main__":
    main()
