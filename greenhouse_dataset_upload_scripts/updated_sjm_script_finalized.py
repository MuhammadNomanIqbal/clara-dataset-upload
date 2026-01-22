import re
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
import pdfplumber

# ============== CONFIG ==============
BASE_DIR = Path("/home/asim/Downloads/job_wise_resumes")

# Map: folder name -> job_obj_id
JOB_ID_MAP = {
    "job_1393": "6970c43309b0d28599ec8071",
    # "job_1394": "....",
}

API_BASE = "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job"
VALIDATE_EMAIL_URL = f"{API_BASE}/validate-email/"
UPLOAD_URL_TEMPLATE = f"{API_BASE}/upload-candidate-resume/{{job_obj_id}}"

HEADERS = {"Accept": "application/json"}

EMAIL_DOMAIN = "fake-domain.com"
EMAIL_PREFIX = "fake-for-warden"
UPLOAD_FILE_FIELD = "file"

SKIP_ON_VALIDATE_FAIL = True

OUT_DIR = Path("/home/asim/Desktop/clara-dataset-upload/logs")
SUCCESS_CSV_PATH = OUT_DIR / "success.csv"
FAILURES_CSV_PATH = OUT_DIR / "failures.csv"

# NEW: progress log file (for monitoring with tail -f)
PROGRESS_LOG_PATH = OUT_DIR / "progress.log"

REQUEST_TIMEOUT_VALIDATE = 60
REQUEST_TIMEOUT_UPLOAD = 120
# ====================================

# Example: app_pcf_249_55317_0.pdf
FILENAME_RE = re.compile(
    r"^(?P<full>app_(?P<prefix>[A-Za-z]+)_(?P<job>\d+)_(?P<resume>\d+)_(?P<idx>\d+))\.pdf$"
)

BAD_KEYWORDS = {
    "summary", "experience", "education", "skills", "certifications", "projects",
    "profile", "objective", "contact", "portfolio", "linkedin",
    "phone", "email", "address", "curriculum", "vitae", "resume", "cv"
}

# ---------------- CSV schema ----------------
SUCCESS_HEADERS = [
    "timestamp",
    "job_obj_id",
    "job_id",
    "profile_id",
    "profile_name",
    "email",
    "stage",
    "status_code",
    "message",
    "candidate_obj_id",
    "application_obj_id",
]

FAIL_HEADERS = [
    "timestamp",
    "job_obj_id",
    "job_id",
    "profile_id",
    "profile_name",
    "email",
    "stage",
    "status_code",
    "message",
    "candidate_obj_id",
    "application_obj_id",
]


# ---------------- File logging (progress.log) ----------------
def ensure_progress_log_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def log_progress(line: str):
    """
    Writes one line to progress.log (and also prints it).
    You can monitor with:
      tail -f /home/asim/Desktop/clara-dataset-upload/logs/progress.log
    """
    ensure_progress_log_dir()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"{ts} | {line}"
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


# ---------------- Utility ----------------
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


def extract_profile_id_from_filename(pdf_name: str) -> Optional[str]:
    info = parse_filename(pdf_name)
    return info["profile_id"] if info else None


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


# ---------------- Main ----------------
def main():
    ensure_csv_header(SUCCESS_CSV_PATH, SUCCESS_HEADERS)
    ensure_csv_header(FAILURES_CSV_PATH, FAIL_HEADERS)
    ensure_progress_log_dir()

    session = requests.Session()

    grand_total = grand_ok = grand_fail = grand_skip = 0

    log_progress(f"RUN START | BASE_DIR={BASE_DIR} | API_BASE={API_BASE}")

    for external_id, job_obj_id in JOB_ID_MAP.items():
        folder_path = BASE_DIR / external_id
        if not folder_path.exists():
            log_progress(f"[FOLDER MISSING] {external_id} | job_obj_id={job_obj_id} | path={folder_path}")
            continue

        pdfs = sorted(folder_path.glob("*.pdf"))

        # per job counters
        job_total = 0
        job_upload_ok = 0
        job_upload_fail = 0
        job_validate_fail = 0
        job_parse_fail = 0

        log_progress(f"=== START JOB {external_id} -> job_obj_id={job_obj_id} | files={len(pdfs)} ===")

        for pdf_path in pdfs:
            grand_total += 1
            job_total += 1

            info = parse_filename(pdf_path.name)
            if not info:
                grand_skip += 1
                job_parse_fail += 1

                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id="",
                    profile_id="",
                    profile_name="",
                    email="",
                    stage="parse",
                    status_code="",
                    message="Bad filename format",
                    candidate_obj_id="",
                    application_obj_id="",
                )

                log_progress(f"[{external_id}] #{job_total} PARSE_FAIL | ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail}")
                continue

            job_id = info["job_id"]
            profile_id = info["profile_id"]
            full_stem = info["full_stem"]
            email = build_fake_email(full_stem)

            first_name, last_name = extract_first_last_name(pdf_path)
            profile_name = f"{first_name} {last_name}".strip()

            # VALIDATE
            try:
                v_ok, v_status, v_json = validate_email(session, email, job_obj_id)
            except Exception as e:
                grand_fail += 1
                job_validate_fail += 1

                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id=job_id,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    email=email,
                    stage="validate",
                    status_code="",
                    message=f"Exception: {e}",
                    candidate_obj_id="",
                    application_obj_id="",
                )

                log_progress(f"[{external_id}] #{job_total} VALIDATE_EXCEPTION | ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail}")
                continue

            v_msg, v_candidate, v_app = get_message_candidate_app(v_json)

            if not v_ok:
                grand_skip += 1
                job_validate_fail += 1

                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id=job_id,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    email=email,
                    stage="validate",
                    status_code=str(v_status),
                    message=v_msg or "validate_failed",
                    candidate_obj_id=v_candidate or "",
                    application_obj_id=v_app or "",
                )

                log_progress(f"[{external_id}] #{job_total} VALIDATE_FAIL({v_status}) | ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail}")
                if SKIP_ON_VALIDATE_FAIL:
                    continue

            # UPLOAD
            try:
                u_ok, u_status, u_json = upload_resume(session, job_obj_id, first_name, last_name, email, pdf_path)
            except Exception as e:
                grand_fail += 1
                job_upload_fail += 1

                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id=job_id,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    email=email,
                    stage="upload",
                    status_code="",
                    message=f"Exception: {e}",
                    candidate_obj_id="",
                    application_obj_id="",
                )

                log_progress(f"[{external_id}] #{job_total} UPLOAD_EXCEPTION | ok={job_upload_ok} upload_fail={job_upload_fail}")
                continue

            u_msg, u_candidate, u_app = get_message_candidate_app(u_json)

            if u_ok:
                grand_ok += 1
                job_upload_ok += 1

                write_success_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id=job_id,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    email=email,
                    stage="upload",
                    status_code=str(u_status),
                    message=u_msg or "upload_ok",
                    candidate_obj_id=u_candidate or "",
                    application_obj_id=u_app or "",
                )

                log_progress(f"[{external_id}] #{job_total} UPLOAD_OK | ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail}")

            else:
                grand_fail += 1
                job_upload_fail += 1

                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    job_obj_id=job_obj_id,
                    job_id=job_id,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    email=email,
                    stage="upload",
                    status_code=str(u_status),
                    message=u_msg or "upload_failed",
                    candidate_obj_id=u_candidate or "",
                    application_obj_id=u_app or "",
                )

                log_progress(f"[{external_id}] #{job_total} UPLOAD_FAIL({u_status}) | ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail}")

        log_progress(f"=== DONE JOB {external_id} | total={job_total} ok={job_upload_ok} upload_fail={job_upload_fail} validate_fail={job_validate_fail} parse_fail={job_parse_fail} ===")

    log_progress("====== GRAND SUMMARY ======")
    log_progress(f"Total processed: {grand_total}")
    log_progress(f"Uploaded OK:     {grand_ok}")
    log_progress(f"Skipped:         {grand_skip}  (validate fail or parse fail)")
    log_progress(f"Upload Failed:   {grand_fail}")
    log_progress(f"Success CSV: {SUCCESS_CSV_PATH}")
    log_progress(f"Failures CSV: {FAILURES_CSV_PATH}")
    log_progress(f"Progress log: {PROGRESS_LOG_PATH}")
    log_progress("RUN END")


if __name__ == "__main__":
    main()
