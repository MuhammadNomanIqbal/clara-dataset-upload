import re
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
import pdfplumber

# ============== CONFIG ==============
BASE_DIR = Path("/home/asim/Downloads/job_wise_resumes")  # job_1393/, job_1394/...

JOB_ID_MAP = {
    "job_1393": "6970c43309b0d28599ec8071",

}

# IMPORTANT: your current env
API_BASE = "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job"
VALIDATE_EMAIL_URL = f"{API_BASE}/validate-email/"  # POST JSON
UPLOAD_URL_TEMPLATE = f"{API_BASE}/upload-candidate-resume/{{job_obj_id}}"

HEADERS = {
    "Accept": "application/json",
    # "Authorization": "Bearer YOUR_TOKEN",
}

EMAIL_DOMAIN = "fake-domain.com"
EMAIL_PREFIX = "fake-for-warden"
UPLOAD_FILE_FIELD = "file"

SKIP_ON_VALIDATE_FAIL = True

# Where logs will be written
LOG_DIR = Path("/home/asim/Desktop/clara-dataset-upload/logs")
RUN_LOG_PATH = LOG_DIR / "run.log"
FAILURES_CSV_PATH = LOG_DIR / "failures.csv"

REQUEST_TIMEOUT_VALIDATE = 60
REQUEST_TIMEOUT_UPLOAD = 120
# ====================================

FILENAME_RE = re.compile(
    r"^app_(?P<prefix>[A-Za-z]+)_(?P<job>\d+)_(?P<resume>\d+)_(?P<idx>\d+)\.pdf$"
)

BAD_KEYWORDS = {
    "summary", "experience", "education", "skills", "certifications", "projects",
    "profile", "objective", "contact", "portfolio", "linkedin",
    "phone", "email", "address", "curriculum", "vitae", "resume", "cv"
}


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("resume_uploader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(RUN_LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.WARNING)  # console only warnings/errors
    logger.addHandler(ch)

    return logger


def ensure_failures_csv_header():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not FAILURES_CSV_PATH.exists():
        with open(FAILURES_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "timestamp",
                "job_folder",
                "job_obj_id",
                "pdf_file",
                "profile_id",
                "email",
                "first_name",
                "last_name",
                "stage",           # validate/upload/parse/exception
                "http_status",
                "error_message",
                "response_json",
                "response_text",
            ])


def log_failure_csv(
    *,
    job_folder: str,
    job_obj_id: str,
    pdf_file: str,
    profile_id: str,
    email: str,
    first_name: str,
    last_name: str,
    stage: str,
    http_status: str,
    error_message: str,
    response_json: dict | None,
    response_text: str | None,
):
    ensure_failures_csv_header()
    with open(FAILURES_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            datetime.now().isoformat(timespec="seconds"),
            job_folder,
            job_obj_id,
            pdf_file,
            profile_id,
            email,
            first_name,
            last_name,
            stage,
            http_status,
            error_message,
            json.dumps(response_json, ensure_ascii=False) if response_json else "",
            (response_text[:5000] if response_text else ""),
        ])


def extract_profile_id_from_filename(pdf_name: str) -> Optional[str]:
    m = FILENAME_RE.match(pdf_name)
    if not m:
        return None
    return f"{m.group('prefix')}_{m.group('resume')}"


def build_fake_email(profile_id: str) -> str:
    return f"{EMAIL_PREFIX}-{profile_id}@{EMAIL_DOMAIN}"


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


def extract_first_last_name(pdf_path: Path) -> Tuple[str, str, str]:
    """
    Returns (first_name, last_name, top1_line_for_debug)
    """
    text = extract_text_first_page(pdf_path)
    if not text:
        return ("Unknown", "Candidate", "NO TEXT")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    top1 = lines[0] if lines else "NO TEXT"

    # Try TOP1 first (works on your sample PDFs: Roy Ho, Inaya Tang, etc.)
    if lines and is_reasonable_name(lines[0]):
        cleaned = normalize_line(lines[0])
        parts = cleaned.split()
        return (parts[0].title(), " ".join(parts[1:]).title(), top1)

    # Otherwise scan first 20 lines
    for ln in lines[:20]:
        if is_reasonable_name(ln):
            cleaned = normalize_line(ln)
            parts = cleaned.split()
            return (parts[0].title(), " ".join(parts[1:]).title(), top1)

    return ("Unknown", "Candidate", top1)


def safe_parse_response(resp: requests.Response) -> tuple[dict | None, str]:
    """
    Returns (json_or_none, text)
    """
    try:
        return resp.json(), resp.text
    except Exception:
        return None, resp.text


def validate_email(session: requests.Session, email: str, job_obj_id: str) -> tuple[bool, int, dict | None, str]:
    payload = {"email": email, "job_obj_id": job_obj_id}
    resp = session.post(
        VALIDATE_EMAIL_URL,
        json=payload,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_VALIDATE,
    )
    js, txt = safe_parse_response(resp)
    ok = 200 <= resp.status_code < 300
    return ok, resp.status_code, js, txt


def upload_resume(session: requests.Session, job_obj_id: str, first_name: str, last_name: str, email: str, pdf_path: Path) -> tuple[bool, int, dict | None, str]:
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

    js, txt = safe_parse_response(resp)
    ok = 200 <= resp.status_code < 300
    return ok, resp.status_code, js, txt


def main():
    logger = setup_logging()
    ensure_failures_csv_header()

    session = requests.Session()

    total = uploaded = skipped = failed = 0

    logger.info("Starting run | BASE_DIR=%s | API_BASE=%s", str(BASE_DIR), API_BASE)

    for job_folder, job_obj_id in JOB_ID_MAP.items():
        folder_path = BASE_DIR / job_folder
        if not folder_path.exists():
            logger.error("Missing job folder: %s", str(folder_path))
            continue

        pdfs = sorted(folder_path.glob("*.pdf"))
        logger.info("Job start | job_folder=%s | job_obj_id=%s | files=%d", job_folder, job_obj_id, len(pdfs))

        for pdf_path in pdfs:
            total += 1
            pdf_file = pdf_path.name

            profile_id = extract_profile_id_from_filename(pdf_file)
            if not profile_id:
                skipped += 1
                msg = "Bad filename format (skip)"
                logger.warning("%s | job_folder=%s | file=%s", msg, job_folder, pdf_file)
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id="", email="", first_name="", last_name="",
                    stage="parse", http_status="", error_message=msg,
                    response_json=None, response_text=None
                )
                continue

            email = build_fake_email(profile_id)

            try:
                first_name, last_name, top1 = extract_first_last_name(pdf_path)
            except Exception as e:
                failed += 1
                logger.exception("Name extraction exception | job_folder=%s | file=%s", job_folder, pdf_file)
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id=profile_id, email=email, first_name="Unknown", last_name="Candidate",
                    stage="exception", http_status="", error_message=str(e),
                    response_json=None, response_text=None
                )
                continue

            logger.info(
                "Parsed | job_folder=%s | file=%s | profile_id=%s | email=%s | name=%s %s | TOP1=%s",
                job_folder, pdf_file, profile_id, email, first_name, last_name, top1
            )

            # -------- VALIDATE --------
            try:
                v_ok, v_status, v_json, v_text = validate_email(session, email, job_obj_id)
            except Exception as e:
                failed += 1
                logger.exception("Validate exception | job_folder=%s | file=%s", job_folder, pdf_file)
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id=profile_id, email=email, first_name=first_name, last_name=last_name,
                    stage="validate", http_status="", error_message=str(e),
                    response_json=None, response_text=None
                )
                continue

            if not v_ok:
                skipped += 1
                logger.warning(
                    "VALIDATE FAIL | job_folder=%s | file=%s | status=%s | resp_json=%s | resp_text=%s",
                    job_folder, pdf_file, v_status, v_json, (v_text[:500] if v_text else "")
                )
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id=profile_id, email=email, first_name=first_name, last_name=last_name,
                    stage="validate", http_status=str(v_status),
                    error_message="validate_failed",
                    response_json=v_json, response_text=v_text
                )
                if SKIP_ON_VALIDATE_FAIL:
                    continue

            # -------- UPLOAD --------
            try:
                u_ok, u_status, u_json, u_text = upload_resume(
                    session, job_obj_id, first_name, last_name, email, pdf_path
                )
            except Exception as e:
                failed += 1
                logger.exception("Upload exception | job_folder=%s | file=%s", job_folder, pdf_file)
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id=profile_id, email=email, first_name=first_name, last_name=last_name,
                    stage="upload", http_status="", error_message=str(e),
                    response_json=None, response_text=None
                )
                continue

            if u_ok:
                uploaded += 1
                logger.info(
                    "UPLOAD OK | job_folder=%s | file=%s | status=%s | resp_json=%s",
                    job_folder, pdf_file, u_status, u_json
                )
            else:
                failed += 1
                logger.error(
                    "UPLOAD FAIL | job_folder=%s |job_obj_id=%s | file=%s | status=%s | resp_json=%s | resp_text=%s",
                    job_folder, job_obj_id, pdf_file, u_status, u_json, (u_text[:800] if u_text else "")
                )
                log_failure_csv(
                    job_folder=job_folder, job_obj_id=job_obj_id, pdf_file=pdf_file,
                    profile_id=profile_id, email=email, first_name=first_name, last_name=last_name,
                    stage="upload", http_status=str(u_status),
                    error_message="upload_failed",
                    response_json=u_json, response_text=u_text
                )

        logger.info("Job done | job_folder=%s | uploaded=%d | skipped=%d | failed=%d", job_folder, uploaded, skipped, failed)

    logger.info("Run done | total=%d | uploaded=%d | skipped=%d | failed=%d", total, uploaded, skipped, failed)

    print("\n====== SUMMARY ======")
    print(f"Total: {total}")
    print(f"Uploaded: {uploaded}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Run log: {RUN_LOG_PATH}")
    print(f"Failures CSV: {FAILURES_CSV_PATH}")


if __name__ == "__main__":
    main()
