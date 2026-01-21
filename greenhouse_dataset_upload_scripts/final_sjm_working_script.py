import re
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
import pdfplumber

# ============== CONFIG ==============
BASE_DIR = Path("/home/asim/Downloads/job_wise_resumes")

JOB_ID_MAP = {
    "job_1393": "6970c43309b0d28599ec8071",
}

API_BASE = "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job"
VALIDATE_EMAIL_URL = f"{API_BASE}/validate-email/"
UPLOAD_URL_TEMPLATE = f"{API_BASE}/upload-candidate-resume/{{job_obj_id}}"

HEADERS = {
    "Accept": "application/json",
    # "Authorization": "Bearer YOUR_TOKEN",
}

EMAIL_DOMAIN = "fake-domain.com"
EMAIL_PREFIX = "fake-for-warden"
UPLOAD_FILE_FIELD = "file"

SKIP_ON_VALIDATE_FAIL = True

OUT_DIR = Path("/home/asim/Desktop/clara-dataset-upload/logs")
SUCCESS_CSV_PATH = OUT_DIR / "success.csv"
FAILURES_CSV_PATH = OUT_DIR / "failures.csv"

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


# ---------------- CSV schema (UPDATED as requested) ----------------
# success: remove response_json, remove pdf_file
SUCCESS_HEADERS = [
    "timestamp",
    "external_id",
    "job_obj_id",
    "profile_id",
    "email",
    "stage",
    "status_code",
    "message",
    "candidate_obj_id",
    "application_obj_id",
]

# failures: remove candidate_obj_id, remove response_json, response_text, remove pdf_file
FAIL_HEADERS = [
    "timestamp",
    "external_id",
    "job_obj_id",
    "profile_id",
    "email",
    "stage",
    "status_code",
    "message",
]


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
    """
    - Upload success: {'message':..., 'data':{candidate_obj_id, application_obj_id}}
    - Validate fail:  {'error':'Email already exists.'}
    """
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

    session = requests.Session()

    total = uploaded = skipped = failed = 0

    for external_id, job_obj_id in JOB_ID_MAP.items():
        folder_path = BASE_DIR / external_id
        if not folder_path.exists():
            # record as failure row (no pdf/profile/email)
            write_fail_row(
                timestamp=datetime.now().isoformat(timespec="seconds"),
                external_id=external_id,
                job_obj_id=job_obj_id,
                profile_id="",
                email="",
                stage="folder_missing",
                status_code="",
                message=f"Missing folder: {folder_path}",
            )
            continue

        pdfs = sorted(folder_path.glob("*.pdf"))

        for pdf_path in pdfs:
            total += 1
            pdf_file = pdf_path.name

            profile_id = extract_profile_id_from_filename(pdf_file)
            if not profile_id:
                skipped += 1
                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id="",
                    email="",
                    stage="parse",
                    status_code="",
                    message="Bad filename format",
                )
                continue

            email = build_fake_email(profile_id)

            # name extraction is not stored in csv anymore, but still needed for upload
            first_name, last_name = extract_first_last_name(pdf_path)

            # -------- VALIDATE --------
            try:
                v_ok, v_status, v_json = validate_email(session, email, job_obj_id)
            except Exception as e:
                failed += 1
                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id=profile_id,
                    email=email,
                    stage="validate",
                    status_code="",
                    message=f"Exception: {e}",
                )
                continue

            v_msg, _, _ = get_message_candidate_app(v_json)

            if not v_ok:
                skipped += 1
                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id=profile_id,
                    email=email,
                    stage="validate",
                    status_code=str(v_status),
                    message=v_msg or "validate_failed",
                )
                if SKIP_ON_VALIDATE_FAIL:
                    continue

            # -------- UPLOAD --------
            try:
                u_ok, u_status, u_json = upload_resume(
                    session, job_obj_id, first_name, last_name, email, pdf_path
                )
            except Exception as e:
                failed += 1
                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id=profile_id,
                    email=email,
                    stage="upload",
                    status_code="",
                    message=f"Exception: {e}",
                )
                continue

            u_msg, u_candidate, u_app = get_message_candidate_app(u_json)

            if u_ok:
                uploaded += 1
                write_success_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id=profile_id,
                    email=email,
                    stage="upload",
                    status_code=str(u_status),
                    message=u_msg or "upload_ok",
                    candidate_obj_id=u_candidate,
                    application_obj_id=u_app,
                )
            else:
                failed += 1
                write_fail_row(
                    timestamp=datetime.now().isoformat(timespec="seconds"),
                    external_id=external_id,
                    job_obj_id=job_obj_id,
                    profile_id=profile_id,
                    email=email,
                    stage="upload",
                    status_code=str(u_status),
                    message=u_msg or "upload_failed",
                )

    print("\n====== SUMMARY ======")
    print(f"Total: {total}")
    print(f"Uploaded: {uploaded}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Success CSV: {SUCCESS_CSV_PATH}")
    print(f"Failures CSV: {FAILURES_CSV_PATH}")


if __name__ == "__main__":
    main()
