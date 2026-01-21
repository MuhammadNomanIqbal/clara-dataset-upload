import re
import requests
from pathlib import Path

# -------- CONFIG --------
BASE_DIR = Path("/home/asim/Downloads/job_wise_resumes")  # job_1393/, job_1394/...
JOB_ID_MAP = {
    "job_1393": "6970824f268a3cc01aaeff8d",
}
API_BASE = "https://deindev.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job"

VALIDATE_EMAIL_URL = "https://deindev.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job/validate-email/"  # POST JSON
UPLOAD_URL_TEMPLATE = f"{API_BASE}/upload-candidate-resume/{{job_obj_id}}"  # POST multipart

EMAIL_DOMAIN = "fake-domain.com"
EMAIL_PREFIX = "fake-for-warden"

SKIP_ON_VALIDATE_FAIL = True
MOVE_FILES = False  # keep originals; not used here, but left for you
# ------------------------


FILENAME_RE = re.compile(r"^app_(?P<prefix>[A-Za-z]+)_(?P<job>\d+)_(?P<resume>\d+)_(?P<idx>\d+)\.pdf$")


def extract_profile_id_from_filename(pdf_name: str) -> str | None:
    # app_pcf_1393_100225_0.pdf -> pcf_100225
    m = FILENAME_RE.match(pdf_name)
    if not m:
        return None
    return f"{m.group('prefix')}_{m.group('resume')}"


def build_fake_email(profile_id: str) -> str:
    return f"{EMAIL_PREFIX}-{profile_id}@{EMAIL_DOMAIN}"


def extract_text_pdfplumber(pdf_path: Path, max_pages: int = 2) -> str:
    """
    Reads text fresh for each PDF.
    """
    import pdfplumber

    text_parts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages[:max_pages]):
            t = page.extract_text() or ""
            text_parts.append(t)
    return "\n".join(text_parts)


def is_name_candidate(line: str) -> bool:
    """
    Decide if a line looks like a person name.
    """
    line = line.strip()
    if not line:
        return False
    if len(line) > 60:
        return False

    low = line.lower()
    # skip lines that usually aren't names
    bad = ["resume", "curriculum", "vitae", "cv", "email", "phone", "contact", "address", "linkedin", "objective", "summary"]
    if any(k in low for k in bad):
        return False

    # must be mostly letters/spaces/hyphen/apostrophe
    cleaned = re.sub(r"[^A-Za-z\s\-\']", "", line)
    if len(cleaned) < 3:
        return False

    parts = cleaned.split()
    if not (2 <= len(parts) <= 4):
        return False

    # each token should have letters and not be single-character only
    if any(len(p) < 2 for p in parts):
        return False

    return True


def guess_first_last_name_from_text(text: str) -> tuple[str, str]:
    """
    Better heuristic:
    - check first ~30 non-empty lines
    - pick the first line that looks like a name
    """
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]  # keep non-empty only

    for ln in lines[:30]:
        # remove weird characters but keep spaces/hyphens/apostrophes
        candidate = re.sub(r"[^A-Za-z\s\-\']", " ", ln).strip()
        candidate = re.sub(r"\s+", " ", candidate)

        if is_name_candidate(candidate):
            parts = candidate.split()
            first = parts[0].title()
            last = " ".join(parts[1:]).title()
            return first, last

    return "Unknown", "Candidate"


def validate_email(email: str, job_obj_id: str) -> tuple[bool, dict]:
    """
    FIX: use POST (not GET).
    """
    payload = {"email": email, "job_obj_id": job_obj_id}
    r = requests.post(VALIDATE_EMAIL_URL, json=payload)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    ok = (200 <= r.status_code < 300)
    return ok, data


def upload_resume(job_obj_id: str, first_name: str, last_name: str, email: str, pdf_path: Path) -> tuple[bool, dict]:
    """
    Upload resume multipart.
    """
    url = UPLOAD_URL_TEMPLATE.format(job_obj_id=job_obj_id)
    data = {"first_name": first_name, "last_name": last_name, "email": email}
    with open(pdf_path, "rb") as f:
        files = {"file": (pdf_path.name, f, "application/pdf")}
        r = requests.post(url, data=data, files=files)

    try:
        resp = r.json()
    except Exception:
        resp = {"raw": r.text}

    ok = (200 <= r.status_code < 300)
    return ok, resp


def main():
    total = uploaded = skipped = failed = 0

    for job_folder, job_obj_id in JOB_ID_MAP.items():
        folder_path = BASE_DIR / job_folder
        if not folder_path.exists():
            print(f"[WARN] Missing folder: {folder_path}")
            continue

        pdfs = sorted(folder_path.glob("*.pdf"))
        print(f"\n=== Processing {job_folder} -> {job_obj_id} | files={len(pdfs)} ===")

        for pdf_path in pdfs:
            total += 1

            profile_id = extract_profile_id_from_filename(pdf_path.name)
            if not profile_id:
                print(f"[SKIP] Bad filename: {pdf_path.name}")
                skipped += 1
                continue

            email = build_fake_email(profile_id)

            # IMPORTANT: extract text fresh for THIS PDF
            text = extract_text_pdfplumber(pdf_path)
            first_name, last_name = guess_first_last_name_from_text(text)

            print(f"Parsed name: {first_name} {last_name} from {pdf_path.name}")
            print(f"Validating email: {email} for job_obj_id: {job_obj_id}")

            v_ok, v_resp = validate_email(email=email, job_obj_id=job_obj_id)
            if not v_ok:
                print(f"[VALIDATE FAIL] {pdf_path.name} | resp={v_resp}")
                if SKIP_ON_VALIDATE_FAIL:
                    skipped += 1
                    continue

            u_ok, u_resp = upload_resume(
                job_obj_id=job_obj_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                pdf_path=pdf_path,
            )

            if u_ok:
                uploaded += 1
                print(f"[UPLOADED] {pdf_path.name} -> {email}")
            else:
                failed += 1
                print(f"[UPLOAD FAIL] {pdf_path.name} | resp={u_resp}")

    print("\n====== SUMMARY ======")
    print(f"Total: {total}")
    print(f"Uploaded: {uploaded}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
