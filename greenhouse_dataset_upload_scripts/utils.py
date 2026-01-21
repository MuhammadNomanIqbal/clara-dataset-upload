import csv

def make_fake_email(profile_id):
    return f"fake-for-warden-{profile_id}@fake-domain.com"


def update_csv_with_profile_emails(input_csv, output_csv):
    with open(input_csv, newline="", encoding="utf-8") as infile, \
         open(output_csv, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)

        # Add column only if it does not exist
        fieldnames = reader.fieldnames
        if "profile_emails" not in fieldnames:
            fieldnames = fieldnames + ["profile_emails"]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # If profile_emails already exists AND has value → skip generation
            existing_emails = row.get("profile_emails", "").strip()
            if existing_emails:
                writer.writerow(row)
                continue

            profile_id_value = row.get("profile_id", "").strip()
            emails = []

            if profile_id_value:
                profile_ids = profile_id_value.split(",")
                for pid in profile_ids:
                    pid = pid.strip()
                    if pid:
                        emails.append(make_fake_email(pid))

            row["profile_emails"] = ";".join(emails)
            writer.writerow(row)

    print("CSV updated successfully")


# INPUT_CSV = "/home/asim/Desktop/clara-dataset-upload/clara_dataset/job_dataset.csv"
# OUTPUT_CSV = "/home/asim/Desktop/clara-dataset-upload/clara_dataset/update_job_dataset.csv"
#
# update_csv_with_profile_emails(INPUT_CSV, OUTPUT_CSV)


def fetch_details_from_csv(input_csv):
    jobs_name_description = []
    with open(input_csv, newline="", encoding="utf-8") as infile:

        reader = csv.DictReader(infile)
        for row in reader:
            # Fetch job data (for Greenhouse later)
            job_title = row.get("job_title", "").strip()
            job_description = row.get("job_description", "").strip()
            job_id = row.get("greenhouse_job_id", "").strip()
            if job_id:
                jobs_name_description.append({"job_title": job_title, "job_description": job_description, "job_id": job_id})
            else:
                jobs_name_description.append({"job_title": job_title, "job_description": job_description})
        return jobs_name_description[1]


print(fetch_details_from_csv(input_csv="/home/asim/Desktop/clara-dataset-upload/clara_dataset/final_job_dataset.csv"))


def update_csv_with_greenhouse_job_id(
    input_csv,
    output_csv,
    job_title,
    greenhouse_job_id
):
    with open(input_csv, newline="", encoding="utf-8") as infile, \
         open(output_csv, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)

        fieldnames = reader.fieldnames
        if "greenhouse_job_id" not in fieldnames:
            fieldnames = fieldnames + ["greenhouse_job_id"]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # Match the job row
            if row.get("job_title", "").strip() == job_title.strip():
                # Only update if not already set
                if not row.get("greenhouse_job_id", "").strip():
                    row["greenhouse_job_id"] = str(greenhouse_job_id)

            writer.writerow(row)
