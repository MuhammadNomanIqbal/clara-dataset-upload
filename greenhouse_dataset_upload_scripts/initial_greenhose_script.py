import requests
import time
import base64
from urllib.parse import urlparse, parse_qs
from utils import fetch_details_from_csv, update_csv_with_greenhouse_job_id


class GreenhouseClient:
    def __init__(self, api_key):
        self._base_url = "https://harvest.greenhouse.io/v1"
        self._api_key = api_key
        self._user_id = "4181321007"
        self._next_page = None

    def _make_request(self, method, endpoint, params=None, data=None, json_data=None, max_retries=3):
        url = f"{self._base_url}{endpoint}"

        token = base64.b64encode(f"{self._api_key}:".encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json"
        }

        if method in {"POST", "PATCH", "PUT", "DELETE"}:
            headers["On-Behalf-Of"] = self._user_id

        retries = 0
        while retries <= max_retries:
            try:
                response = requests.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json_data,
                    headers=headers
                )
                response.raise_for_status()
                self._process_headers(response.headers)
                return response.json()

            except requests.HTTPError:
                if response.status_code == 429:
                    retry_after = response.headers.get("retry-after")
                    time.sleep(float(retry_after or 2 ** retries))
                    retries += 1
                    continue
                raise

        raise Exception(f"Max retries exceeded for {method} {endpoint}")

    def _process_headers(self, headers):
        link_header = headers.get("link")
        if not link_header:
            return

        links = requests.utils.parse_header_links(link_header)
        for link in links:
            if link.get("rel") == "next":
                parsed = urlparse(link.get("url"))
                params = parse_qs(parsed.query)
                self._next_page = int(params.get("page", [None])[0])

    # ---------------- JOB METHODS ---------------- #

    def create_job(self, template_job_id, job_title, openings=1):
        payload = {
            "template_job_id": template_job_id,
            "job_name": job_title,
            "number_of_openings": openings
        }
        return self._make_request("POST", "/jobs", json_data=payload)

    def get_job_posts(self, job_id):
        return self._make_request("GET", f"/jobs/{job_id}/job_posts")

    def update_job_post(self, job_post_id, description):
        payload = {
            "content": description,
        }
        return self._make_request("PATCH", f"/job_posts/{job_post_id}", json_data=payload)


job_details = fetch_details_from_csv(input_csv="/home/asim/Desktop/clara-dataset-upload/clara_dataset/update_job_dataset.csv")
print(f"Fetched Job Title: {job_details['job_title']}")
print(f"Fetched Job Description: {job_details['job_description']}")

client = GreenhouseClient(api_key="dc3146c7e00eeea44a3d4ea5be3fbc76-7")
print(f"Initialized Greenhouse Client successfully {client}")
# job = client.create_job(
#     template_job_id="4560475007",
#     job_title=job_details['job_title'],
#     openings=1
# )
# print(f"Created Job successfully: {job}")
# job_id = job["id"]
# update_csv_with_greenhouse_job_id(input_csv="/home/asim/Desktop/clara-dataset-upload/clara_dataset/update_job_dataset.csv",
#                                   output_csv="/home/asim/Desktop/clara-dataset-upload/clara_dataset/final_job_dataset.csv",
#                                   job_title=job_details['job_title'], greenhouse_job_id=job_id)

# Get job post
job_posts = client.get_job_posts(job_id)
job_post_id = job_posts[0]["id"]

# 3. Update job post
client.update_job_post(
    job_post_id=job_post_id,
    description=job_details['job_description']
)

# print("Job created successfully")