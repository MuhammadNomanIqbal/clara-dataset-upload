import requests
import json

from bson import ObjectId

# Test job application URL

# url = "https://deinqa.infosiphon.com/apply-job?job_obj_id=69674859ded556ac902a8424"
# response = requests.get(url)
# print("Status Code:", response.status_code)
# print("Response Text:", response.json())
#
url = "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job/validate-email/"
# payload ={"email": "fake-for-warden-pcf_100225@fake-domain.com", "job_obj_id": "69674859ded556ac902a8424"}
# payload = {'email': 'fake-for-warden-pcf_100225@fake-domain.com', 'job_obj_id': '6970824f268a3cc01aaeff8d'}
# response = requests.post(url, json=payload)
# print("Status Code:", response.status_code)
# print("Response Text:", response.json())


# url= "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job/upload-candidate-resume/69674859ded556ac902a8424"
# payload = {'first_name': 'Ehiremen', 'last_name': 'Abulu', 'email': 'fake-for-warden-pcf_100226@fake-domain.com', "file": open("/home/asim/Downloads/job_wise_resumes/job_1393/app_pcf_1393_100226_0.pdf", "rb")}
# response = requests.post(url, data=payload)
# print("Status Code:", response.status_code)
# print("Response Text:", response.json())


# import requests
#
# url = "https://deinqa.infosiphon.com/dein-api/deincore/partner/jobs/standalone/apply-job/upload-candidate-resume/"
#
# data = {
#     "first_name": "Noman",
#     "last_name": "iqbal",
#     "email": "fake-for-warden-pcf_100226@fake-domain.com",
# }
#
# # IMPORTANT: file must go into `files`, not `data`
# files = {
#     "file": ("app_pcf_1393_100226_0.pdf",
#              open("/home/asim/Downloads/job_wise_resumes/job_1393/app_pcf_1393_100226_0.pdf", "rb"),
#              "application/pdf")
# }
#
# headers = {
#     "Accept": "application/json",
#     # "Authorization": "Bearer YOUR_TOKEN",   # if needed
# }
#
# resp = requests.post(url, data=data, files=files, headers=headers)
#
# print("Status Code:", resp.status_code)
# print("Response Text:", resp.text)
#
# # If server returns JSON
# try:
#     print("JSON:", resp.json())
# except Exception:
#     pass
# from pymongo import MongoClient
# uri="mongodb://$de:$de@mongodb:27017/ats"
#
# client = MongoClient(uri)
# print("Databases:", client.list_database_names())
# Subscriptions = client["ats_integration"]["Subscription"].find({})
# print("Subscriptions:")
