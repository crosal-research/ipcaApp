import requests, json, time

data = {"date": "2021-08-01", "indicator": "IPCA"}
url = "http://127.0.0.1:5000/inflation/api/v0.1/"

t0 = time.time()
resp = requests.post(url, data=json.dumps(data))
print(resp.text)
print(f"Time to download: {time.time() -t0}")
