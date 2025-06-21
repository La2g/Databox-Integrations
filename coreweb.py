import datetime
import requests
import databox
from databox import ApiException

# Constants
PAGESPEED_API_KEY = ""
DATABOX_TOKEN = ""
URL = "https://databox.com"
STRATEGIES = ["desktop", "mobile"]

configuration = databox.Configuration(
    host="https://push.databox.com",
    username=DATABOX_TOKEN,
    password=""
)


def fetch_core_web_vitals(url, strategy):
    """Fetch Core Web Vitals from Google's PageSpeed Insights API."""
    endpoint = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {
        "url": 'https://databox.com',
        "strategy": strategy,
        "key": PAGESPEED_API_KEY,
    }

    response = requests.get(endpoint, params=params)
    data = response.json()

    if data["loadingExperience"]["metrics"]:
        metrics = data["loadingExperience"]["metrics"]
        lcp = metrics.get("LARGEST_CONTENTFUL_PAINT_MS", {}).get("percentile", 0) / 1000
        fid = metrics.get("FIRST_INPUT_DELAY_MS", {}).get("percentile", 0)
        cls = metrics.get("CUMULATIVE_LAYOUT_SHIFT_SCORE", {}).get("percentile", 0)
        return {"LCP": lcp, "FID": fid, "CLS": cls}
    else:
        return {"LCP": None, "FID": None, "CLS": None}


def main():
    push_data = []
    metrics_data = []
    for strategy in STRATEGIES:
        metrics = fetch_core_web_vitals(URL, strategy)
        if metrics:
            metrics_data.append({
                "url": URL,
                "strategy": strategy,
                "LCP": metrics["LCP"],
                "FID": metrics["FID"],
                "CLS": metrics["CLS"],
            })

    for i in range(0, 2):
        push_data.append(
            {
                "key": "LCP",
                "date": datetime.datetime.now().strftime(format='%Y-%m-%d'),
                "value": metrics_data[i]['LCP'],
                "attributes": [{"key": "strategy", "value": metrics_data[i]['strategy']}]
            })
        push_data.append(
            {
                "key": "FID",
                "date": datetime.datetime.now().strftime(format='%Y-%m-%d'),
                "value": metrics_data[i]['FID'],
                "attributes": [{"key": "strategy", "value": metrics_data[i]['strategy']}]
            })
        push_data.append(
            {
                "key": "CLS",
                "date": datetime.datetime.now().strftime(format='%Y-%m-%d'),
                "value": metrics_data[i]['CLS'],
                "attributes": [{"key": "strategy", "value": metrics_data[i]['strategy']}]
            })

    # It's crucial to specify the correct Accept header for the API request
    with databox.ApiClient(configuration, "Accept", "application/vnd.databox.v2+json", ) as api_client:
        api_instance = databox.DefaultApi(api_client)

        try:
            api_instance.data_post(push_data=push_data)
        except ApiException as e:
            # Handle exceptions that occur during the API call, such as invalid data or authentication issues
            print("API Exception occurred: %s\n" % e)
        except Exception as e:
            # Handle any other unexpected exceptions
            print("An unexpected error occurred: %s\n" % e)


if __name__ == "__main__":
    main()
