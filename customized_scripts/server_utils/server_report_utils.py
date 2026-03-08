from dotenv import load_dotenv
import os
import logging

import requests

load_dotenv()

logger = logging.getLogger(__name__)

SERVER_URL = os.getenv("SERVER_URL")
SERVER_API_KEY = os.getenv("SERVER_API_KEY")

def send_request_to_server(api_endpoint, payload):
    print ("Sending request to server endpoint: {}".format(api_endpoint))
    print ("payload: {}".format(payload))
    if not SERVER_URL or not SERVER_URL.strip():
        logger.warning("SERVER_URL is not set, skipping report_post_data_to_server")
        return False

    url = f"{SERVER_URL.rstrip('/')}{api_endpoint}"
    headers = {"Content-Type": "application/json"}
    if SERVER_API_KEY:
        headers["Authorization"] = f"Bearer {SERVER_API_KEY}"

    try:
        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        logger.debug("report_post_data_to_server success: %s %s", url, resp.status_code)
        return resp.json()
    except requests.RequestException as e:
        logger.warning("report_post_data_to_server failed: %s", e)
        return None

def report_post_data_to_server(post_data, platform, task_id=None):
    cleaned_post_data = []
    for post_info in post_data:
        cleaned_info = {
            "aweme_id": post_info["aweme_id"],
            "desc": post_info["desc"],
            "create_time": post_info["create_time"],
            "comment_count": post_info["statistics"]["comment_count"],
            "upvote_count": post_info["statistics"]["digg_count"],
            "collect_count": post_info["statistics"]["collect_count"],
            "creator_sec_uid": post_info["author"]["sec_uid"],
            "creator_nickname": post_info["author"]["nickname"],
        }
        cleaned_post_data.append(cleaned_info)
        print (cleaned_info)

    payload = {"post_data": cleaned_post_data, "platform": platform, "task_id": task_id}
    return send_request_to_server("/api/sync_post_data", payload)

def report_comments_data_to_server(comment_data, platform, task_id=None):
    payload = {"comment_data": comment_data, "platform": platform, "task_id": task_id}
    return send_request_to_server("/api/sync_comment_data", payload)

def report_crawler_task_outcome(is_success, error, task_id=None):
    payload = {"is_success": is_success, "error": error, "task_id": task_id}
    return send_request_to_server("/api/report_crawler_task_outcome", payload)

def get_next_crawling_task(platform):
    payload = {"platform": platform}
    return send_request_to_server("/api/get_next_crawling_task", payload)

if __name__ == '__main__':
    # post_data = []
    # platform = "bili"
    # report_post_data_to_server(post_data, platform)
    # report_comments_data_to_server(post_data, platform)
    #
    # report_crawler_error("error")
    # get_next_crawling_task(platform)
    pass