import os
import subprocess
import time
from dotenv import load_dotenv

from customized_scripts.cf_utils.R2_utils import upload_to_r2

load_dotenv()
from customized_scripts.server_utils.server_report_utils import report_crawler_task_outcome, get_next_crawling_task

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
TMP_DATA_DIR = os.path.join(os.path.dirname(__file__), "tmp_data")
if not os.path.exists(TMP_DATA_DIR):
    os.makedirs(TMP_DATA_DIR)

def build_cmd_for_crawl_account_posts(params):
    main_filepath = os.path.join(os.path.dirname(__file__), "..", "..", "main.py")
    task_id = params['task_id']
    creator_id = params['creator_id']
    save_data_path = TMP_DATA_DIR
    max_num_posts = params['max_num_posts']
    min_create_time = params['min_create_time']
    print(main_filepath)

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform dy --lt qrcode --type creator --save_data_option json --creator_id {creator_id} --get_comment false --get_sub_comment false --headless false --save_data_path {save_data_path} --only_fetch_post_metadata true --max_num_posts {max_num_posts} --min_create_time {min_create_time} --report_to_server true  --task_id {task_id}'''

    return cmd

def build_cmd_for_crawl_account_metadata(params):
    task_id = params['task_id']
    save_data_path = TMP_DATA_DIR
    creator_id = ",".join(params['creator_ids'])
    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform dy --lt qrcode --type creator_metadata --save_data_option json --creator_id {creator_id} --headless false --save_data_path {save_data_path} --report_to_server true  --task_id {task_id}'''

    return cmd

def build_cmd_for_comments_given_post(params):
    post_ids = ",".join(params['post_ids'])
    task_id = params['task_id']
    save_data_path = TMP_DATA_DIR
    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform dy --lt qrcode --type detail --save_data_option json --specified_id {post_ids} --get_comment true --get_sub_comment true --headless false  --save_data_path {save_data_path} --max_comments_count_singlenotes 10000 --report_to_server true --task_id {task_id}'''
    return cmd

def build_cmd_for_download_media(params):
    post_ids = ",".join(params['post_ids'])
    task_id = params['task_id']
    save_data_path = TMP_DATA_DIR
    media_download_proxy_http = os.getenv('MEDIA_DOWNLOAD_PROXY_HTTP')
    report_to_server = True
    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform dy --lt qrcode --type detail --save_data_option json --specified_id {post_ids} --get_media true --media_download_proxy_http "{media_download_proxy_http}" --get_comment false --get_sub_comment false --headless false  --save_data_path {save_data_path} --max_comments_count_singlenotes 0 --report_to_server {report_to_server} --task_id {task_id}'''
    return cmd

def build_cmd_for_search_keyword(params):
    keywords = ",".join(params['keywords'])
    task_id = params['task_id']
    dy_search_sort_type = params['search_sort_type'] # general, most_like, latest
    dy_search_publish_time = params['search_publish_time'] # 0, 1, 7, 180
    crawler_max_notes_count = params.get("crawler_max_notes_count", 100)
    save_data_path = TMP_DATA_DIR

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform dy --lt qrcode --type search --save_data_option json --keywords "{keywords}" --crawler_max_notes_count {crawler_max_notes_count} --dy_search_sort_type {dy_search_sort_type} --dy_search_publish_time {dy_search_publish_time} --get_comment false --get_sub_comment false --headless false  --save_data_path {save_data_path} --report_to_server true --task_id {task_id}'''
    return cmd


def sync_media_to_CF(params):
    save_data_path = TMP_DATA_DIR

    for post_id in params['post_ids']:
        images_media_dir = os.path.join(save_data_path, "douyin", "images", post_id)
        print ("images_media_dir: {}".format(images_media_dir))
        if os.path.exists(images_media_dir):
            for root, dirs, filenames in os.walk(images_media_dir):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    object_key = "dy_content/{}/{}_{}".format(post_id, post_id, filename)
                    print ("uploading {} ===> {}".format(file_path, object_key))
                    upload_to_r2(object_key, file_path, {}, max_num_retry=1)

        videos_media_dir = os.path.join(save_data_path, "douyin", "videos", post_id)
        if os.path.exists(videos_media_dir):
            for root, dirs, filenames in os.walk(videos_media_dir):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    object_key = "dy_content/{}/{}_{}".format(post_id, post_id, filename)
                    print ("uploading {} ===> {}".format(file_path, object_key))
                    upload_to_r2(object_key, file_path, {}, max_num_retry=1)


def execute_dy_task(params):
    task_id = params["task_id"]
    task_type = params["task_type"]

    cmd = None
    if task_type == "crawl_account_posts":
        cmd = build_cmd_for_crawl_account_posts(params)
    elif task_type == "crawl_account_metadata":
        cmd = build_cmd_for_crawl_account_metadata(params)
    elif task_type == "crawl_comments_specific_post":
        cmd =  build_cmd_for_comments_given_post(params)
    elif task_type == "search":
        cmd =  build_cmd_for_search_keyword(params)
    elif task_type == "download_media":
        cmd =  build_cmd_for_download_media(params)

    if not cmd:
        print ("ERROR: failed to recognize task type {}".format(task_type))
        return

    print ("executing command: {}".format(cmd))
    is_success = False
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            check=False,
            encoding='utf-8',
            errors='ignore',
            timeout=3600,  # 60 minutes
        )
        log_lines = []
        if result.stdout:
            log_lines.append(result.stdout)
        if result.stderr:
            log_lines.append(result.stderr)
        log_output = "\n".join(log_lines) if log_lines else ""
        print ("log_output: {}".format(log_output))

        # if "comments have all been obtained and filtered" in log_output:
        #     is_success = True
        if "Douyin Crawler finished" in log_output:
            is_success = True
    except subprocess.TimeoutExpired as e:
        log_output = (
            f"[TIMEOUT] Process exceeded 10 minutes and was terminated.\n"
            f"Partial stdout: {e.stdout or ''}\n"
            f"Partial stderr: {e.stderr or ''}"
        )
        print(log_output)
    except (OSError, subprocess.SubprocessError) as e:
        log_output = f"[ERROR] Subprocess failed: {type(e).__name__}: {e}"
        print(log_output)
    except Exception as e:
        log_output = f"[ERROR] Unexpected error: {type(e).__name__}: {e}"
        print(log_output)

    # Push the download files to cloud storage
    if task_type == "download_media":
        sync_media_to_CF(params)

    # report the task to the server
    error = ""
    if not is_success:
        error = log_output
    report_crawler_task_outcome(is_success, error, "douyin", task_id)

    #=================================
    print("Done")
    # print(log_output)
    return is_success

if __name__ == '__main__':
    last_search_task_ts = 0

    while 1:
        task_info = get_next_crawling_task("douyin")
        print ("task_info: {}".format(task_info))
        if task_info is None or len(task_info) == 0:
            print ("{}: No task from the server, wait for 60s....".format(int(time.time())))
            # wait 1 min before get next task
            time.sleep(60)
            continue

        is_success = execute_dy_task(task_info)
        # wait 5s before each tasks
        wait_time = 60
        if task_info["task_type"] == "search":
            time_since_last_search = int(time.time() - last_search_task_ts)
            wait_time = max(120, 240 - time_since_last_search)
            time_since_last_search = int(time.time())
        print("{}: Completed task with status {}, wait for {}s before start next ask".format(int(time.time()), is_success, wait_time))
        time.sleep(wait_time)


    # params = {
    #     "creator_id": "MS4wLjABAAAAAm9qIeDDdB0-uO4XMe4SsYnXNaOwKMPtRmmx3cqxFyE",
    #     "max_num_posts": 20,
    #     "min_create_time": 0
    # }
    # build_cmd_for_crawl_account(params)
    #
    # ========
    # params = {
    #     "post_ids": ["7620322771906065318"],
    #     "min_create_time": 0,
    #     "task_type": "download_media",
    #     "task_id": "test_download_url"
    # }
    # params = {
    #     "post_ids": ["7618945593016077606"],
    #     "task_type": "download_media",
    #     "task_id": "test_download_url"
    # }
    # # sync_media_to_CF(params)
    # # exit()
    # execute_dy_task(params)
    # # cmd = build_cmd_for_download_media(params)
    # # print (cmd)
    # exit(0)
    # execute_dy_task(params)
    # params = {
    #     "task_id": "test_creator_metadata",
    #     "creator_ids": [
    #         "MS4wLjABAAAA6so2nqIyALknUhGMiMPB64ZyROQqdP9NfYzMCu2VRG3XEePSX3nulInehsSOCqtt",
    #         "MS4wLjABAAAADF4-IXU8jZmuZSBeEw13MzdsQ5O1KwOjVjccIjIePhGVokQjRH6DZQuM3sNDwwBt"
    #     ]
    # }
    # cmd = build_cmd_for_crawl_account_metadata(params)
    # print (cmd)
    # exit(0)
    # params = {
    #     "keywords": ["考研英语"],
    #     "task_type": "search",
    #     "task_id": "test_task",
    #     "search_sort_type": "most_like",
    #     "search_publish_time": 7
    # }
    # cmd = build_cmd_for_search_keyword(params)
    # print (cmd)
    # execute_dy_task(params)