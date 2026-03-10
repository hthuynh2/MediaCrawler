import os
import subprocess
import time

from dotenv import load_dotenv
from customized_scripts.server_utils.server_report_utils import report_crawler_task_outcome, get_next_crawling_task

load_dotenv()

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
TMP_DATA_DIR = os.path.join(os.path.dirname(__file__), "tmp_data")
if not os.path.exists(TMP_DATA_DIR):
    os.makedirs(TMP_DATA_DIR)

BILIBILI_COOKIES = os.environ.get("BILIBILI_COOKIES")

def build_cmd_for_crawl_account_posts(params):
    main_filepath = os.path.join(os.path.dirname(__file__), "..", "..", "main.py")
    task_id = params['task_id']
    creator_id = params['creator_id']
    save_data_path = TMP_DATA_DIR
    max_num_posts = params['max_num_posts']
    min_create_time = params['min_create_time']
    print(main_filepath)

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform bili --lt cookie --type creator --save_data_option json --creator_id {creator_id} --get_comment false --get_sub_comment false --cookies "{BILIBILI_COOKIES}" --headless true --save_data_path {save_data_path} --only_fetch_post_metadata true --max_num_posts {max_num_posts} --min_create_time {min_create_time} --report_to_server true  --task_id {task_id}'''

    return cmd

def build_cmd_for_search_keyword(params):
    keywords = ",".join(params['keywords'])
    task_id = params['task_id']

    #  DEFAULT = ""
    #
    #     # Most clicks
    #     MOST_CLICK = "click"
    #
    #     # Latest published
    #     LAST_PUBLISH = "pubdate"
    #
    #     # Most danmu (comments)
    #     MOST_DANMU = "dm"
    #
    #     # Most bookmarks
    #     MOST_MARK = "stow"
    bili_search_sort_type = params['search_sort_type'] # "", click, pubdate, dm, stow
    bilibili_search_start_ts = params['search_start_ts'] # ts in s (note: set to 0 to be default)
    bilibili_search_end_ts = params['search_end_ts'] # ts in s (note: set to 0 to be default)

    save_data_path = TMP_DATA_DIR
    crawler_max_notes_count = 10

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform bili --lt cookie --type search --save_data_option json --keywords {keywords} --cookies "{BILIBILI_COOKIES}" --bili_search_sort_type {bili_search_sort_type} --bilibili_search_end_ts {bilibili_search_end_ts} --bilibili_search_start_ts {bilibili_search_start_ts} --crawler_max_notes_count {crawler_max_notes_count} --bili_search_mode normal --get_comment false --get_sub_comment false --headless false  --save_data_path {save_data_path} --report_to_server true --task_id {task_id}'''
    return cmd

def execute_task(params):
    task_id = params["task_id"]
    task_type = params["task_type"]

    cmd = None
    if task_type == "crawl_account_posts":
        cmd = build_cmd_for_crawl_account_posts(params)
    elif task_type == "search":
        cmd =  build_cmd_for_search_keyword(params)

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
            timeout=600,  # 10 minutes
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
        if "Bilibili Crawler finished" in log_output:
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

    # report the task to the server
    error = ""
    if not is_success:
        error = log_output
    report_crawler_task_outcome(is_success, error, "bilibili", task_id)

    #=================================
    print("At {}: Done for task {}; is_success: {};".format(int(time.time()), task_id, is_success))
    # print(log_output)
    return is_success

if __name__ == '__main__':
    while 1:
        task_info = get_next_crawling_task("bilibili")
        print ("task_info: {}".format(task_info))
        if task_info is None or len(task_info) == 0:
            print ("{}: No task from the server, wait for 60s....".format(int(time.time())))
            # wait 1 min before get next task
            time.sleep(60)
            continue

        is_success = execute_task(task_info)
        # wait 5s before each tasks
        print("{}: Completed task with status {is_success}, wait for 10s before start next ask".format(int(time.time()),  is_success=is_success))
        time.sleep(10)