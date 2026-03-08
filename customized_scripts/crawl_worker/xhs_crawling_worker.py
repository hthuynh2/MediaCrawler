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


def build_cmd_for_crawl_account_posts(params):
    main_filepath = os.path.join(os.path.dirname(__file__), "..", "..", "main.py")
    task_id = params['task_id']
    creator_id = params['creator_id']
    if "https://" not in creator_id:
        creator_id = "https://www.xiaohongshu.com/user/profile/{}".format(creator_id)
    save_data_path = TMP_DATA_DIR
    crawler_max_notes_count = params['max_num_posts']

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform xhs --lt qrcode --type creator --save_data_option json --creator_id {creator_id} --get_comment false --get_sub_comment false --headless false --save_data_path {save_data_path} --crawler_max_notes_count {crawler_max_notes_count} --report_to_server true  --task_id {task_id}'''

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

    #   GENERAL = "general"
    #     # Most popular
    #     MOST_POPULAR = "popularity_descending"
    #     # Latest
    #     LATEST = "time_descending"
    sort_type = params['search_sort_type'] # general, popularity_descending, time_descending
    xhs_min_upvotes_search_filter = params['xhs_min_upvotes_search_filter'] # general, popularity_descending, time_descending

    save_data_path = TMP_DATA_DIR
    crawler_max_notes_count = params['max_num_posts']

    cmd = f'''cd {PROJECT_DIR} && uv run python main.py --platform xhs --lt qrcode --type search --save_data_option json --keywords {keywords} --sort_type {sort_type}  --crawler_max_notes_count {crawler_max_notes_count} --xhs_min_upvotes_search_filter {xhs_min_upvotes_search_filter} --get_comment false --get_sub_comment false --headless false  --save_data_path {save_data_path} --report_to_server true --task_id {task_id}'''
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

    # report the task to the server
    error = ""
    if not is_success:
        error = log_output
    report_crawler_task_outcome(is_success, error, task_id)

    #=================================
    print("Done")
    print(log_output)
    return is_success

if __name__ == '__main__':
    while 1:
        task_info = get_next_crawling_task("xhs")
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

