import asyncio
from datetime import datetime, timedelta
import multiprocessing
from multiprocessing import cpu_count
import aiohttp
import aiofiles
import concurrent.futures
from math import floor
import json
import sys
import functools
import pandas as pd
import os
import time

# workround for asyncio event loop issues on windows
# from https://github.com/encode/httpx/issues/914
if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def request_new_token(connparams, apikey):
    """Requests new token

    Args:
        connparams (dict): dictionary containing "client_id", "email", "name" values
        apikey (multiprocessing.Management dictionary): dictionary containing api token and validity timestamp
    """
    
    # request new token
    async with aiohttp.ClientSession() as client_session:
        body = {
            "client_id" : connparams["client_id"],
            "email" : connparams["email"],
            "name" : connparams["name"]
            }

        async with client_session.post(url=connparams["authurl"],data=body) as response:
            token_response = await response.text()
            token_response_json = json.loads(token_response)
            apikey['token'] = token_response_json["data"]["sl_token"]
            apikey['isvalid'] = datetime.now() + timedelta(minutes=59)


def renew_access_token(func):
    """Decorator function for renewing access token

    Args:
        func (function): input function

    Returns:
        function: a wrapper for the input function
    """
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):        
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            lock = args[0]
            apikey = args[1]
            connparams = args[2]

            # Get a new token
            ## Acquire the lock so other processes do not request ne token at the same time
            lock.acquire()
            
            ## First process to execute this will execute this, others will skip
            if datetime.now() > apikey["isvalid"]:
                print(f"Exception {e}: Unable to do the request, requesting new token")
                await request_new_token(connparams, apikey)
            
            lock.release()
            
            # once the token is refreshed, retry the operation
            return await func(*args, **kwargs)
    
    return wrapper


def analyze_posts_in_response(resp: dict):
    """Creates summary statistics for the given http response json.

    Args:
        resp (dict): http response json

    Returns:
        tuple of pandas.DataFrames: four data frames with summary statistics
    """

    posts = pd.DataFrame(resp["data"]["posts"])

    posts["len"] = posts["message"].map(len)

    posts["created_time"] = posts["created_time"].map(pd.to_datetime)
    posts["day"]=posts["created_time"].map(lambda x: x.day)
    posts["week"]=posts["created_time"].map(lambda x: x.isocalendar().week)
    posts["month"]=posts["created_time"].map(lambda x: x.month_name())
    posts["year"]=posts["created_time"].map(lambda x: x.year)


    max_length_per_month = posts.groupby(["year","month"])["len"].max().reset_index().rename(columns={"len":"max len"})
    mean_length_per_month = posts.groupby(["year", "month"])["len"].mean().reset_index().rename(columns={"len":"mean len"})
    total_posts_per_week_number = posts.groupby(["year","week"])["message"].count().reset_index().rename(columns={"message":"total posts"})
    total_posts_per_user_per_month = posts.groupby(["from_name","year", "month"])["message"].count().reset_index().rename(columns={"message":"total posts"})

    return (max_length_per_month, mean_length_per_month, total_posts_per_week_number, total_posts_per_user_per_month)

def generate_statistics(max_df, mean_df, tot_mon_df, tot_usr_mon_df):
    """_summary_

    Args:
        max_df (pandas.DataFrame): data frame with summary statistics
        mean_df (pandas.DataFrame): data frame with summary statistics
        tot_mon_df (pandas.DataFrame): data frame with summary statistics
        tot_usr_mon_df (pandas.DataFrame): data frame with summary statistics

    Returns:
        pandas.DataFrame tuple: four data frames with summary statistics
    """
    a = max_df.groupby(["year","month"])["max len"].max().reset_index()
    b = mean_df.groupby(["year","month"])["mean len"].mean().reset_index()
    c = tot_mon_df.groupby(["year","week"])["total posts"].sum().reset_index()
    d = tot_usr_mon_df.groupby(["from_name"])["total posts"].mean().reset_index().rename(columns={"total posts":"average posts per month"})

    return (a, b, c, d)


@renew_access_token
async def process_page(lock, apikey, connparams: dict, page_id: int, output_file_base: str):
    """Makes asynchronous HTTP request, downloads response content into a file and returns a set of summary statistics

    Args:
        lock (multiprecessing.Management Lock): A lock to share between processes
        apikey (multiprocessing.Management dictionary): dictionary containing api token and validity timestamp
        connparams (dict): dictionary containing "client_id", "email", "name" values
        page_id (int): id of the page to fetch
        output_file_base (str): template of thf file name in which to store response

    Returns:
        tuple of pandas.DataFrame: four data frames with summary statistics
    """

    print(f"Processing page {page_id}")

    rawdatadir = outdir = os.path.join(os.getcwd(), "rawdata")
    if not os.path.exists(rawdatadir):
        os.makedirs(rawdatadir)

    output_file_name = rawdatadir + "/"+ output_file_base.format(page_id = page_id)
    async with aiohttp.ClientSession() as client_session, aiofiles.open(output_file_name,'w+',encoding="utf-8") as f:
        params = {
            "sl_token" : apikey["token"],
            "page" : page_id
        }
        async with client_session.get(url=connparams["url"], params=params) as response:
            page_response = await response.text()
            page_response_json = json.loads(page_response)
            analysis = analyze_posts_in_response(page_response_json)
            await f.write(page_response + "\n")
    
    print(f"Processing page {page_id} completed")

    return analysis


async def process_page_group(lock, apikey, connparams: dict, min_id: int, max_id: int, file_base: str):
    """Processes a range of pages

    Args:
        lock (multiprecessing.Management Lock): A lock to share between processes
        apikey (multiprocessing.Management dictionary): dictionary containing api token and validity timestamp
        connparams (dict): dictionary containing "client_id", "email", "name" values
        min_id (int): minimum id of the page to fetch
        max_id (int): maximum id of the page to fetch
        file_base (str): template of thf file name in which to store response
    """
    tasks = []
    for id in range(min_id,max_id+1):
        tasks.append(process_page(lock, apikey, connparams, id, file_base))
    res = await asyncio.gather(*tasks)
    return res


def process_page_group_async_wrapper(lock, apikey, connparams:dict, min_id: int, max_id: int, file_base: str):
    """A wrapper for asynchronously executing process_page_group function 

    Args:
        lock (multiprecessing.Management Lock): A lock to share between processes
        apikey (multiprocessing.Management dictionary): dictionary containing api token and validity timestamp
        connparams (dict): dictionary containing "client_id", "email", "name" values
        min_id (int): minimum id of the page to fetch
        max_id (int): maximum id of the page to fetch
        file_base (str): template of thf file name in which to store response
    """
    
    res = asyncio.run(process_page_group(lock, apikey, connparams, min_id, max_id, file_base))
    return res


def process_pages_parallel(connparams, min_id, max_id, file_base):
    """Creates a set of page processing tasks, 
    distributes them accross available cpu cores, 
    collects summary statistics and stores them into a "statistics.json" file   

    Args:
        connparams (dict): dictionary containing "client_id", "email", "name" values
        min_id (int): minimum id of the page to fetch
        max_id (int): maximum id of the page to fetch
        file_base (str): template of thf file name in which to store response
    """

    with multiprocessing.Manager() as manager:
        # get server-based lock and api token variable to be shared between processes.
        lock = manager.Lock()
        apitoken = manager.dict()
        apitoken["token"] = ""
        apitoken["isvalid"] = datetime.now()

        # calculate how many pages to read per worker
        no_cpus = cpu_count()
        no_pages = max_id-min_id + 1
        pages_per_cpu = floor(no_pages/no_cpus)

        # initialize max amount of workers and allocate tasks to workers
        tasks = []
        with concurrent.futures.ProcessPoolExecutor(no_cpus) as executor:
            # allocate first no_cpu-1 tasks
            for i in range(no_cpus-1):
                new_task = executor.submit(process_page_group_async_wrapper, lock, apitoken, connparams, i*pages_per_cpu+1, i*pages_per_cpu+pages_per_cpu, file_base)
                tasks.append(new_task)
        
            # allocate the last task to the executor
            tasks.append(executor.submit(process_page_group_async_wrapper, lock, apitoken, connparams, (no_cpus-1)*pages_per_cpu+1, no_pages, file_base))

            # start workers and wait for results
            result_list_max = []
            result_list_mean = []
            result_list_tot_month = []
            result_list_tot_user_month = []
            
            for task in concurrent.futures.as_completed(tasks):
                for df_list in task.result():
                    result_list_max.append(df_list[0])
                    result_list_mean.append(df_list[1])
                    result_list_tot_month.append(df_list[2])
                    result_list_tot_user_month.append(df_list[3])
            
            result_list_max_df = pd.concat(result_list_max)
            result_list_mean_df = pd.concat(result_list_mean)
            result_list_tot_month_df = pd.concat(result_list_tot_month)
            result_list_tot_user_month_df = pd.concat(result_list_tot_user_month)

            a,b,c,d = generate_statistics(result_list_max_df, result_list_mean_df, result_list_tot_month_df, result_list_tot_user_month_df)
            res_dict = {
                "maximum length per month": json.loads(a.to_json(orient="records")),
                "average length per month": json.loads(b.to_json(orient="records")),
                "total posts by week number": json.loads(c.to_json(orient="records")),
                "average posts per user per month": json.loads(d.to_json(orient="records"))
            }

            # TODO: move file path to configuration.json
            outdir = os.path.join(os.getcwd(), "output")
            if not os.path.exists(outdir):
                os.makedirs(outdir)

            with open(outdir + '/statistics.json', 'w') as fp:
                json.dump(res_dict, fp, indent=2)
 

def read_configuration():
    """Reads "configuration.json" file into a json object

    Returns:
        dict: json object with configuration parameters.
    """
    with open("configuration.json") as f:
        data = json.load(f)
        config = json.loads(json.dumps(data))
   
    return config

def process_pages():
    """Reads configuration from "configuration.json" and performas analysis.
    """

    # Load configuration parameters
    config = read_configuration()
    connparams = {
        "url": config["url"],
        "authurl": config["authurl"],
        "client_id": config["client_id"],
        "email": config["email"],
        "name": config["name"]
    }

    # Fetch and process the pages
    print("Starting fetching files, please wait ...")
    start = time.time()
    process_pages_parallel(connparams, config["min_page_id"], config["max_page_id"], config["output_file"])
    stop = time.time()
    print(f"Processing time: {round(stop - start, 2)} seconds.")


if __name__== "__main__":
    process_pages()
