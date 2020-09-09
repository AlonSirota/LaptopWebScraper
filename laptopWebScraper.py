import asyncio
import concurrent.futures
from time import time
import os
import csv
from typing import Any, Callable, Coroutine, List, Tuple
import aiohttp
from bs4 import BeautifulSoup, SoupStrainer
from PrioritizedJob import PrioritizedJob
import sys

myHeaders = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
             'Accept-Encoding': 'gzip, deflate, br',
             'Accept-Language': 'en,en-GB;q=0.9,en-US;q=0.8',
             'Cache-Control': 'max-age=0',
             'Connection': 'keep-alive',
             'Host': 'ksp.co.il',
             'Sec-Fetch-Dest': 'document',
             'Sec-Fetch-Mode': 'navigate',
             'Sec-Fetch-Site': 'none',
             'Sec-Fetch-User': '?1',
             'Upgrade-Insecure-Requests': '1',
             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
             '(KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
RELEVANT_TITLES = ['אחריות', 'דגם', 'זיכרון',
                   'כונן קשיח', 'משקל', 'סוללה', 'מעבד', 'מאיץ גרפי']
NUMBER_OF_CATALOG_PAGES_TO_SCRAPE = 1
BASE_URL = 'https://ksp.co.il/'
CATALOG_URL = BASE_URL + \
    '?select=.268..271..211..5870..2640..347..1798..4517..442..2639.&kg=&list=' # after the '=' should come the page number
POST_PAGE_NUMBER_SUFFIX = \
    """&sort=2&glist=0&uin=0&txt_search=&buy=&minprice=0&maxprice=0&intersect=&rintersect=&store_real="""




def run_with_process_pool(executor, function, *args) -> Coroutine[concurrent.futures.Executor, Callable, List[Any]]:
    """Uses a concurrent.futures.ProcessPoolExecuter to run the parameter 'function'
    with parameters 'args' with a process pool, in parralel.
    returns an awaitable task representing the 'function' running in parralel.

    Args:
        executor (concurrent.futures.ProcessPoolExecuter): executes the process pool
        function (typing.Callable): function to run in parralel
        *args: parameters to pass to function

    Returns:
        typing.Coroutine[concurrent.futures.Executor, typing.Callable, typing.List[typing.Any]]: awaitable task, returns the return of 'function'
    """
    event_loop = asyncio.get_event_loop()
    return event_loop.run_in_executor(executor, function, *args)


async def get_html_from_URL(URL, session) -> Tuple[str, str]:
    """uses aiohttp session to get the html in URL

    Args:
        URL (String): URL to get html of
        session (NoneType): aiohttp session, used to send a request

    Returns:
        typing.Tuple[str, str]: (html, URL)
    """
    async with session.get(URL, headers=myHeaders) as resp:
        html = await resp.text()
        assert resp.status == 200
        return (html, URL)


def scrape_hrefs_from_catalog_html(raw_html) -> List:
    """scrape html for all hrefs of laptops for sale and return them
    
    Example of 'linestitle' tag content (there's one for each laptop on page):

    <div class="linestitle">
        <a href="<LAPTOP URL HERE>">
        </a>
    </div>

    Args:
        raw_html (String): the catalog's html

    Returns:
        typing.List: the laptop hrefs
    """
    page_soup = BeautifulSoup(raw_html, "lxml")
    linestitle_tags = page_soup.find_all(class_='linestitle')
    hrefs = []
    for linestitle in linestitle_tags:
        a = linestitle.find('a') # find next <a> tag
        href = a['href'] # get 'href' attribute of tag
        hrefs.append(href)
        print(href)
    return hrefs


async def scan_catalog(catalog_link, multiprocessing_job_queue, http_session):
    """extracts laptop urls from catalog_link's html, 
    requests their html
    extracts and returns their specs.
    uses multiprocessing_job_queue to do cpu blocking jobs with mulitprocessing

    Args:
        catalog_link (String): URL of catalog page
        multiprocessing_job_queue (asyncio.PriorityQueue): Jobs inserted will be run with a process pool
        http_session (NoneType): used when making http requests

    Returns:
        List[List[String]]: specs of laptops found in catalog page
    """
    print("requesting catalog: " + catalog_link)  # Debug print

    catalog_html, _ = await get_html_from_URL(catalog_link, http_session)

    # set job as high priority because this job is a potential bottleneck
    job = PrioritizedJob(PrioritizedJob.HIGH_PRIORITY, scrape_hrefs_from_catalog_html, catalog_html)

    # Enqueue the job, worker loop will run it
    multiprocessing_job_queue.put_nowait(job)
    # wait for worker to run this job
    laptop_links = await job.future

    get_laptop_html_tasks = []
    for url in laptop_links:
        # request laptop's html
        get_laptop_html_tasks.append(
            asyncio.create_task(
                get_html_from_URL(BASE_URL + url, http_session)))

    laptops_specs_futures = []
    for get_html_task in asyncio.as_completed(get_laptop_html_tasks):
        (laptop_html, laptop_url) = await get_html_task

        # add job to queue, low priority because it doesn't bottleneck
        scrape_laptop_job = PrioritizedJob(PrioritizedJob.LOW_PRIOTITY, scrape_laptop, laptop_html, laptop_url)
        multiprocessing_job_queue.put_nowait(scrape_laptop_job)

        laptops_specs_futures.append(scrape_laptop_job.future)

    laptop_specs = []
    for specs_future in asyncio.as_completed(laptops_specs_futures):
        laptop_specs.append(await specs_future)

    return laptop_specs


def scrape_laptop(raw_html, link):
    """Scrape data specified by 'RELEVANT_TITLES' from the raw_html
    the price is contained in the text of 'span-new-price-get-item' tag
    the specs are conainted in a 'fieldset' tag

    'fieldset' tag example: (check '<!-->' tags for my commments)
    <fieldset>
        <dl>
            <dt>
                title_example1 <!-- if matches RELEVANT_TITLES... see next dd tag ->
            </dt>
            <dd>
                data_example1 <!-- this text will be in the returned list ->
            </dd>
        </dl>
        <dl>
            <dt>
                title_example2
            </dt>
            <dd>
                data_example2
            </dd>
        </dl>
        .
        .
        .
    </fieldset>


    Args:
        raw_html (String): html to extract data from
        link (String): the html's url, used as part of the data returned

    Returns:
        List[String]: list of specs extracted from html, according to RELEVANT_DATA 
    """

    print("scraping laptop html: " + link)  # debug print

    # parse the tag cpntaining price
    price_soup = BeautifulSoup(raw_html, "lxml", parse_only=SoupStrainer(
        "span", class_="span-new-price-get-item"))
    # parse the tag containing the lap top specs
    spec_soup = BeautifulSoup(
        raw_html, "lxml", parse_only=SoupStrainer('fieldset'))

    price = price_soup.get_text()
    relevant_laptop_specs = [price, link]

    table_content = {}  # This dictionary will hold the data in the website's table

    # Extract all data from table to dictionary
    dl_tags = spec_soup.find_all('dl')
    for dl in dl_tags:
        # Stripping whitespace to prevent false negative when matching this text to RELEVENT_TITLES
        title = dl.find('dt').text.strip()
        detail = dl.find('dd').text.strip()
        table_content[title] = detail

    # filter RELEVANT_TITLES elements
    for parameter in RELEVANT_TITLES:
        relevant_laptop_specs.append(
            table_content.setdefault(parameter, "no data found"))

    return relevant_laptop_specs


async def worker(job_queue, process_executor: concurrent.futures.ProcessPoolExecutor):
    """ This worker scheduales tasks to the process pool only after the last task finished.
        Each worker occupies one core"""
    while True:
        job: PrioritizedJob = await job_queue.get()

        # execute the job with process pool
        result = await run_with_process_pool(process_executor, job.function, *job.args)

        # update the future with the result
        job.future.set_result(result)

        job_queue.task_done()


def start_workers_per_core(process_executor):
    """Initialize worker(...) tasks
    Each workers will work on a single process, to make use of each core,
    one worker for each core.

    Args:
        process_executor (concurrent.futures.ProcessPoolExecutor()): Workers initialize process jobs to this executoer

    Returns:
        [Tuple([asyncio.Task], asyncio.queues.PriorityQueue())]: Worker tasks, and job queue to send the workers jobs
    """
    job_queue = asyncio.queues.PriorityQueue()  # queue holding jobs for the workers
    workers = []
    for _ in range(os.cpu_count() - 1):
        workers.append(
            asyncio.create_task(
                worker(
                    job_queue, process_executor)
            )
        )
    return workers, job_queue


def shutdown_workers(workers):
    """cancel workers, otherwise they continue in infinite loop

    Args:
        workers ([asncyio.Task]): list of worker(...) tasks in progress
    """
    for worker_coro in workers:
        worker_coro.cancel()

def get_catalog_page_url(page_number: int) -> str:
    """Calculate the URL for a certain numbered catalog page
    example of URL: https://ksp.co.il/?select=.268..271.&kg=&list=2 <-
    The GET parameter 'list' represents the page number 

    Args:
        page_number (int): the page's index number (first page is number 1)

    Returns:
        str: The numbered page's url
    """
    return CATALOG_URL + str(page_number) + POST_PAGE_NUMBER_SUFFIX

async def start_scanning_catalogs_async(job_queue, session):
    """Initiate async tasks of scan_catalog(...) for multiple urls

    Args:
        job_queue (asyncio.PriorityQueue): interface for sending jobs to workers
        session: http client 

    Returns:
        [asncio.Task]: scan_catalog(...) tasks running
    """

    catalog_scan_tasks = []
    for page_number in range(1, NUMBER_OF_CATALOG_PAGES_TO_SCRAPE + 1):
        catalog_page_url = get_catalog_page_url(page_number)

        catalog_scan_tasks.append(
            asyncio.create_task(
                scan_catalog(
                    catalog_page_url, job_queue, session)))

    return catalog_scan_tasks


def write_rows_to_csv(rows):
    # encoding: hebrew
    # newline='' prevents redundent line breaks
    with open('output.csv', 'w', encoding='windows-1255', newline='', errors='replace') as output:
        writer = csv.writer(output)
        writer.writerows(rows)


async def main():
    t = time()
    """Scrapes online laptop catalogs for laptop specifications and writes them to csv file"""
    laptop_specs = []

    # create executor for workers
    with concurrent.futures.ProcessPoolExecutor() as process_executor:
        # job_queue is used to send jobs to workers
        workers, job_queue = start_workers_per_core(process_executor)
        
        async with aiohttp.ClientSession() as session:
            catalog_scan_tasks = await asyncio.create_task(start_scanning_catalogs_async(job_queue, session))

            # block until scanning tasks finish
            for laptop_specs_future in asyncio.as_completed(catalog_scan_tasks):
                laptop_specs += await laptop_specs_future

        # block until the worker's job queue is completed
        await job_queue.join()
        shutdown_workers(workers)

    table_titles = [["price", "link"] + RELEVANT_TITLES]
    data_for_csv = table_titles + laptop_specs
    write_rows_to_csv(data_for_csv)
    print (str(time() - t))

if __name__ == "__main__":
    if(sys.platform == 'win32'):
        """ On Windows, the default event loop policy, 'proactor', occasionaly causes runtime erros with the 'aiohttp' module
        switching to 'selector' policy prevents these error.
        Bug report: https://bugs.python.org/issue39232
        """
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) 
    
    asyncio.run(main(), debug=False)
