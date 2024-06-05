import os, json, time
from datetime import datetime
import requests
from openai import OpenAI
from transformers import GPT2Tokenizer
import cdx_toolkit

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from .file_utils import csv_to_dict


# Setup tokenizer for truncating long inputs
tokenizer = GPT2Tokenizer.from_pretrained("gpt2")

# Load OpenAI client
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Google Custon Search Engine ID and API Key
CSE_ID = '91b031290f09844c8'  # 'a201862aec58741ab'
CSE_API_KEY = os.getenv('GOOGLE_API_KEY_2')

# Load URL lookup table
os.makedirs('html_pages', exist_ok=True)
url_table = csv_to_dict('html_pages/url_table.csv')


def fetch_html(
    url: str, use_buffer: bool = True, cc_fetch: bool = True, browser: str = 'chrome'
):
    """ Fetch the URL and return the response
    Args:
        url (str):  The URL to fetch
        use_buffer (bool):  Whether to load from buffer. Defaults to True
        cc_fetch (bool):    Whether to fetch from Common Crawl. Defaults to True
        browser (str):      The browser to use. Defaults to 'chrome'
    Returns:
        Response:   The response object
    """
    url = url.replace(' ', '')
    new_url = url_table.get(url, url)
    new_url_noslash = new_url.replace('/', '\\')
    buffer_path = f"html_pages/pages/{new_url_noslash}.html"[:200]

    if not os.path.exists('html_pages/pages'):
        os.makedirs('html_pages/pages', exist_ok=True)

    if use_buffer and os.path.exists(buffer_path):
        # Load from buffer if the page has already been fetched
        print(f"Loaded from buffer: {url}")
        with open(buffer_path, 'r') as bfr:
            return new_url, bfr.read()

    else:
        try:
            if cc_fetch:
                print(f'Fetching from common crawl: {url}')
                cdx = cdx_toolkit.CDXFetcher(source='cc')
                timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')

                for obj in cdx.iter(
                    url, limit=1, to=timestamp, filter=['status:200', 'mimetype:text/html']
                ):
                    # Convert bytes to string
                    html = obj.content.decode('utf-8')

                assert html is not None
            else:
                print(f"Fetching with {browser}: {url}")

                # Set preferences to ensure cookies and JavaScript are enabled
                if browser == 'chrome':
                    web_driver = webdriver.Chrome()
                elif browser == 'firefox':
                    options = Options()
                    options.set_preference("javascript.enabled", True)
                    options.set_preference("network.cookie.cookieBehavior", 0)
                    web_driver = webdriver.Firefox(options=options)
                else:
                    raise ValueError(f"Invalid browser: {browser}")

                # Fetch the URL. Wait until all images are loaded and an extra .5 seconds
                web_driver.get(url)
                WebDriverWait(web_driver, timeout=10).until(
                    EC.presence_of_all_elements_located((By.TAG_NAME, 'img'))
                )
                time.sleep(0.5)

                # Save the redirected URL and the HTML content
                new_url = web_driver.current_url
                html = web_driver.page_source
                new_url = new_url.replace(' ', '')
                new_url_noslash = new_url.replace('/', '\\')

            # Save the redirected URL and HTML content to the buffer
            if use_buffer:
                if new_url != url:
                    url_table[url] = new_url
                    with open('html_pages/url_table.csv', 'a') as tbl:
                        tbl.write(f"{url}|{new_url}\n")

                with open(buffer_path, 'w') as bfr:
                    bfr.write(html)

            return new_url, html

        except Exception as e:
            print(f"Failed to fetch: {url}")
            print(f"Error message: {e}")
            return None, None

        finally:
            if 'web_driver' in locals():
                web_driver.quit()


def query_openai_llm(
    content: str, buffer_path: str, llm_model: str = 'gpt-3.5-turbo',
    use_buffer: bool = True, recursion_depth: int = 0
):
    """ Query the LLM model to check if the text is a product page
    Args:
        content (str):          The content to feed to the OpenAI LLM model
        buffer_path (str):      The path to the buffer file to store the response
        llm_model (str, optional):  The model to use. Defaults to 'gpt-3.5-turbo'
        use_buffer (bool):      Whether to skip loading from buffer. Defaults to True
    Returns:
        str:        The response from the LLM
        bool:       True if the response is loaded from buffer, False otherwise
    """
    buffer_path = buffer_path[:200]
    if not os.path.exists(os.path.dirname(buffer_path)):
        # Create the buffer directory if it does not exist
        os.makedirs(os.path.dirname(buffer_path), exist_ok=True)

    if use_buffer and os.path.exists(buffer_path):
        # Load from buffer if the response has already been fetched
        from_buffer = True
        with open(buffer_path, 'r') as file:
            response = file.read()
    else:
        try:
            from_buffer = False
            # Query the LLM model if the response is not found in the buffer
            chat_completion = openai_client.chat.completions.create(
                messages=[{"role": "user", "content": content}], model=llm_model,
            )
            response = chat_completion.choices[0].message.content
            with open(buffer_path, 'w') as file:
                file.write(response)

        except Exception as e:
            if "maximum context length" in str(e):
                # If the input is too long, truncate and retry
                print(f"Input too long. Truncating input to OpenAI LLM.")
                inputs = tokenizer(
                    content, return_tensors="pt", max_length=16385, truncation=True
                )
                content_tr = tokenizer.decode(inputs['input_ids'][0])
                if recursion_depth < 1:
                    return query_openai_llm(
                        content_tr, buffer_path, llm_model,
                        use_buffer, recursion_depth + 1
                    )

            print(f"Failed to query OpenAI LLM: {e}")
            response, from_buffer = None, False

    return response, from_buffer


def query_google_cse(query: str, verbose: bool = True):
    """ Query the Google Custom Search API for a given product, brand, and model
    Args:
        query (str):    The query to search
    Returns:
        dict:           The JSON response from the Google Custom Search API
    """
    # Google Custom Search endpoint
    cse_url = "https://www.googleapis.com/customsearch/v1"
    params = {'key': CSE_API_KEY, 'cx': CSE_ID, 'q': query}
    buffer_path = f"google_responses/{query}"[:200]

    if os.path.exists(buffer_path):
        # If the query has already been searched, return the saved result
        with open(buffer_path, 'r') as file:
            results = json.load(file)
            if "error" not in results.keys():
                print(f"Google search results loaded from buffer: {query}")
                return results

    # Otherwise, make a Google Search API request to fetch results
    response = requests.get(cse_url, params=params)
    results = response.json()

    if response.status_code in [403, 429]:
        print(f"**** Google API rate limit exceeded ({response.status_code}). ****")
    elif response.status_code != 200:
        print(f"**** Google API request failed ({response.status_code}). ****")
    elif verbose:
        print(f"Google API request successful for {query}.")
        try:
            with open(buffer_path, 'w') as file:
                json.dump(results, file, indent=4)
        except:
            print(f"**** Failed to save the Google API response. ****")

    return results
