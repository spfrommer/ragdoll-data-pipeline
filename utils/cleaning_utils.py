from bs4 import BeautifulSoup
from unidecode import unidecode
import time

from .keywords import PRODUCT_KEYWORDS, GOOG_DISCARD_KEYWORDS, UNAVAILABLE_KEYWORDS
from .query_utils import fetch_html, query_openai_llm, query_google_cse


def check_with_llm(
    prod: str = None, brand: str = None, model: str = None,
    title: str = None, url: str = None, text: str = None,
    query_type: str = 'check_product_page', llm_model: str = 'gpt-3.5-turbo'
):
    """ Query the LLM model to check if the text is a product page
    Args:
        prod (str):     The product name to search for in the catalog page
        brand (str):    The product brand name
        model (str):    The product model name
        title (str):    The title of the website
        url (str):      The URL of the website
        text (str):     The raw text extracted from the website
        query_type (str, optional):
            The type of query to perform. Defaults to 'check_product_page'.
        llm_model (str, optional):
            The model to use. Defaults to 'gpt-3.5-turbo'
    Returns:
        str:            The response from the LLM
    """
    if query_type == 'check_product_page':
        content = \
            "You will be given the raw text extracted from a webpage. " \
            "Your goal is to determine if this page is likely an " \
            f"OFFICIAL product page that contains a SINGLE {prod} product. " \
            f"If the page is likely an official SINGLE product page for {prod}, " \
            "return 'True'. If this page is likely a catalog page or a lineup " \
            "introduction page where multiple products (or a product family) " \
            "are listed, say 'False' and give me the name of one of the " \
            f"listed {prod} products after a line break. If this page does " \
            f"not exist, or is a non-official third-party site, or is unlikely a " \
            f"{prod} product page (or not a product page at all), return 'False'." \
            f"\nThe page is: " + text
    if query_type == 'update_model_name':
        content = \
            f"Consider a webpage, whose title is {title}, and the URL is {url}. " \
            f"This is a product webpage for a {prod} from the brand {brand}. " \
            f"The product model name could be {model}, but this may be incorrect. " \
            "Your goal is to determine the actual model name of this product based " \
            "on the provided information. Your output should be the model name, " \
            "followed by the reasoning of the answer after a line break. Note that " \
            "the first line of your answer should only mention the model name " \
            "(include the product series of applicable), and should not include " \
            "the brand name or the type of product (i.e., don't explicitly " \
            f"mention '{prod}'). If the title is uninformative and you are unable " \
            "to decide based on the title and the URL, say 'title uninformative'."
    elif query_type == 'check_url':
        query_url = url.split('://')[-1].split('.html')[0]
        content = \
            f"Here is a URL: {query_url}.\nDetermine if it likely " \
            f"points to an OFFICIAL product page that contains a single {prod} " \
            f"product. If the page is likely an official single product page for a " \
            f"{prod}, return 'True' and say the reason after a line break. " \
            "If you are VERY certain that this URL points to a non-official " \
            f"third-party site or is not for a {prod}, return 'False' and say the " \
            "reason after a line break. If you are VERY certain that this URL " \
            "points to an official catalog page or a lineup introduction page, " \
            "return 'Catalog' and say the reason after a line break. " \
            "If you are not sure, say 'Unsure'."
    elif query_type == 'check_url_brand':
        content = \
            f"Does a URL beginning with {url} likely point to an official website " \
            f"of the brand {brand}? Return 'True' or 'False' and no extra words."
    elif query_type == 'find_in_catalog':
        content = \
            "You will be given the raw text extracted from a product catalog page. " \
            f"Return the name of one single {prod} product in this page " \
            f"without any extra words.\nThe page is: " + text
    else:
        raise ValueError(f"Invalid LLM query type: {query_type}.")

    # Specify whether to activate the buffer for the query
    use_buffer = True  # query_type != 'update_model_name'
    buffer_path = f"llm_responses/{query_type}/{url.replace('/', '\\')}.html"
    response, from_buffer = query_openai_llm(
        content, buffer_path, llm_model=llm_model, use_buffer=use_buffer
    )

    # Retry if the response is None, which means the query failed
    max_retry, retry_left = 5, 5
    while response is None and max_retry > 0:
        time.sleep(1.5 ** (max_retry - retry_left))
        response, from_buffer = query_openai_llm(
            content, buffer_path, llm_model=llm_model, use_buffer=use_buffer
        )
        retry_left -= 1
    assert response is not None, f"Failed to query LLM for {query_type}: {url}."

    buffer_status = 'from buffer' if from_buffer else 'fresh request'
    print(f"OpenAI LLM {query_type} response ({buffer_status}) "
          f"to {url}:\n{response.replace('\n\n', ' ')}")
    if query_type != 'update_model_name':
        response = response.lower()
    return response.replace("'", '').strip()


def is_product(text: str, html: str, prod: str, url: str, brand: str):
    """ Check the website text to see whether it is likely an offical product page
    Args:
        text (str):  The stripped text extracted from the website
        html (str):  The raw HTML content of the website
        prod (str):  The product name to search for in the catalog page
        url (str):   The URL of the website
        brand (str): The product brand name
    Returns:
        bool:        True if the text is likely a product page, False otherwise
        bool:        True if the product is unavailable, False otherwise
        str:         A single product name if the page is a catalog page
    """
    if url[-1] == '/': url = url[:-1]  # Remove trailing slash
    model_from_catalog = None

    # Check if any of the product-related keywords appear in the text
    # Amazon makes products itself and thus should be treated separately
    is_prod = '/' in url.split('://')[-1] and \
        any([pkw in text.lower() for pkw in PRODUCT_KEYWORDS]) and \
        all([gkw not in url.lower() for gkw in GOOG_DISCARD_KEYWORDS]) and \
        ('amazon' in brand.lower() or 'amazon' not in url.lower())

    # Check if any of the unavailable keywords appear in the text
    ukw_counts = {ukw: text.lower().count(ukw) for ukw in UNAVAILABLE_KEYWORDS}
    found_ukw = [ukw for ukw in UNAVAILABLE_KEYWORDS if ukw_counts[ukw] > 0]
    total_ukw = sum([ukw_counts[ukw] for ukw in UNAVAILABLE_KEYWORDS])
    unavailable = len(found_ukw) > 0 and total_ukw > 0
    if unavailable:
        print(f"Unavailable keywords {found_ukw} found "
              f"{[ukw_counts[w] for w in found_ukw]} times for {url}")

    # Query LLM to check if the URL likely points to an official product page
    if is_prod and not unavailable:
        response = check_with_llm(
            prod=prod, url=url, query_type='check_url', llm_model='gpt-3.5-turbo'
        ).split(' ')[0]
        if response.split('\n')[0] == 'false':      # Unlikely official product page
            is_prod = False
        elif response.split('\n')[0] == 'catalog':  # Likely a catalog page
            response = check_with_llm(
                prod=prod, text=text, url=url, query_type='find_in_catalog',
                llm_model='gpt-3.5-turbo'
            )
            model_from_catalog = response[:-1] if response[-1] == '.' else response

    # Query LLM to check whether the text is likely from a product page
    if is_prod and not unavailable and model_from_catalog is None:
        response = check_with_llm(
            prod=prod, text=text, url=url, query_type='check_product_page',
            llm_model='gpt-3.5-turbo'
        )
        if response == 'false':     # Likely not a product page at all
            is_prod = False
        elif response != 'true':    # Likely a catalog page
            split_res = response.split('\n')
            if len(split_res) > 1:
                model_from_catalog = split_res[1]
            else:
                is_prod = False

    return is_prod, unavailable, model_from_catalog


def fetch_and_check(prod: str, url: str, brand: str, verbose: bool = True):
    """ Fetch the URL and check if it is a product page
    Args:
        prod (str):     The product name to search for in the catalog page
        url (str):      The URL to fetch
        brand (str):    The product brand name
        verbose (bool, optional): If True, print details. Defaults to True
    Returns:
        bool:       True if the text is likely a product page, False otherwise
        bool:       True if the product is unavailable, False otherwise
        str:        A single product name if the page is a catalog page
    """
    if url is None: return None, False, False, None
    url = url.replace(' ', '')
    if url.endswith('.pdf'): return url, False, False, None

    # Fetch the URL
    url, html = fetch_html(url, cc_fetch=False, browser='firefox')
    if html is None: return url, False, False, None

    # Extract text from the response
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text()
    html = soup.prettify()  # Optionally prettify the HTML content
    if verbose:  # Print the first 30 words of the extracted text
        print(f"Extracted text: {' '.join(text.split()[:30])}")

    # Check if the text is a product page
    is_prod, unavailable, model_from_catalog = is_product(
        text=text, html=html, prod=prod, url=url, brand=brand
    )

    # Print the result
    if verbose:
        bool_str = 'Likely' if is_prod else 'Unlikely'
        unavail_str = ' unavailable' if is_prod and unavailable else ''
        prod_str = 'product' if model_from_catalog is None else 'catalog'
        print(f"{bool_str}{unavail_str} {prod_str} page: {url}")

    return url, is_prod, unavailable, model_from_catalog


def search_google(prod: str, brand: str, model: str, recursion_depth: int = 0):
    """ Perform a Google search using the Custom Search API
    Args:
        prod (str):     The product to search
        brand (str):    The product brand name to search
        model (str):    The product model name to search
        recursion_depth (int, optional): The depth of recursion. Defaults to 0
    Returns:
        str:    The title of search result (None if result is unlikely a product page)
        str:    The URL of search result (None if result is unlikely a product page)
        str:    The potentially updated product model name
    """
    results = query_google_cse(f"buy {brand} {model} {prod}")
    if len(results.get('items', [])) == 0:
        print(f"No Google search results found for {brand} {model} {prod}.")

    # Extracting URLs from search results
    for result in results.get('items', []):
        title, link = result.get('title'), result.get('link')
        if link[-1] == '/': link = link[:-1]  # Remove trailing slash

        # Skip URLs with less than 3 slashes
        if '/' not in link.split('://')[-1]:
            continue
        # Discard search results that are from Amazon, eBay, etc.
        if any(gkw in link for gkw in GOOG_DISCARD_KEYWORDS) or \
            ('amazon' in link.lower() and 'amazon' not in brand.lower()):
            continue

        # Discard search results that does not match the brand name
        split_link = link.split('/')
        cleaned_link = '/'.join(split_link[2:4] if '://' in link else split_link[:2])
        if unidecode(brand).replace(' ', '').replace('-', '').replace('_', '').lower() \
            not in cleaned_link.replace('-', '').replace('_', '').lower():
            # Double-check with LLM if the brand is in the URL
            response = check_with_llm(
                prod=prod, brand=brand, url=cleaned_link, query_type='check_url_brand',
                llm_model='gpt-3.5-turbo'
            )
            if not response.lower().strip().startswith('true'):
                continue

        # Fetch the webpage and check if it is a product page
        link, is_prod, unavailable, model_from_catalog = \
            fetch_and_check(prod=prod, url=link, brand=brand)

        if is_prod and not unavailable:  # This is likely an available product page
            if model_from_catalog is not None:
                # If the search result is likely a catalog page,
                # search again for a model found in the catalog page,
                # but skip the current model to avoid repetition
                if model_from_catalog == model or recursion_depth >= 3:
                    continue
                else:
                    return search_google(
                        prod, brand, model_from_catalog, recursion_depth + 1
                    )
            else:  # This is the product page we want! Return title and link
                return title, link, model

    # None of the search results are likely matching product pages. Return None
    print("No matching product page found in Google search results.")
    return None, None, None
