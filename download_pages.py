import shutil
from bs4 import BeautifulSoup
import click, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import tqdm

from utils.file_utils import get_product_list
from utils.query_utils import fetch_html


def fetch_page(idx_urls: list[tuple]):
    print('----- Fetching new product')
    for idx_url in idx_urls:
        idx, url = idx_url
        print(f'Fetching {url}...')
        new_url, html = fetch_html(url)
        if html != None:
            return idx, html
        else:
            print(f"Failed to fetch: {url}")

    return None, None


@click.command()
@click.option(
    '--cat_file_path', type=str, default='dataset/categories.md',
    help="The path to the file containing the list of categories. " \
        "Defaults to dataset/categories.md."
)
@click.option(
    '--start_prod', type=int, default=0,
    help="The index of the first product to process. Defaults to 0."
)
@click.option(
    '--num_prods', type=int, default=None,
    help="Number of products to process. Defaults to None (process all products)."
)
@click.option(
    '--max_workers', type=int, default=1, help='Maximum number of concurrent workers'
)
def download_pages(
    cat_file_path: str, start_prod: int, num_prods: int, max_workers: int
):
    prod_list = get_product_list(cat_file_path, start_prod, num_prods)
    print(f"Products to download: {[pm[1] for pm in prod_list]}")

    for _, prod in tqdm.tqdm(prod_list):
        # Skip if the dataset does not exist for this product
        if not os.path.exists(f'dataset/{prod}/latest.csv'):
            print(f"No dataset found for product {prod}. Skipping..")
            continue

        # Read the latest URLs from the dataset
        print(f"==================== Downloading {prod} webpages...")
        with open(f'dataset/{prod}/latest.csv', 'r') as file:
            df = pd.read_csv(file)
            df['index'] = range(0, len(df) + 0)
            items = []
            for group in df.groupby('Brand'):
                items.append([
                    (cntr, url) for cntr, url in zip(group[1]['index'], group[1]['URL'])
                ])
                
        if os.path.exists(f"dataset/{prod}/pages"):
            shutil.rmtree(f"dataset/{prod}/pages")
        os.makedirs(f"dataset/{prod}/pages", exist_ok=True)
        
        if os.path.exists(f"dataset/{prod}/content"):
            shutil.rmtree(f"dataset/{prod}/content")
        os.makedirs(f"dataset/{prod}/content", exist_ok=True)

        # Fetch HTML pages in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_items = {executor.submit(fetch_page, item): item for item in items}

            # Gather the HTML pages in parallel and save them to disk
            for future in as_completed(future_items):
                idx, html = future.result()
                if html is None:
                    continue
                with open(f"dataset/{prod}/pages/{idx}.html", 'w') as file:
                    file.write(html)
                with open(f"dataset/{prod}/content/{idx}.txt", 'w') as file:
                    file.write(BeautifulSoup(html, 'lxml').get_text())


if __name__ == "__main__":
    download_pages()
