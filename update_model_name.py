import os, re, click
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.query_utils import fetch_html
from utils.cleaning_utils import check_with_llm
from utils.file_utils import get_product_list


def update_prod_name(prod_brand_model_url: tuple):
    id, (prod, brand, model, url) = prod_brand_model_url

    new_url, html = fetch_html(url, cc_fetch=False, browser='firefox')
    if html is None:
        print(f"Failed to fetch: {url}")
        return id, None

    soup = BeautifulSoup(html, 'lxml')
    text, title = soup.get_text(), soup.title.string
    if title is None:
        print(f"Title is None for {new_url}")
        return id, None

    title = re.sub(r'\s+', ' ', re.sub(r'[\n—\u2013|-]', ' ', title.strip()))
    print(f"{id}: Title is '{title}' for {new_url}")

    response = check_with_llm(
        prod=prod, brand=brand, url=new_url, text=text, title=title, model=model,
        query_type='update_model_name', llm_model='gpt-3.5-turbo'
    )
    new_model_name = response.split('\n')[0].strip()
    return id, new_model_name


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
    '--max_workers', type=int, default=6, help='Maximum number of concurrent workers'
)
@click.option(
    '--instances_to_proc', type=int, default=None,
    help='Number of instances to process for each product'
)
def clean_sites(
    cat_file_path: str, start_prod: int, num_prods: int,
    max_workers: int, instances_to_proc: int
):
    prod_list = get_product_list(cat_file_path, start_prod, num_prods)

    for prod_cntr, prod in prod_list:
        data_pardir = f'dataset/{prod}'
        if not os.path.exists(data_pardir):  # Skip if product directory does not exist
            continue

        orig_dataset_path = f'{data_pardir}/latest.csv'
        new_dataset_path = f'{data_pardir}/updated_names.csv'

        # Skip if the original dataset does not exist
        if not os.path.exists(orig_dataset_path):
            print(f"Skip product {prod}: missing raw dataset {orig_dataset_path}.")
            continue

        # Load the original dataset and clean the sites
        print(f"Processing product {prod_cntr}: {prod} at {orig_dataset_path}...")
        data_df = pd.read_csv(orig_dataset_path)[:instances_to_proc]

        # Load dataset and initialize variables
        prods, brands, models, urls = tuple([list(data_df[col]) for col in data_df.columns])

        # Check each product in the dataset in parallel
        items = list(enumerate(zip(prods, brands, models, urls)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            # Gather the results of the parallelized tasks
            future_items = {executor.submit(update_prod_name, item): item for item in items}

            # Update the dataset with the new URLs
            for future in as_completed(future_items):
                id, new_model_name = future.result()

                if new_model_name is not None:
                    # Update the model name in the dataset
                    data_df.at[id, 'Model'] = new_model_name

        # Sort the dataset by product, brand, and model
        # data_df.sort_values(by=['Product', 'Brand', 'Model'], inplace=True)

        # Save the cleaned data to a new file
        data_df.to_csv(f'{new_dataset_path}', index=False)
        print(f"Cleaned data saved to '{new_dataset_path}'")


if __name__ == "__main__":
    clean_sites()
