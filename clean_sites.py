# Yatong Bai, 04/2024

import os
import pandas as pd
import click
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.cleaning_utils import fetch_and_check, search_google
from utils.file_utils import get_next_versioned_filename


def check_product(id_prod_brand_model_url: tuple):
    """ Check if a URL points to a product page. If the provided URL is likely not a product
        page but a product page is found via Google search, update the model name and the URL
    Args:
        id_prod_brand_model_url (tuple):
            A tuple containing the index, product name, brand, model, and URL
    Returns:
        tuple:
            A tuple containing the index, product name, brand, model, updated product model,
            URL, updated URL, and a boolean indicating if the URL points to a product page
    """
    id, (prod, brand, model, url) = id_prod_brand_model_url
    print(f"**** Product {id + 1} URL: {url} ****")

    # Fetch the URL and check if it is a matching product page
    url, is_prod, unavailable, model_from_catalog = \
        fetch_and_check(prod=prod, url=url, brand=brand)

    if is_prod and not unavailable and model_from_catalog is None:
        # If the URL points to a web page that is likely a match, return the URL
        new_model, new_url = model, url
    else:
        # If the URL is not a match, search Google for the product
        title, new_url, new_model = search_google(prod, brand, model)
        if new_url is None and model_from_catalog is not None and model_from_catalog != model:
            title, new_url, new_model = search_google(prod, brand, model_from_catalog)

        print(f"Product {id + 1} search result: {title}")
        print(f"Product {id + 1} search URL: {new_url}")
        # If the search result is likely not a product page, search_google will return None
        is_prod = new_url is not None

    new_model = new_model.replace('"', '') if new_model is not None else None
    return (id, prod, brand, model, new_model, url, new_url, is_prod)


def clean_sites_in_dataset(
    data_df: pd.DataFrame, data_pardir: str, new_dataset_path: str, max_workers: int
):
    """ Clean the websites in the dataset so that only valid product pages are included
    Args:
        data_df (pd.DataFrame): The dataset to clean
        data_pardir (str):      The parent directory of the dataset
        new_dataset_path (str): The path of the new cleaned dataset file to save
        max_workers (int):      The maximum number of concurrent workers
    """
    # Create a temporary file to store the cleaned and dropped data
    for prefix in ['temp', 'dropped']:
        with open(f'{data_pardir}/{prefix}_{new_dataset_path}.csv', 'w') as file:
            file.write('Product,Brand,Model,URL\n')

    # Load dataset and initialize variables
    prods, brands, models, urls = tuple([list(data_df[col]) for col in data_df.columns])

    # Check each product in the dataset in parallel
    items = list(enumerate(zip(prods, brands, models, urls)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        # Gather the results of the parallelized tasks
        future_items = {executor.submit(check_product, item): item for item in items}

        # Update the dataset with the new URLs
        for future in as_completed(future_items):
            id, pr0d, brand, model, new_model, url, new_url, is_prod = future.result()

            if is_prod:
                # Update the URL in the dataset
                data_df.at[id, 'URL'] = new_url
                data_df.at[id, 'Model'] = new_model
                with open(f'{data_pardir}/temp_{new_dataset_path}.csv', 'a') as file:
                    file.write(f"{pr0d},{brand},{new_model},{new_url}\n")
            else:
                # Drop the product from the dataset
                data_df.drop(id, inplace=True)
                with open(f'{data_pardir}/dropped_{new_dataset_path}.csv', 'a') as file:
                    file.write(f"{pr0d},{brand},{model},{url}\n")

    # Deduplicate the dataset based on the 'URL' column
    data_df = data_df.drop_duplicates(subset=['URL'])

    # Save the cleaned data to a new file
    data_df.to_csv(f'{data_pardir}/{new_dataset_path}.csv', index=False)
    print(f"Cleaned data saved to '{data_pardir}/{new_dataset_path}.csv'")

    # Remove the temporary file
    os.remove(f'{data_pardir}/temp_{new_dataset_path}.csv')


@click.command()
@click.option(
    '--prod', type=str, required=True, help='The product name work on'
)
@click.option(
    '--major_version', type=int, default=None, help='Custom major version number'
)
@click.option(
    '--minor_version', type=int, default=None, help='Custom minor version number'
)
@click.option(
    '--max_workers', type=int, default=6, help='Maximum number of concurrent workers'
)
@click.option(
    '--instances_to_proc', type=int, default=None, help='Number of instances to process'
)
def clean_sites(
    prod: str, major_version: int, minor_version: int,
    max_workers: int, instances_to_proc: int
):
    # Create buffer directories to store the dataset, HTML pages, and responses
    for dir_names in [
        'html_pages/pages', 'llm_responses/check_product_page', 'google_responses'
    ]:
        os.makedirs(dir_names, exist_ok=True)

    # Skip until the start product; also skip if product directory does not exist
    data_pardir = f'dataset/{prod}'

    # Get cleaned dataset file name and max existing major and minor versions
    (max_major, max_minor), new_dataset_path = get_next_versioned_filename(
        data_pardir, increment_minor=True, custom_major_version=major_version
    )

    # Load the original dataset to clean
    if minor_version is not None:  # Use custom minor version if provided
        max_minor = minor_version
    orig_dataset_path = f'{data_pardir}/products_v{max_major}.{max_minor}.csv'

    # Load the original dataset and clean the sites
    print(f"Processing product {prod} at {orig_dataset_path}...")
    data_df = pd.read_csv(orig_dataset_path)[:instances_to_proc]
    clean_sites_in_dataset(data_df, data_pardir, new_dataset_path, max_workers)


if __name__ == "__main__":
    clean_sites()
