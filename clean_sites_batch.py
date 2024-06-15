# Yatong Bai, 04/2024

import os, click
import pandas as pd

from clean_sites import clean_sites_in_dataset
from utils.file_utils import get_next_versioned_filename, get_product_list


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
    '--major_version', type=int, default=None, help='Custom major version number'
)
@click.option(
    '--minor_version', type=int, default=None, help='Custom minor version number'
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
    major_version: int, minor_version: int, max_workers: int, instances_to_proc: int
):
    # Create buffer directories to store the dataset, HTML pages, and responses
    for dir_names in [
        'html_pages/pages', 'llm_responses/check_product_page', 'google_responses'
    ]:
        os.makedirs(dir_names, exist_ok=True)

    prod_list = get_product_list(cat_file_path, start_prod, num_prods)

    for prod_cntr, prod in prod_list:
        data_pardir = f'dataset/{prod}'
        if not os.path.exists(data_pardir):  # Skip if product directory does not exist
            continue

        # Get cleaned dataset file name and max existing major and minor versions
        (max_major, max_minor), new_dataset_path = get_next_versioned_filename(
            data_pardir, increment_minor=True, custom_major_version=major_version
        )

        # Load the original dataset to clean
        if minor_version is not None:  # Use custom minor version if provided
            max_minor = minor_version
        orig_dataset_path = f'{data_pardir}/products_v{max_major}.{max_minor}.csv'

        # Skip if the original dataset does not exist
        if not os.path.exists(orig_dataset_path):
            print(f"Skip product {prod}: missing raw dataset {orig_dataset_path}.")
            continue

        # Load the original dataset and clean the sites
        print(f"Processing product {prod_cntr}: {prod} at {orig_dataset_path}...")
        data_df = pd.read_csv(orig_dataset_path)[:instances_to_proc]
        clean_sites_in_dataset(data_df, data_pardir, new_dataset_path, max_workers)


if __name__ == "__main__":
    clean_sites()
