import os, re
import pandas as pd
import click
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

from utils.file_utils import get_next_versioned_filename, get_product_list
from utils.query_utils import query_openai_llm


def find_sites_for_prod(prod_model: tuple[int, str, str, bool], brand_count: int = 20):
    """ Query OpenAI LLM to find a list of brands and models for a given product
    Args:
        prod_model (tuple[int, str, str]):
            Index and name of the product to search and name of the LLM to use
    """
    prod_cntr, prod, model, exclude_existing = prod_model
    print(f"Processing product {prod_cntr}: {prod}")
    llm_buffer_dir = f'llm_responses/find_sites/{model}'
    os.makedirs(llm_buffer_dir, exist_ok=True)

    # Get the next versioned dataset file name
    save_dir = f'dataset/{prod}'
    os.makedirs(save_dir, exist_ok=True)
    (max_major, _), filename = \
        get_next_versioned_filename(folder_path=save_dir, increment_minor=False)

    exclude_str = ''
    if exclude_existing and max_major > 0:
        # Load the existing dataset to exclude the brands already mentioned
        existing_df = pd.read_csv(f'{save_dir}/products_v{max_major}.0.csv')
        existing_brands = set(existing_df['Brand'].str.lower())
        brand_count -= len(existing_brands)
        exclude_str = f"Exclude brands: {', '.join(existing_brands)}. "

    # Query the OpenAI LLM model to find a list of brands and models
    if brand_count <= 0:
        print(f"More than enough brands already exist. Nothing needs to be done.")
        return

    buffer_path = f"{llm_buffer_dir}/{prod}{'_ee' if exclude_existing else ''}.txt"
    content = \
        f"Find me {brand_count} distinct {prod} manufacturers. " \
        "For each brand, give me the manufacturer website URLs of " \
        f"three randomly chosen {prod} models. {exclude_str}Try to reach " \
        f"{brand_count * 3} products in total if possible. Do not repeat. " \
        "Format results as semicolon-delimited CSV file (no space after " \
        "delimiter) with columns Brand;Model;URL (include this header)."
    response, from_buffer = query_openai_llm(
        content, buffer_path=buffer_path, model=model
    )
    print(f"OpenAI LLM response ("
          f"{'from buffer' if from_buffer else 'fresh request'}):\n{response}")

    # Remove redundant commas in the response
    response = response.replace(',', ' ').replace(';', ',')
    # Convert LLM response into rows and remove rows with too many/few delimiters
    lines = [l.strip() for l in response.split('\n')]
    lines = [l[:-1] if l.endswith(',') else l for l in lines]
    lines = [l for l in lines if l.count(',') == 2]

    # Convert the lines to a DataFrame
    data_df = pd.read_csv(StringIO('\n'.join(lines)))

    # Remove the index and special characters from the brand names
    def remove_index(_text):
        _parts = _text.replace('**', '').split('. ')
        _text = _parts[1] if len(_parts) > 1 else _text
        _text = re.sub(r'[%&\?\/:;=\+\!\*\(\)\'"\\]', '', _text)
        return re.sub(r'\s+', ' ', _text.strip())  # Remove consecutive spaces
    for col in ['Brand', 'Model']:
        data_df[col] = data_df[col].apply(remove_index)

    # Add a 'Product' column, and save to a CSV file
    data_df.insert(0, 'Product', prod)
    data_df.to_csv(f'dataset/{prod}/{filename}.csv', index=False)


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
    '--model', type=str, default='gpt-3.5-turbo',
    help="The OpenAI model to use for the query. Defaults to gpt-3.5-turbo."
)
@click.option(
    '--exclude_existing', is_flag=True, default=False,
    help="Whether to exclude the brands mentioned in the existing version."
)
@click.option(
    '--max_workers', type=int, default=8, help='Maximum number of concurrent workers'
)
def find_sites(
    cat_file_path: str, start_prod: int, num_prods: int,
    model: str, exclude_existing: bool, max_workers: int
):
    prod_list = get_product_list(cat_file_path, start_prod, num_prods)
    prod_models = [
        (prod_cntr, prod, model, exclude_existing) for prod_cntr, prod in prod_list
    ]
    print(f"Products to process: {[pm[1] for pm in prod_models]}")

    # Query OpenAI LLM to find product brands and models in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        {executor.submit(find_sites_for_prod, pm): pm for pm in prod_models}


if __name__ == "__main__":
    find_sites()
