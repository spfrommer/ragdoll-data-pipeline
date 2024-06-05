import os, re, csv, shutil


def get_product_list(cat_file_path: str, start_prod: int, num_prods: int):
    """ Read the list of products from the specified file and return a list of products
    Args:
        cat_file_path (str): The path to the file containing the product category list
        start_prod (int):    The index of the first product to process
        num_prods (int):     The number of products to process

    Returns:
        list:                The list of products
    """
    with open(cat_file_path, 'r') as cat_file:
        prod_cntr, prod_list = 0, []  # Initialize product counter and list

        for line in cat_file:
            # Stop processing if the specified number of products is reached
            if num_prods is not None and prod_cntr >= start_prod + num_prods:
                break

            # Remove spaces and line breaks and convert to lowercase
            prod = line.strip().lower().replace('\n', '')
            # Exclude empty lines and section headers
            if not prod or prod.startswith('#'):
                continue
            # Skip lines until the start product
            if prod_cntr >= start_prod:
                prod_list += [(prod_cntr, prod)]

            prod_cntr += 1  # Increment product counter

    return prod_list


def csv_to_dict(csv_file: str):
    """ Read a CSV file and return its first two columns as a dictionary
    Args:
        csv_file (str): The path to the CSV file
    Returns:
        dict:           The dictionary read from the CSV file
    """
    data_dict = {}

    # Create a new lookup table if it doesn't exist
    if not os.path.exists(csv_file):
        print(f"The file '{csv_file}' doesn't exist. Creating a new file.")
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Key', 'Value'])  # Write header if file is newly created

    # Read the CSV file and populate the dictionary
    with open(csv_file, 'r') as file:
        reader = csv.reader(file, delimiter='|')
        next(reader)  # Skip header row
        for row in reader:
            if len(row) == 2:  # Ensure each row has exactly two columns
                key, value = row
                data_dict[key] = value
            else:
                print(f"Ignoring row: {row}. Expected exactly 2 columns per row.")

    return data_dict


def get_next_versioned_filename(
    folder_path: str, increment_minor: bool = False, custom_major_version: int = None
):
    """ Generate a new versioned file name based on existing files in the specified directory
    Args:
        folder_path (str):
            The path to the directory containing the files.
        increment_minor (bool, optional):
            If True, update the number after the dot. Otherwise, increment the
            number before the dot. Defaults to False.
        custom_major_version (int, optional):
            When increment_minor is true, if provided, the major version will be set to
            this value. Uneffective when When increment_minor is False. Defaults to None.
    Returns:
        (int, int), str:
            The current maximum major and minor version numbers; the new versioned file name.
    """
    # Regular expression to extract version numbers from file names
    version_pattern = re.compile(r'products_v(\d+)\.(\d+)\.csv')

    # List files in the specified directory
    files = os.listdir(folder_path)

    # Initialize variables to track the maximum major and minor versions
    max_major = 0
    max_minor = -1  # Start from -1 to correctly handle case where no files are found

    # Extract and compute the maximum version numbers
    for filename in files:
        match = version_pattern.search(filename)

        # Extract major and minor version numbers from file name if found matching pattern
        if match:
            major_version = int(match.group(1))
            minor_version = int(match.group(2))

            # Update the maximum major version
            if major_version > max_major:
                max_major = major_version
                # Reset minor version if a new major version is found
                if custom_major_version is None:
                    max_minor = minor_version

            # Update the maximum minor version when found matching major version
            if major_version == \
                (custom_major_version if custom_major_version is not None else max_major):
                if minor_version > max_minor:
                    max_minor = minor_version

    # Get the current maximum version
    cur_max_version = (max_major, max_minor)

    # Increment the appropriate version component
    if increment_minor:
        # Use a custom version for the minor version if provided
        if custom_major_version is not None:
            max_major = custom_major_version
        max_minor += 1
    else:
        max_major += 1
        max_minor = 0  # Reset minor version on major increment

    # Construct the new file name
    new_filename = f'products_v{max_major}.{max_minor}'
    return cur_max_version, new_filename


def update_latest(root_directory: str):
    """ Update the 'latest.csv' file in each subdirectory of a root directory
    Args:
        root_directory (str): The path to the root directory to process
    """
    # Iterate over each subdirectory in the root directory
    for subdir, dirs, files in os.walk(root_directory):
        dst_path = os.path.join(subdir, f"latest.csv")

        # If the latest file does not exist, copy the latest versioned file
        if not os.path.exists(dst_path):
            (max_major, max_minor), _ = get_next_versioned_filename(subdir)
            src_path = os.path.join(subdir, f"products_v{max_major}.{max_minor}.csv")
            if max_minor > 0:
                shutil.copyfile(src_path, dst_path)
