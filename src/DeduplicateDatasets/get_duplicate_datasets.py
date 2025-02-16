import csv
import os
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm  # Import tqdm for progress bar

from DeduplicateDatasets.config import BUCKET_NAME, s3_client


def list_obj_keys(prefix, s3_client, bucket_name):
    """
    Fetches all object keys (file paths) from an S3 bucket with a given prefix.

    Args:
        prefix (str): The folder path in the S3 bucket to search in.
        s3_client (boto3.client): An initialized S3 client.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        list: A list of all object keys (file paths) in the specified S3 bucket prefix.
    """
    obj_keys = []
    continuation_token = None

    with tqdm(desc="Fetching S3 Objects", unit=" objects") as pbar:
        while True:
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=continuation_token,
                )
            else:
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if "Contents" in response:  # Ensure 'Contents' exists to avoid errors
                for obj in response["Contents"]:
                    obj_keys.append(obj["Key"])
                    pbar.update(1)  # Update progress bar

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

    return obj_keys


def generate_duplicate_filenames_csv(
    s3_client, bucket_name, prefix="", csv_output_path="data/duplicate_filenames.csv"
):
    """
    Generates a CSV file listing duplicate filenames in an S3 bucket.

    Args:
        s3_client (boto3.client): An initialized S3 client.
        bucket_name (str): The name of the S3 bucket.
        prefix (str, optional): The folder path to search within the bucket. Default is "" (root).
        csv_output_path (str, optional): The file path for the generated CSV. Default is "data/duplicate_filenames.csv".

    Returns:
        str: The path to the generated CSV file.
    """
    # Retrieve all object keys from S3
    all_object_keys = list_obj_keys(prefix, s3_client, bucket_name)

    # Extract filenames and group by filename
    filename_map = defaultdict(list)

    with tqdm(
        total=len(all_object_keys), desc="Processing Filenames", unit=" files"
    ) as pbar:
        for object_key in all_object_keys:
            filename = Path(object_key).name  # Extract the filename
            filename_map[filename].append(
                object_key
            )  # Store file path under the filename
            pbar.update(1)  # Update progress bar

    # Filter only duplicate filenames (those with multiple paths)
    duplicate_files = {
        fname: paths for fname, paths in filename_map.items() if len(paths) > 1
    }

    if not duplicate_files:
        print("No duplicate filenames found.")
        return None

    # Determine the maximum number of file paths per duplicate filename
    max_paths = max((len(paths) for paths in duplicate_files.values()), default=2)

    # Create column headers dynamically based on max paths
    columns = ["Filename"] + [f"File Path {i+1}" for i in range(max_paths)]

    # Ensure output directory exists
    os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)

    # Write duplicate filenames to CSV
    with open(csv_output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)

        # Write header row
        writer.writerow(columns)

        with tqdm(
            total=len(duplicate_files), desc="Writing to CSV", unit=" rows"
        ) as pbar:
            # Write rows with filename and their multiple file paths
            for filename, paths in duplicate_files.items():
                writer.writerow([filename] + paths)
                pbar.update(1)  # Update progress bar

    print(f"✅ CSV file saved: {csv_output_path}")
    return csv_output_path


if __name__ == "__main__":
    csv_file_generated = generate_duplicate_filenames_csv(
        s3_client, BUCKET_NAME, prefix=""
    )
    if csv_file_generated:
        print(f"Duplicate filenames CSV generated: {csv_file_generated}")
