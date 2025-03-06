import os
import pandas as pd
import hashlib
import random
import string
import argparse

def generate_salted_md5_hash(value):
    salt = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    combined = value + salt
    return hashlib.md5(combined.encode()).hexdigest()

def anonymize_files(input_directory, output_directory, mapping_file):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    id_mapping = []

    for filename in os.listdir(input_directory):
        if filename.endswith('.parquet'):
            household_id = filename.replace('.parquet', '')
            anonymized_id = generate_salted_md5_hash(household_id)
            file_path = os.path.join(input_directory, filename)
            df = pd.read_parquet(file_path)

            # Add the anonymized ID as a new column
            df['anonymized_id'] = anonymized_id

            # Create the new filenames
            output_file_name = f"{anonymized_id}.parquet"

            # Save to Parquet
            output_parquet_path = os.path.join(output_directory, output_file_name)
            df.to_parquet(output_parquet_path, index=False)

            # Add to mapping list
            id_mapping.append({'old_id': household_id, 'new_id': anonymized_id})

            print(f"Processed {filename} and saved to {output_parquet_path}")

    # Save the mapping to a CSV file
    mapping_df = pd.DataFrame(id_mapping)
    mapping_df.to_csv(mapping_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Anonymize household IDs in Parquet files based on filenames and generate a mapping.')
    parser.add_argument('input_directory', type=str, help='Path to the input directory containing Parquet files')
    parser.add_argument('output_directory', type=str, help='Path to the output directory for anonymized files')
    parser.add_argument('mapping_file', type=str, help='Path to the CSV file for the ID mapping')

    args = parser.parse_args()

    # Ensure paths are handled correctly on Windows
    input_directory = os.path.normpath(args.input_directory)
    output_directory = os.path.normpath(args.output_directory)
    mapping_file = os.path.normpath(args.mapping_file)

    anonymize_files(input_directory, output_directory, mapping_file)