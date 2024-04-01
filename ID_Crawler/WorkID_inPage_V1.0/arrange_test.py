import os
import pandas as pd

def process_csv_files(directory_path):
    # Initialize an empty set to store unique IDs
    unique_ids = set()

    # Iterate through all CSV files in the specified directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path, usecols=["ID"])  # Read only the "ID" column
            unique_ids.update(df["ID"])  # Add IDs to the set

    # Split the unique IDs into batches of 2000
    id_batches = [list(unique_ids)[i:i + 2000] for i in range(0, len(unique_ids), 2000)]

    # Save each batch as a separate CSV file
    for idx in range(len(id_batches)):
        batch_df = pd.DataFrame({"ID": id_batches[idx]})
        batch_filename = f"batch_{idx + 1}.csv"
        batch_df.to_csv(batch_filename, index=False)

    # Combine all batches into a single CSV file
    combined_df = pd.DataFrame({"ID": sorted(unique_ids, reverse=True)})
    combined_filename = "combined_data.csv"
    combined_df.to_csv(combined_filename, index=False)

    print(f"Processed {len(unique_ids)} unique IDs.")
    print(f"{len(id_batches)} batches saved as separate CSV files.")
    print(f"Combined data saved in {combined_filename}.")

# Example usage:
directory_path = "D:\\2024Spring\\DLsite-Analysis\\ID_Crawler\\WorkID_inPage_V0.9\\test_tag"
process_csv_files(directory_path)