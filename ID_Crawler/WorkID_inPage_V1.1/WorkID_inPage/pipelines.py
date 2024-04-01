# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from tqdm.autonotebook import tqdm
import pandas as pd
import os

class WorkidInpagePipeline:
    def process_item(self, item, spider):
        data = {}
        for key, value in item.items():
            if(isinstance(value,list)):
                data[key] = value
        df = pd.DataFrame(data)
        df.to_csv(spider.crawl_folder_path + item['Title'] + '.csv', index=False)
        return item
    
    def process_csv_files(self, directory_path):
        # Initialize an empty set to store unique IDs
        file_lst = []
        for filename in os.listdir(directory_path):
            if filename.endswith(".csv"):
                file_lst.append(filename)
        print("Merging {0} Files...".format(len(file_lst)))
        unique_ids = set()
        pbar = tqdm(total=len(file_lst), desc="Merging CSVs:", leave=True, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]', position = 0 ,mininterval = 1)
        
        # Iterate through all CSV files in the specified directory
        for filename in file_lst:
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path, usecols=["ID"])  # Read only the "ID" column
            unique_ids.update(df["ID"])  # Add IDs to the set
            pbar.update(1)

        # Split the unique IDs into batches of 2000
        id_batches = [list(unique_ids)[i:i + 2000] for i in range(0, len(unique_ids), 2000)]

        # Save each batch as a separate CSV 
        batch_folder = directory_path+'batches\\'
        if os.path.exists(batch_folder) is False:
            os.makedirs(batch_folder)
        for idx in range(len(id_batches)):
            batch_df = pd.DataFrame({"ID": id_batches[idx]})
            batch_filename = batch_folder + f"batch_{idx + 1}.csv"
            batch_df.to_csv(batch_filename, index=False)

        # Combine all batches into a single CSV file
        combined_df = pd.DataFrame({"ID": sorted(unique_ids, reverse=True)})
        combined_filename = directory_path + "combined_data.csv"
        combined_df.to_csv(combined_filename, index = False)

        pbar.close()
        print(f"Processed {len(unique_ids)} unique IDs.")
        print(f"{len(id_batches)} batches saved as separate CSV files.")
        print(f"Combined data saved in {combined_filename}.")
    
    def close_spider(self, spider):
        if(hasattr(spider,'pbar_lst')):
            for each in spider.pbar_lst:
                each.close()
            self.process_csv_files(spider.crawl_folder_path)