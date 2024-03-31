# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd

class WorkidInpagePipeline:
    def process_item(self, item, spider):
        data = {'ID': item['ID']}
        df = pd.DataFrame(data)
        df.to_csv(item['Title'] + '.csv', index=False)
        return item
