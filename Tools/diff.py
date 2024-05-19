import json
from deepdiff import DeepDiff

# 加载两个JSON文件
def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# 比较两个JSON对象
def compare_json(file1, file2):
    json1 = load_json(file1)
    json2 = load_json(file2)
    diff = DeepDiff(json1, json2, ignore_order=True).pretty()
    return diff

# 示例文件路径
file_path1 = r'D:\2024Spring\DLsite-Analysis\日間ランキング.json'
file_path2 = r'D:\2024Spring\DLsite-Analysis\日間ランキング_v1.json'

# 比较并打印差异
differences = compare_json(file_path1, file_path2)
print(differences)