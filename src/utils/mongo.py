import pandas as pd
from pymongo import MongoClient

# 读取 CSV 文件
df = pd.read_csv('D:\download\working\TEMP_GEAR_INFO.csv', encoding='utf-8')

# 数据预处理（可选）：比如空值处理、类型转换
# df['age'] = pd.to_numeric(df['age'], errors='coerce')

# 转换为字典列表
data = df.to_dict(orient='records')

# 连接 MongoDB
client = MongoClient('mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas')
db = client['d_eagle3_guangzhou_llt_meas']           # 数据库名
collection = db['TEMP_GEAR_INFO']      # 集合名

# 插入数据
collection.insert_many(data)

print(f"成功导入 {len(data)} 条记录")