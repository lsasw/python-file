# -*- coding: utf-8 -*-
import datetime
import os
import pymongo
import pandas as pd
import json
from typing import Any, Dict, List
import re

# ==================== 配置区 ====================
# 修改以下参数以匹配你的 MongoDB 环境
MONGO_URI = "mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas"          # MongoDB 连接地址
DATABASE_NAME = "d_eagle3_guangzhou_llt_meas"             # 替换为你的数据库名

# 要导出的集合列表
COLLECTION_NAMES = [
    "TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL"
]

# 输出文件目录（脚本所在目录下）
OUTPUT_DIR = "./mongo_export_csv"
# =================================================

def flatten_document(doc: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    递归展平嵌套的字典文档，例如 {'a': {'b': 1}} -> {'a.b': 1}
    """
    items = []
    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_document(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # 将列表转为字符串，如 [1,2,3] -> "1,2,3"
            items.append((new_key, ','.join(map(str, v)) if v else None))
        elif isinstance(v, (datetime.datetime, datetime.date)):
            # 日期时间转字符串
            items.append((new_key, v.isoformat()))
        else:
            items.append((new_key, v))
    return dict(items)

def export_collection_to_csv(db, collection_name: str, output_dir: str):
    """
    将指定集合导出为 CSV 文件
    """
    collection = db[collection_name]
    print(f"正在读取集合: {collection_name} ...")

    # 查询所有文档
    cursor = collection.find({})
    documents = list(cursor)

    if not documents:
        print(f"⚠️ 集合 {collection_name} 中没有数据。")
        return

    print(f"共读取到 {len(documents)} 条文档。")

    # 展平所有文档
    flattened_docs = [flatten_document(doc) for doc in documents]

    # 转为 DataFrame
    df = pd.DataFrame(flattened_docs)

    # 构建输出路径
    output_file = os.path.join(output_dir, f"{collection_name}.csv")

    # 导出为 CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig')  # utf-8-sig 避免 Excel 中文乱码
    print(f"✅ 集合 {collection_name} 已成功导出至: {output_file}")

def main():
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"输出目录: {OUTPUT_DIR}")

    # 连接 MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # 测试连接
        print("✅ 成功连接到 MongoDB")
    except Exception as e:
        print(f"❌ 连接 MongoDB 失败: {e}")
        return

    db = client[DATABASE_NAME]

    # 验证数据库是否存在
    if DATABASE_NAME not in client.list_database_names():
        print(f"❌ 数据库 '{DATABASE_NAME}' 不存在。")
        client.close()
        return

    # 遍历集合并导出
    for collection_name in COLLECTION_NAMES:
        if collection_name not in db.list_collection_names():
            print(f"⚠️ 集合 {collection_name} 不存在，跳过...")
            continue
        export_collection_to_csv(db, collection_name, OUTPUT_DIR)

    # 关闭连接
    client.close()
    print("🔚 所有集合导出完成。")

if __name__ == "__main__":
    main()