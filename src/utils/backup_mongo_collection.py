import pymongo
from datetime import datetime
import sys

# ================ 配置区域 ===================
MONGO_URI = "mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas"  # 修改为你的 MongoDB 连接地址
DATABASE_NAME = "d_eagle3_guangzhou_llt_meas"      # 修改为你的数据库名
SOURCE_COLLECTION = "cim_distmeasminute20250601" # 修改为你要备份的集合名

# 备份集合命名规则：原名 + _backup + 时间戳（防止重复）
# BACKUP_SUFFIX = "_backup_" + datetime.now().strftime("%Y%m%d_%H%M%S")
# 或者固定名称，如 "_backup"，但会覆盖之前的备份
BACKUP_SUFFIX = "_backup"

# 是否删除已存在的备份集合（避免重复）
DROP_EXISTING_BACKUP = True
# ============================================

def backup_collection():
    try:
        # 连接 MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        source_col = db[SOURCE_COLLECTION]
        backup_col_name = SOURCE_COLLECTION + BACKUP_SUFFIX
        backup_col = db[backup_col_name]

        # 检查源集合是否存在
        if SOURCE_COLLECTION not in db.list_collection_names():
            print(f"❌ 源集合 '{SOURCE_COLLECTION}' 不存在！")
            sys.exit(1)

        # 可选：删除已存在的备份集合
        if DROP_EXISTING_BACKUP and backup_col_name in db.list_collection_names():
            db[backup_col_name].drop()
            print(f"🗑️ 已删除已存在的备份集合: {backup_col_name}")

        # 执行备份：使用 aggregate + $out 或 insert_many
        print(f"🚀 开始备份集合 '{SOURCE_COLLECTION}' 到 '{backup_col_name}'...")

        # 方法1：使用 aggregate + $out（推荐，高效，保留索引结构）
        pipeline = [{"$match": {}}, {"$out": backup_col_name}]
        source_col.aggregate(pipeline)

        # 获取备份文档数量
        count = backup_col.count_documents({})
        print(f"✅ 备份完成！共 {count} 条文档已备份到 '{backup_col_name}'")

    except Exception as e:
        print(f"❌ 备份失败：{e}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    print("🔍 正在执行 MongoDB 集合备份...")
    backup_collection()