from pymongo import MongoClient
from bson.int64 import Int64  # 用于 Long 类型
from bson import json_util
import json
from utils.damengQuery import query_measurement_ids

# =================== 配置 ===================
# MongoDB 连接信息
client = MongoClient('mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas')  # 修改为你的地址
db = client['d_eagle3_guangzhou_llt_meas']                  # 修改为你的数据库名
collection = db['cim_distmeasminute20250601']

# 要更新的 measurementId 列表（字符串）
measurement_ids = query_measurement_ids()

# ============================================
#               执行更新操作
# ============================================

for str_id in measurement_ids:
    # 转换为 Int64 (MongoDB Long)
    long_id = Int64(str_id)
    filter_query = {"measurementId": long_id}

    # === 第一步：更新 P 相关字段 ===
    update_P_pipeline = [
        {
            "$set": {
                "P": {"$multiply": ["$P_C", 3]},
                "PQ_P_A": {"$multiply": ["$P_C", 3]},
                "PQ_P_A_CALC": {"$multiply": ["$P_C", 3]}
            }
        }
    ]

    try:
        result_P = collection.update_many(filter_query, update_P_pipeline)
        matched_count_P = result_P.matched_count
        modified_count_P = result_P.modified_count

        # === 第二步：更新 Q 相关字段 ===
        update_Q_pipeline = [
            {
                "$set": {
                    "Q": {"$multiply": ["$Q_C", 3]},
                    "PQ_P_R": {"$multiply": ["$Q_C", 3]},
                    "PQ_P_R_CALC": {"$multiply": ["$Q_C", 3]}
                }
            }
        ]
        result_Q = collection.update_many(filter_query, update_Q_pipeline)
        matched_count_Q = result_Q.matched_count
        modified_count_Q = result_Q.modified_count

        # === 输出日志 ===
        print(f"✅ measurementId={str_id}")
        print(f"   P更新 → 匹配: {matched_count_P}, 修改: {modified_count_P}")
        print(f"   Q更新 → 匹配: {matched_count_Q}, 修改: {modified_count_Q}")

    except Exception as e:
        print(f"❌ 更新失败 measurementId={str_id}: {e}")