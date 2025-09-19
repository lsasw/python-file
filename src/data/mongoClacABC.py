from pymongo import MongoClient
from bson.int64 import Int64  # 用于 Long 类型
# from bson import json_util  # 未使用可删除
# import json  # 未使用可删除
from src.data.damengQuery import query_measurement_ids

# =================== 配置 ===================
# MongoDB 连接信息

client = MongoClient('mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas')
db = client['d_eagle3_guangzhou_llt_meas']

# 封装批量更新函数
def update_all_collections(phase, measurement_ids):
    for day in range(1,32):
        date_str = f"202508{day:02d}"
        collection_name = f"cim_distmeasminute{date_str}"
        collection = db[collection_name]
        print(f"\n===== 正在处理集合: {collection_name} =====")

        # 字段名动态切换
        p_field = f"P_{phase}"
        q_field = f"Q_{phase}"

        for str_id in measurement_ids:
            long_id = Int64(str_id)
            filter_query = {"measurementId": long_id}

            # 更新 P 相关字段，先将字段转为 double 类型再乘法
            update_P_pipeline = [
                {
                    "$set": {
                        "P": {"$multiply": [ {"$toDouble": f"${p_field}"}, 3 ]},
                        "PQ_P_A": {"$multiply": [ {"$toDouble": f"${p_field}"}, 3 ]},
                        "PQ_P_A_CALC": {"$multiply": [ {"$toDouble": f"${p_field}"}, 3 ]}
                    }
                }
            ]

            try:
                result_P = collection.update_many(filter_query, update_P_pipeline)
                matched_count_P = result_P.matched_count
                modified_count_P = result_P.modified_count

                # 更新 Q 相关字段，先将字段转为 double 类型再乘法
                update_Q_pipeline = [
                    {
                        "$set": {
                            "Q": {"$multiply": [ {"$toDouble": f"${q_field}"}, 3 ]},
                            "PQ_P_R": {"$multiply": [ {"$toDouble": f"${q_field}"}, 3 ]},
                            "PQ_P_R_CALC": {"$multiply": [ {"$toDouble": f"${q_field}"}, 3 ]}
                        }
                    }
                ]
                result_Q = collection.update_many(filter_query, update_Q_pipeline)
                matched_count_Q = result_Q.matched_count
                modified_count_Q = result_Q.modified_count

                print(f"✅ measurementId={str_id}")
                print(f"   P更新 → 匹配: {matched_count_P}, 修改: {modified_count_P}")
                print(f"   Q更新 → 匹配: {matched_count_Q}, 修改: {modified_count_Q}")

            except Exception as e:
                print(f"❌ 更新失败 measurementId={str_id}: {e}")

# 主程序入口
if __name__ == "__main__":
    # 定义ABC三组参数
    configs = [
        # phase, company_id, distribution_line_id
        ("A", "888795033246355456", "888827363571552257"),
        ("B", "888795261907226624", "888827775720640512"),
        ("C", "888795392035508224", "888827837720842240"),
    ]
    for phase, company_id, distribution_line_id in configs:
        
        print(f"\n=== 开始处理相位: {phase}, 公司ID: {company_id}, 线路ID: {distribution_line_id} ===")
        measurement_ids = query_measurement_ids(company_id, distribution_line_id)
        print("查询到的计量点 ID 列表总数： ", len(measurement_ids))
        update_all_collections(phase, measurement_ids)



