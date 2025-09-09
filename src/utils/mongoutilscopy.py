from pymongo import MongoClient
from datetime import datetime, timedelta

# ----------------------------
# 1. 连接 MongoDB
# ----------------------------
client = MongoClient('mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas')  # 修改为你的 MongoDB 地址
db = client['d_eagle3_guangzhou_llt_meas']  # ⚠️ 请替换为你的数据库名

# ----------------------------
# 2. 获取集合
# ----------------------------
collection_source = db['LLT_DISTTRANSFORMERMINCALC20250601']  # 源数据集合
collection_gear = db['TEMP_GEAR_INFO']                         # 档位信息集合
collection_target = db['TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL']  # 目标结果集合

# ----------------------------
# 3. 预加载 TEMP_GEAR_INFO 到字典（用于左连接）
# ----------------------------
gear_info_map = {}
for doc in collection_gear.find():
    disttran_name = doc.get("disttran_name")
    gear_info_map[disttran_name] = {
        "gear_type": doc.get("gear_type"),
        "gear_status": doc.get("gear_status")
    }

# ----------------------------
# 4. 查询主表数据（带筛选条件）
# ----------------------------
cursor = collection_source.find({
    "companyName": "LWGDJ",
    "distributionLineName": "10kV东平线F11A相",
    "tranType": "PRIVATE"
})

# ----------------------------
# 5. 构建结果并计算字段
# ----------------------------
results = []

for a in cursor:
    tran_id = a.get("tranId")
    tran_name =a.get("tranName")
    # 时间加八个小时
    data_time = a.get("dataTime")
    if data_time:
        if isinstance(data_time, str):
            try:
                data_time = datetime.fromisoformat(data_time)
            except Exception:
                pass
        data_time = data_time + timedelta(hours=8)
    
    # 获取档位信息（模拟 LEFT JOIN）
    gear = gear_info_map.get(tran_name, None)
    gear_type = None
    gear_status = None
    bianbi = None

    # 处理电压：如果 voltage 为 0，则设为 10
    voltage_raw = a.get("voltage", 0)
    voltage = 10 if voltage_raw == 0 else voltage_raw



    # 计算 highVolt 和 lowVolt
    highVolt = round(voltage, 2)


    # 构建结果文档
    result_doc = {
        "TRAN_ID": tran_id,
        "TRAN_NAME": tran_name,
        "DISTRIBUTION_LINE_ID": a.get("distributionLineId"),
        "DISTRIBUTION_LINE_NAME": a.get("distributionLineName"),
        "DATA_TIME": data_time,  # ISODate 类型直接写入
        "highVolt": highVolt
        # 增加一个时间日期字段，记录插入时间
        ,"INSERT_TIME": datetime.now(),
        "DATA_TIME_SIGN": data_time.date() if isinstance(data_time, datetime) else data_time,  # 只保留日期部分
    }

    results.append(result_doc)

# ----------------------------
# 6. 批量插入到目标集合
# ----------------------------
if results:
    # 按 DISTRIBUTION_LINE_NAME 清空旧数据
    distribution_line_name = results[0]["DISTRIBUTION_LINE_NAME"] if results else None
    if distribution_line_name:
        collection_target.delete_many({"DISTRIBUTION_LINE_NAME": distribution_line_name})

    collection_target.insert_many(results)
    print(f"✅ 成功插入 {len(results)} 条记录到 {collection_target.name}")
else:
    print("❌ 没有符合条件的数据需要插入")

# ----------------------------
# 7. 关闭连接
# ----------------------------
client.close()