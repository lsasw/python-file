import dmPython
import pymongo
from datetime import datetime
import re

# -----------------------------
# 配置信息（请根据实际情况修改）
# -----------------------------

# 达梦数据库配置
DM_DSN = {
    "user": "d_eagle3_guangzhou_llt_cim",             # 用户名
    "password": "Dfdb19c",  
    "server": "192.168.0.78",   # 替换为你的达梦数据库 IP 和端口
    "port": "35236"        # 实例名或数据库名
}

# MongoDB 配置
MONGO_URI = "mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas"
MONGO_DB_NAME = "d_eagle3_guangzhou_llt_meas"

# 字段映射：达梦字段 -> MongoDB 字段（若相同可省略，仅列出需要转换的）
# 这里为空，表示字段名直接对应，无特殊转换
# 如有特殊映射可添加，例如：'pa': 'P_A', ...
FIELD_MAPPING = {}

# MongoDB 中每个字段的默认值（不存在时设为 null）
MONGO_FIELDS = [
    "averageVoltage", "companyId", "D", "F", "F_A", "F_B", "F_C", "GRID_TYPE",
    "I", "I_A", "I_B", "I_C", "k", "kVA", "measTime", "measurementId",
    "P", "P_A", "P_B", "P_C", "powerUpTime", "PQ_P_A", "PQ_P_A_CALC",
    "PQ_P_R", "PQ_P_R_CALC", "PQ_R_A", "PQ_R_A_CALC", "PQ_R_R", "PQ_R_R_CALC",
    "Q", "Q_A", "Q_B", "Q_C", "READ_P_A", "READ_P_A_BEGIN", "READ_P_R",
    "READ_P_R_BEGIN", "READ_R_A", "READ_R_A_BEGIN", "READ_R_R", "READ_R_R_BEGIN",
    "S", "V", "V_A", "V_B", "V_C", "WEATHER"
]

def query_dm_line_data(conn, company_id, date):
    """查询线路运行数据"""
    sql = """
        SELECT
            b.ID measurementId,
            REPLACE(REPLACE(REPLACE(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
            a."ua"/1000 V
        FROM
            (SELECT a."data_date", a."ua", a."distline_name"
             FROM TEMP_DISTLINE_RUNDATA a
             GROUP BY a."data_date", a."ua", a."distline_name") a
        LEFT JOIN CIM_DISTMEASUREMENT b
            ON CONCAT(a."distline_name", 'C相计量点') = b.NAME
        WHERE SUBSTR(a."data_date", 0, 11) = ?
          AND b.COMPANY_ID = ?
        ORDER BY a."data_date"
    """
    cursor = conn.cursor()
    cursor.execute(sql, (date, company_id))
    columns = [desc[0].lower() for desc in cursor.description]  # 字段转小写
    results = []
    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        results.append(record)
    cursor.close()
    return results

def query_dm_transformer_data(conn, company_id, date):
    """查询配变运行数据"""
    sql = """
        SELECT
            b.ID measurementId,
            REPLACE(REPLACE(REPLACE(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
            a."pa" P_A,
            a."pb" P_B,
            a."pc" P_C,
            a."p" P,
            a."p" PQ_P_A,
            a."p" PQ_P_A_CALC,
            a."qa" Q_A,
            a."qb" Q_B,
            a."qc" Q_C,
            a."q" Q,
            a."q" PQ_P_R,
            a."q" PQ_P_R_CALC
        FROM
            (SELECT
                 a.*,
                 b."ia",
                 b."ib",
                 b."ic",
                 b."i",
                 b."ua",
                 b."ub",
                 b."uc"
             FROM
                 (SELECT *
                  FROM (SELECT *,
                               ROW_NUMBER() OVER(PARTITION BY x."cust_no", x."cust_name", x."data_date"
                                                 ORDER BY x."cust_no") AS rn
                        FROM TEMP_DISTTRAN_RUNDATA x
                        WHERE SUBSTR(x."data_date", 0, 11) = ?) a
                  WHERE rn = 1) a
             LEFT JOIN (SELECT *
                        FROM (SELECT *,
                                     ROW_NUMBER() OVER(PARTITION BY a."cust_no", a."cust_name", a."data_date"
                                                       ORDER BY a."cust_no") AS rn
                              FROM TEMP_DISTTRAN_VOL_RUNDATA a) a
                        WHERE rn = 1) b
                 ON a."cust_no" = b."cust_no" AND a."data_date" = b."data_date") a
        LEFT JOIN CIM_DISTMEASUREMENT b
            ON CONCAT(a."dittran_name", '计量点') = b.NAME
           AND b.COMPANY_ID = ?
        ORDER BY a."data_date"
    """
    cursor = conn.cursor()
    cursor.execute(sql, (date, company_id))
    columns = [desc[0].lower() for desc in cursor.description]
    results = []
    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        results.append(record)
    cursor.close()
    return results

def map_to_mongo_doc(source_dict):
    """将查询结果映射为 MongoDB 文档，缺失字段补 null"""
    doc = {}
    import decimal
    for field in MONGO_FIELDS:
        key = FIELD_MAPPING.get(field.lower(), field)  # 是否有映射
        value = source_dict.get(key.lower())
        # 如果是 decimal.Decimal 类型，转为 float，但排除 measurementId
        if field != "measurementId" and isinstance(value, decimal.Decimal):
            value = float(value)
        doc[field] = value if value is not None else None
    # 特殊处理：measurementId 转为 Long 类型（如果 pymongo 支持 bson.int64.Int64）
    if doc["measurementId"] is not None:
        try:
            from bson.int64 import Int64
            doc["measurementId"] = Int64(doc["measurementId"])
        except Exception:
            pass  # 或者保持为 int
    return doc

def insert_to_mongo(mongo_db, data_list):
    """插入数据到 MongoDB，按日期分表"""
    if not data_list:
        return
    # 取第一条数据的 measTime 来确定日期
    sample = data_list[0]
    meas_time = sample.get("meastime") or sample.get("measTime")
    if not meas_time:
        raise ValueError("Missing measTime in data")
    date_str = meas_time[:8]  # YYYYMMDD
    collection_name = f"cim_distmeasminute{date_str}"
    collection = mongo_db[collection_name]
    docs = [map_to_mongo_doc(row) for row in data_list]
    if docs:
        # 先删后插入，保证唯一索引数据为最新
        for doc in docs:
            filter_ = {"measurementId": doc["measurementId"], "measTime": doc["measTime"]}
            collection.delete_many(filter_)
        collection.insert_many(docs)
        print(f"✅ 已插入 {len(docs)} 条数据到集合 {collection_name}（已先删除重复数据）")

def main(company_id: str, date_str: str):
    """
    主函数
    :param company_id: 单位 ID，如 '781160878035460096'
    :param date_str: 日期字符串，格式 '2025-06-02'
    """
    # 连接达梦
    try:
        dm_conn = dmPython.connect(
            user=DM_DSN["user"],
            password=DM_DSN["password"],
            server=DM_DSN["server"],
            port=DM_DSN["port"]
        )
        print("✅ 成功连接到达梦数据库")
    except Exception as e:
        print(f"❌ 达梦连接失败: {e}")
        return

    # 连接 MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB_NAME]
        print("✅ 成功连接到 MongoDB")
    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        dm_conn.close()
        return

    try:
        # 查询数据
        
        trans_data = query_dm_transformer_data(dm_conn, company_id, date_str)
        line_data = query_dm_line_data(dm_conn, company_id, date_str)

        all_data = trans_data + line_data 
        # all_data = line_data 
        print(f"📊 查询到配变数据 {len(trans_data)} 条")
        print(f"📊 查询到线路数据 {len(line_data)} 条")
        

        # 去重：根据 measurementId + measTime
        unique_keys = set()
        filtered_data = []
        for item in all_data:
            key = (item.get('measurementid'), item.get('meastime') or item.get('measTime'))
            if key not in unique_keys:
                unique_keys.add(key)
                filtered_data.append(item)

        print(f"🔍 去重后共 {len(filtered_data)} 条数据")

        # 插入 MongoDB
        insert_to_mongo(mongo_db, filtered_data)

    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
    finally:
        dm_conn.close()
        client.close()
        print("🔗 连接已关闭")

# -----------------------------
# 使用示例
# -----------------------------
if __name__ == "__main__":
    COMPANY_ID = "781162051559776256"
    # DATE = "2025-06-02"
    # main(COMPANY_ID, DATE)
    for day in range(2, 11):
        DATE = f"2025-06-{day:02d}"
        print(f"\n===== C相正在处理日期: {DATE} =====")
        main(COMPANY_ID, DATE)