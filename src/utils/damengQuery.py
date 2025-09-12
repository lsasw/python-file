# -*- coding: utf-8 -*-
import dmPython


# 数据库连接配置
DB_CONFIG = {
    "user": "d_eagle3_guangzhou_llt_cim",             # 用户名
    "password": "Dfdb19c",  
    "server": "192.168.0.78",   # 替换为你的达梦数据库 IP 和端口
    "port": "35236"        # 实例名或数据库名
}

# SQL 查询语句
SQL = """
SELECT ID 
FROM CIM_DISTMEASUREMENT 
WHERE PSR_ID IN (
    SELECT ID 
    FROM CIM_DISTTRANSFORMER 
    WHERE COMPANY_ID = ?
      AND DISTRIBUTION_LINE_ID = ?
      AND NAME NOT IN ('课堂水电站（含0.235MW小水电）', '仕礼混凝土专变', '课堂村牧原农牧#2专变')
);
"""

def query_measurement_ids(company_id, distribution_line_id):
    conn = None
    cursor = None
    try:
         # 建立连接
        conn = dmPython.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            server=DB_CONFIG["server"],
            port=DB_CONFIG["port"]
        )
        cursor = conn.cursor()

        # 执行查询
        cursor.execute(SQL, (company_id, distribution_line_id))

        # 获取所有结果
        results = cursor.fetchall()

        # 提取 ID 列（假设返回的是单列）
        ids = [row[0] for row in results]

        return ids

    except Exception as e:
        print(f"数据库错误: {e}")
        return []

    finally:
        # 关闭资源
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 主程序
if __name__ == "__main__":
    company_id = 1  # 替换为实际的公司 ID
    distribution_line_id = 123  # 替换为实际的配电线路 ID
    measurement_ids = query_measurement_ids(company_id, distribution_line_id)
    print("查询到的计量点 ID 列表：")
    for mid in measurement_ids:
        print(mid)