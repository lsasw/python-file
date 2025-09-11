from pymongo import MongoClient
from datetime import datetime, timedelta


def process_transformer(tableName):
    client = MongoClient(
        "mongodb://d_eagle3_guangzhou_llt_meas:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_meas"
    )
    client_cim = MongoClient(
        "mongodb://d_eagle3_guangzhou_llt_cim:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_cim"
    )
    db = client["d_eagle3_guangzhou_llt_meas"]
    db_cim = client_cim["d_eagle3_guangzhou_llt_cim"]
    collection_source = db[tableName]
    collection_gear = db["TEMP_GEAR_INFO"]
    collection_target = db_cim["TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL"]

    gear_info_map = {}
    for doc in collection_gear.find():
        disttran_name = doc.get("disttran_name")
        gear_info_map[disttran_name] = {
            "gear_type": doc.get("gear_type"),
            "gear_status": doc.get("gear_status"),
        }

    cursor = collection_source.find(
        {
            "companyName": "HZGDJ",
            "distributionLineName": "10kV东平线F11",
            # "tranType": tran_type
        }
    )

    results = []
    for a in cursor:
        tran_id = a.get("tranId")
        tran_name = a.get("tranName")
        data_time = a.get("dataTime")
        tran_type = a.get("tranType")
        if data_time:
            if isinstance(data_time, str):
                try:
                    data_time = datetime.fromisoformat(data_time)
                except Exception:
                    pass
            data_time = data_time + timedelta(hours=8)

        gear = gear_info_map.get(tran_name, None)
        gear_type = None
        gear_status = None
        bianbi = None
        if tran_type == "PUBLIC":
            if gear:
                gear_type = gear["gear_type"]
                gear_status = gear["gear_status"]
                if gear_type == 5:
                    if gear_status == 1:
                        bianbi = 26.25
                    elif gear_status == 2:
                        bianbi = 25.625
                    elif gear_status == 3:
                        bianbi = 25.0
                    elif gear_status == 4:
                        bianbi = 24.375
                    elif gear_status == 5:
                        bianbi = 23.75
                elif gear_type == 3:
                    if gear_status == 1:
                        bianbi = 26.25
                    elif gear_status == 2:
                        bianbi = 25.0
                    elif gear_status == 3:
                        bianbi = 23.75

            voltage_raw = a.get("voltage", 0)
            voltage = 10 if voltage_raw == 0 else voltage_raw
            if bianbi is None or bianbi == 0:
                print(
                    f"警告：变压器 {tran_id} 缺少有效变比 (gear_type={gear_type}, gear_status={gear_status})，跳过"
                )
                continue
            lowVolt = round(voltage / bianbi * 1000 / 1.732, 2)
        else:
            voltage_raw = a.get("voltage", 0)
            voltage = 10 if voltage_raw == 0 else voltage_raw
            lowVolt = None
        highVolt = round(voltage, 2)
        result_doc = {
            "TRAN_ID": tran_id,
            "TRAN_NAME": tran_name,
            "DISTRIBUTION_LINE_ID": a.get("distributionLineId"),
            "DISTRIBUTION_LINE_NAME": a.get("distributionLineName"),
            "DATA_TIME": data_time,
            "highVolt": highVolt,
            "lowVolt": lowVolt,
            "gear_type": gear_type,
            "gear_status": gear_status,
            "INSERT_TIME": datetime.now(),
            "DATA_TIME_SIGN": data_time.strftime("%Y-%m-%d")
            if isinstance(data_time, datetime)
            else str(data_time),
            "tran_type":tran_type
        }
        results.append(result_doc)

    if results:
        distribution_line_name = (
            results[0]["DISTRIBUTION_LINE_NAME"] if results else None
        )
        data_time_sign = results[0]["DATA_TIME_SIGN"] if results else None
        if distribution_line_name and data_time_sign:
            print("开始删除旧数据...")
            collection_target.delete_many(
                {
                    "DISTRIBUTION_LINE_NAME": distribution_line_name,
                    "DATA_TIME_SIGN": data_time_sign,
                }
            )
        collection_target.insert_many(results)
        print(
            f"✅ {tran_type} 成功插入 {len(results)} 条记录到 {collection_target.name}"
        )
    else:
        print(f"❌ {tran_type} 没有符合条件的数据需要插入")
    client.close()


# 主程序入口
if __name__ == "__main__":
    for day in range(2, 11):
        tableName = f"LLT_DISTTRANSFORMERMINCALC202506{day:02d}"
        print(f"\n===== 正在处理表: {tableName} =====")
        process_transformer(tableName)
    # process_transformer("PRIVATE")
