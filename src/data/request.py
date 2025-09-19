import requests
import json
import time
import redis
#  自动执行脚本，定时触发
# 每天8号、9号、10号，计算前一天的数据
def get_latest_token_from_redis():
    # 连接 Redis（如有密码请补充）
    r = redis.Redis(host='redis.d-guangzhou-llt-dameng.svc.dfsoft.com.cn', port=6379, db=0,password='df@Redis@2024Op' , decode_responses=True)
    # 获取分值最大的 token
    result = r.zrevrange('token_dfsoft', 0, 0)
    if result:
        return result[0]
    else:
        raise Exception('Redis 未获取到 token_dfsoft')

url = "http://main.d-guangzhou-llt-dameng.k8s.dfsoft.com.cn/api/ltc/calcTask/offline/dist"
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "http://main.d-guangzhou-llt-dameng.k8s.dfsoft.com.cn",
    "Pragma": "no-cache",
    "Referer": "http://main.d-guangzhou-llt-dameng.k8s.dfsoft.com.cn/ltc/hand-computation/32100",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
}
cookies = {
    "_ga": "GA1.1.134510562.1750150380",
    "_ga_YFKNQX5E65": "GS2.1.s1757552710$o70$g0$t1757552713$j57$l0$h0",
    "accessToken": get_latest_token_from_redis()
}

def run_batch_requests():
    for day in range(8, 11):
        date_str = f"2025-06-{day:02d}"
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        data = {
            "belongCompanyId": "773222052638515200",
            "calcType": 0,
            "gridType": "DIST",
            "calcScopeDistAddDTOList": [
                {
                    "companyId": "781162051559776256",
                    "objectId": "781162051559776256",
                    "objectName": "HPGDJ",
                    "psrTypeCode": "COMPANY"
                }
            ],
            "taskName": f"{now_str} 配电线路计算任务",
            "whetherSaveAndExecute": 1,
            "interval": 15,
            "calcParameterDistAddDTO": {
                "isFit": 0,
                "calcMethod": "NEWTONRAPHSON",
                "dataTimeType": 1,
                "meteloss": "6",
                "temperature": "20",
                "dataTime": date_str
            }
        }
        print("计算日期:", data["calcParameterDistAddDTO"]["dataTime"])
        response = requests.post(
            url,
            headers=headers,
            cookies=cookies,
            data=json.dumps(data, ensure_ascii=False),
            verify=False
        )
        print("Status Code:", response.status_code)
        print("Response Body:", response.text)
        if response.status_code == 200:
            try:
                print("JSON Response:", response.json())
            except json.JSONDecodeError:
                print("Response is not JSON format.")
        else:
            print("Request failed.")
        # 每隔5分钟触发一次
        if day != 10:
            print("等待5分钟...")
            time.sleep(300)

if __name__ == "__main__":
    run_batch_requests()
    # get_latest_token_from_redis()