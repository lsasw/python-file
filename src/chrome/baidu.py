from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome()
# ...existing code...
driver.get("https://www.baidu.com")

# 等待页面加载
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

# 处理可能的弹窗
try:
    agree_btn = WebDriverWait(driver, 3).until(
        EC.element_to_be_clickable((By.XPATH, "//span[text()='同意' or text()='接受' or text()='知道了']"))
    )
    agree_btn.click()
    time.sleep(1)
except Exception:
    pass
# 获取页面标题
print(driver.title)
# 通过JS获取当前光标闪动的输入框（即获得焦点的input）
active_element = driver.execute_script("return document.activeElement;")

# 检查是否为input元素
if active_element.tag_name == "input":
    search_box = active_element
else:
    # 兜底：用常规方式查找
    search_box = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input"))
    )

# 输入内容
search_box.send_keys("Runoob")
search_box.send_keys(Keys.RETURN)
time.sleep(2)
driver.quit()