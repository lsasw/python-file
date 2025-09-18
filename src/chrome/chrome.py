from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

# 设置驱动路径（如果已添加到系统PATH，可省略）
# driver_path = "path/to/chromedriver"  # 例如：C:/chromedriver/chromedriver.exe

# 创建浏览器实例
driver = webdriver.Chrome()  # 或者直接 webdriver.Chrome()

try:
    # 1. 打开网页
    driver.get("https://www.google.com")
    
    # 2. 查找搜索框
    search_box = driver.find_element(By.NAME, "q")
    
    # 3. 输入内容并搜索
    search_box.send_keys("Python Selenium tutorial")
    search_box.send_keys(Keys.RETURN)
    
    # 等待页面加载
    time.sleep(3)
    
    # 4. 获取搜索结果标题
    results = driver.find_elements(By.CSS_SELECTOR, "h3")
    for result in results[:5]:  # 打印前5个结果
        print(result.text)
        
finally:
    # 5. 关闭浏览器
    driver.quit()