# 相对导入（仅包内有效）
from .hello import say_hello

if __name__ == "__main__":
    print(say_hello("相对导入"))
