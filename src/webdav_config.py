# webdav_config.py

from wsgidav.fs_dav_provider import FilesystemProvider
from wsgidav.wsgidav_app import WsgiDAVApp
from cheroot.wsgi import Server as WSGIServer
# from wsgidav.util import resolve_root_path
# import os

# ==============================
# 1. 配置文件提供者 (Providers)
# ==============================
c_drive_provider = FilesystemProvider(r"C:\\")  # C盘
d_drive_provider = FilesystemProvider(r"D:\\")  # D盘

# ==============================
# 2. 配置服务器
# ==============================
config = {
    "host": "0.0.0.0",  # 允许局域网内其他设备访问
    "port": 8080,         # WebDAV 服务端口
    "server": "cheroot", # 使用 cheroot 服务器
    "ssl_certificate": None,
    "ssl_private_key": None,
    "dir_browser": {
        "enable": True,
        "enable_props": True,
        "enable_readme": True,
        "enable_content_length": True,
        "ms_sharepoint_support": None,
        "response_trailer": ""
    },
    "http_authenticator": {
        "domain_controller": None,
        "accept_basic": True,
        "accept_digest": False,
        "default_to_digest": False,
    },
    "provider_mapping": {
        "/": c_drive_provider,
        "/D": d_drive_provider,
        "/d": d_drive_provider,  # 兼容小写/d
    },
    "simple_dc": {
        "user_mapping": {
            "/": {
                "admin": {
                    "password": "123456",
                    "roles": ["admin"]
                }
            },
            "/D": {
                "admin": {
                    "password": "123456",
                    "roles": ["admin"]
                }
            },
            "/d": {
                "admin": {
                    "password": "123456",
                    "roles": ["admin"]
                }
            },
            "*": {
                "admin": {
                    "password": "123456",
                    "roles": ["admin"]
                }
            },
            None: {
                "admin": {
                    "password": "123456",
                    "roles": ["admin"]
                }
            }
        }
    },
    "simple_logger": {
        "file_path": "wsgidav.log",
        "level": "INFO"
    },
    "verbose": 1,
}



# # 将提供者映射到 URL 路径
# config["provider_mapping"] = {
#     "/": c_drive_provider,     # 根路径 / 显示 C:\
#     "/D": d_drive_provider,    # /D 路径显示 D:\
#     # 可以添加更多，如 "/E": FilesystemProvider(r"E:\\")
# }

# ==============================
# 3. (可选) 添加钩子以美化根目录显示
# ==============================
def add_d_drive_link(app):
    """一个简单的钩子，在根目录的 HTML 列表中添加一个指向 /D 的链接"""
    # 这只是一个概念。wsgidav 的 dir_browser 不直接支持此功能。
    # 更好的方法是理解 dir_browser 的模板，但这很复杂。
    # 我们依赖用户知道 /D 这个路径存在。
    pass

# 如果你想完全自定义根目录，可以创建一个虚拟的 "根" 文件夹，里面放指向 C:\ 和 D:\ 的快捷方式，
# 然后让 provider_mapping["/"] 指向这个虚拟文件夹。但这更复杂。

# ==============================
# 4. 创建 WSGI 应用
# ==============================
app = WsgiDAVApp(config)

# 如果需要，可以包装应用（例如添加 CORS 等）
# application = SomeMiddleware(app)
application = app # WSGI 入口点

# ==============================
# 5. (可选) 直接运行服务器 (方便测试)
# ==============================
if __name__ == "__main__":
    print("WebDAV 服务器启动中...")
    print(f"  地址: http://{config['host']}:{config['port']}/")
    print(f"  C 盘: http://{config['host']}:{config['port']}/")
    print(f"  D 盘: http://{config['host']}:{config['port']}/D")
    print("  用户名: admin")
    print(f"  密码: {config['simple_dc']['user_mapping']['/']['admin']['password']}")
    print("按 Ctrl+C 停止服务器...")

    server = WSGIServer((config["host"], config["port"]), app)
    try:
        server.start()
    except KeyboardInterrupt:
        print("停止服务器...")
        server.stop()