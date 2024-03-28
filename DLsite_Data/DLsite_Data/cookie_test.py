import requests

# 构造请求头和请求参数
headers = {
 	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Referer": "https://dojindb.net",
}

# 新建Session对象
s = requests.Session()

# 发送GET请求，获取登录页面Cookie
s.get("https://dojindb.net", headers=headers)

# 发送POST请求，完成登录，并获取登录后Cookie
#s.post("https://dojindb.net/r/all", headers=headers)

# 获取登录成功后的Cookie
cookie_dict = s.cookies.get_dict()
print(cookie_dict)

url = 'https://dojindb.net/r/all/?g=comic'
print(requests.get(url,headers = headers,cookies = cookie_dict).status_code)