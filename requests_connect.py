import requests

HEADER = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36 Edg/97.0.1072.55",
    "Accept": "application/json", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "sec-ch-ua-platform": "Windows",
    "Sec-Fetch-Mode": "cors"}


def connect():
    r = requests.get('http://data.10jqka.com.cn/market/rzrq/board/ls/field/rzjmr/order/desc/page/2/ajax/1/',
                     headers=HEADER)
    print(r.text)


if __name__ == '__main__':
    connect()
