from datetime import date

from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager


def new_edge_webdriver():
    # webdrive init
    options = webdriver.EdgeOptions()
    # 去除顶部栏的 "Microsoft Edge 正由自动测试软件控制" 字样
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    driver = webdriver.Edge(options=options, service=EdgeService(EdgeChromiumDriverManager().install()))
    # 设置 window.navigator.webdriver=undefined
    # 否则 使用 drive.get() 启动浏览器时，window.navigator.webdriver属性为true，
    # 反爬虫脚本会根据此属性判断为爬虫脚本
    script = '''
            Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
            })
            '''
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': script})
    print('webdriver init successfully...')
    return driver


def plus_month(_date: date, month_add) -> date:
    months = _date.year * 12 + _date.month
    _month = months + month_add
    # 不考虑结果年份为负的情况
    if _month < 0:
        raise Exception(f'月份参数必须大于 {-months}')
    year = _month // 12
    month = _month % 12
    return __valid_day(year, month, _date.day)


def __valid_day(year: int, month: int, day: int) -> date:
    """
    如果日期大于当月最大天数，则调整日期至合规
    :param year:
    :param month:
    :param day:
    :return: 调整日期后的date
    """
    match month:
        case 2:
            day = min(day, 29 if __is_leap_year(year) else 28)
        case 4 | 6 | 9 | 11:
            day = min(day, 30)
    return date(year, month, day)


def __is_leap_year(year: int) -> bool:
    """
    年份是否是闰年（能被4整除的非整百年 或 能被400整除的年份）
    :return: 闰年 True，非闰年 False
    """
    return ((year & 3) == 0) and ((year % 100) != 0 or (year % 400) == 0)


if __name__ == '__main__':
    print(plus_month(date.today(), -83))
