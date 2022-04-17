import json

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

DIVER = webdriver.Chrome(service=Service(ChromeDriverManager().install()))


def selenium_doubanmovie(url) -> dict:
    DIVER.get(url)
    json_data = DIVER.find_elements(by=By.TAG_NAME, value='pre')[0].text
    return json.loads(json_data)


def close_webdriver():
    DIVER.quit()


if __name__ == '__main__':
    pass
