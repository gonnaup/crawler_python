import json

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


def selenimu_test():
    with webdriver.Chrome(service=Service(ChromeDriverManager().install())) as driver:
        for start in range(0, 20, 20):
            driver.get(f'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={start}')
            json_data = driver.find_elements(by=By.TAG_NAME, value='pre')[0].text
            print(len(json.loads(json_data)))


if __name__ == '__main__':
    selenimu_test()
