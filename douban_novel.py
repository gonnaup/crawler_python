from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service=ChromeDriverManager().install())

if __name__ == '__main__':
    pass
