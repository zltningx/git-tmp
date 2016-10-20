from bs4 import BeautifulSoup
import requests


url = 'http://web.sycsec.com/0b3a7c6ca7f1f2e6/'
s = requests.Session()
t = s.get(url)

times =t.text.count('@')


data = {
    'mytext': times-1
}

t = s.post("http://web.sycsec.com/0b3a7c6ca7f1f2e6/judge.php", data=data)

soup = BeautifulSoup(t.content, 'html5lib')
print(soup)