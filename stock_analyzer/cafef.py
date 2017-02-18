from stock_analyzer import web_scraping1_Scraping_with_JS_support as web_scraping1
import dryscrape
from bs4 import BeautifulSoup
import pandas

if __name__ == '__main__':
    stockcode = ('VIC', 'http://s.cafef.vn/bao-cao-tai-chinh/VIC/BSheet/2016/4/0/0/ket-qua-hoat-dong-kinh-doanh-tap-doan-vingroup-cong-ty-co-phan.chn')

    session = dryscrape.Session()
    session.visit(stockcode[1])

    # wait for full page loaded
    # time.sleep(5)
    session.wait_for(lambda: web_scraping1.watToWaitComponentLoaded(session, waifor=[('table', {'id':'tableContent'}),]))
    response = session.body()
    soup = BeautifulSoup(response, 'lxml')

    data = web_scraping1.parse_class(soup, 'h_t')
    print(data)

    data = web_scraping1.parse_table(soup, 'tableContent')
    for e in data:
        print(e)