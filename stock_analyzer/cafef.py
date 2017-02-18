from stock_analyzer import web_scraping1_Scraping_with_JS_support as web_scraping1
import dryscrape
from bs4 import BeautifulSoup
import pandas
import datetime
from data_model import data_access

if __name__ == '__main__':
    stock = 'VIC'
    year = 2016
    stockcode = ('VIC', 'http://s.cafef.vn/bao-cao-tai-chinh/VIC/BSheet/2016/4/0/0/ket-qua-hoat-dong-kinh-doanh-tap-doan-vingroup-cong-ty-co-phan.chn')

    session = dryscrape.Session()
    session.visit(stockcode[1])

    # wait for full page loaded
    # time.sleep(5)
    session.wait_for(lambda: web_scraping1.watToWaitComponentLoaded(session, waifor=[('table', {'id':'tableContent'}),]))
    response = session.body()
    soup = BeautifulSoup(response, 'lxml')
    # print(soup.prettify())
    quarter = web_scraping1.parse_class(soup, 'h_t')
    columns = ['LOAIDL']+ quarter
    # print(columns)
    data = web_scraping1.parse_table(soup, 'tableContent')
    for e in data:
        print(e)

    datadf = pandas.DataFrame(data, columns=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'] )
    datadf = datadf.dropna(subset=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'], how='all')
    datadf['Q1'] = ([v.replace(',', '') if v else v for v in datadf['Q1']])
    datadf['Q2'] = ([v.replace(',', '') if v else v for v in datadf['Q2']])
    datadf['Q3'] = ([v.replace(',', '') if v else v for v in datadf['Q3']])
    datadf['Q4'] = ([v.replace(',', '') if v else v for v in datadf['Q4']])

    datadf['STOCK'] = stock
    datadf['Y'] = year
    datadf['NOTE'] = 'NA'
    datadf['CREATEDDT'] = datetime.datetime.now().strftime('%Y-%m-%y %H:%M%S')
    datadf['UPDATEDDT'] = datetime.datetime.now().strftime('%Y-%m-%y %H:%M%S')
    datadf = datadf.reset_index(drop=True)
    datadf['IDX'] = datadf.index
    # datadf.index += 1
    # print(datadf.head(10))
    # print(datadf.columns.values)
    # print(datadf.values.tolist())


    data_access.insert_or_replace(data_access.connection, 'CAFEF', datadf.columns.values, datadf.values.tolist())
    data_access.connection.commit()
    data_access.connection.close()