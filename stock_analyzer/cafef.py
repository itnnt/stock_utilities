from stock_analyzer import web_scraping1_Scraping_with_JS_support as web_scraping1
import dryscrape
from bs4 import BeautifulSoup
import pandas
import datetime
from data_model import data_access
import sqlite3
import requests

def update(stock, year, url):
    """

    :param stock:
    :param year:
    :param url:
    :return: dataframe
    """
    session = dryscrape.Session()
    # we don't need images
    session.set_attribute('auto_load_images', False)
    session.visit(url)

    # wait for full page loaded
    # time.sleep(5)
    session.wait_for(
        lambda: web_scraping1.watToWaitComponentLoaded(session, waifor=[('table', {'id': 'tableContent'}), ]))
    response = session.body()
    soup = BeautifulSoup(response, 'lxml')
    # print(soup.prettify())
    quarter = web_scraping1.parse_class(soup, 'h_t')
    columns = ['LOAIDL'] + quarter
    # print(columns)
    data = web_scraping1.parse_table(soup, 'tableContent')
    for e in data:
        print(e)
    try:
        datadf = pandas.DataFrame(data, columns=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'])
    except AssertionError as err:
        try:
            datadf = pandas.DataFrame(data, columns=['DATATYPE', ])
        except AssertionError as err:
            pass
    try:
        datadf = datadf.dropna(subset=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'], how='all')
        datadf['Q1'] = ([v.replace(',', '') if v else v for v in datadf['Q1']])
        datadf['Q2'] = ([v.replace(',', '') if v else v for v in datadf['Q2']])
        datadf['Q3'] = ([v.replace(',', '') if v else v for v in datadf['Q3']])
        datadf['Q4'] = ([v.replace(',', '') if v else v for v in datadf['Q4']])
    except KeyError as kerr:
        try:
            datadf = datadf.dropna(subset=['DATATYPE', ], how='all')
        except KeyError as kerr:
            pass
    datadf['STOCK'] = stock
    datadf['Y'] = year
    datadf['NOTE'] = 'NA'
    datadf['CREATEDDT'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M%S')
    datadf['UPDATEDDT'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M%S')
    print(datetime.datetime.now())
    datadf = datadf.reset_index(drop=True)
    datadf['IDX'] = datadf.index
    # datadf.index += 1
    # print(datadf.head(10))
    # print(datadf.columns.values)
    # print(datadf.values.tolist())
    return datadf


if __name__ == '__main__':
    year = 2011
    cafef_stocks = data_access.get_stock_setting('CAFEF_SETTING', 'STOCK,NAME,LINK,TYPE')
    for st in cafef_stocks:
        if st['TYPE'] == 'CAN_DOI_KE_TOAN':
            df = update(st['STOCK'], year, st['LINK'].format(year))
            try:
                data_access.insert(data_access.connection, 'CAFEF_CAN_DOI_KE_TOAN', df.columns.values, df.values.tolist())
            except sqlite3.IntegrityError as er:
                print(er)
                pass
        if st['TYPE'] == 'KET_QUA_KINH_DOANH':
            df = update(st['STOCK'], year, st['LINK'].format(year))
            try:
                data_access.insert(data_access.connection, 'CAFEF_KET_QUA_KINH_DOANH',
                                          df.columns.values, df.values.tolist())
            except sqlite3.IntegrityError as er:
                print(er)
                pass
        if st['TYPE'] == 'LUU_CHUYEN_TIEN_TE_GIAN_TIEP':
            df = update(st['STOCK'], year, st['LINK'].format(year))
            try:
                data_access.insert(data_access.connection, 'CAFEF_LUU_CHUYEN_TIEN_TE_GIAN_TIEP',
                                          df.columns.values, df.values.tolist())
            except sqlite3.IntegrityError as er:
                print(er)
                pass
        if st['TYPE'] == 'LUU_CHUYEN_TIEN_TE_TRUC_TIEP':
            df = update(st['STOCK'], year, st['LINK'].format(year))
            print(df.head(len(df)))
            try:
                data_access.insert(data_access.connection, 'CAFEF_LUU_CHUYEN_TIEN_TE_TRUC_TIEP',
                                          df.columns.values, df.values.tolist())
            except sqlite3.IntegrityError as er:
                print(er)
                pass
        data_access.connection.commit()
    data_access.connection.close()


    # THAM KHAO SUBMIT FORM ---------------------------------------------------------------------------------------------------
    # session = dryscrape.Session(base_url='http://cafef.vn')
    # # we don't need images
    # session.set_attribute('auto_load_images', False)
    # session.visit('/')
    # # session.wait_for(lambda: web_scraping1.watToWaitComponentLoaded(session, waifor=[('span', {'class': 's-submit'}), ]))
    #
    #
    # q = session.at_xpath('//*[@id="CafeF_SearchKeyword_Company"]')
    # q.set('VNH')
    # # q.form().submit()
    # submitbutton = session.at_xpath('//*[@class="s-submit"]')
    # submitbutton.click()
    #
    # print(session.body())
    # print(session.base_url)
