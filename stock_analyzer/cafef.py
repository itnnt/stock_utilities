from stock_analyzer import web_scraping1_Scraping_with_JS_support as web_scraping1
import dryscrape
from bs4 import BeautifulSoup
import pandas
import datetime
from data_model import data_access
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

    datadf = pandas.DataFrame(data, columns=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'])
    datadf = datadf.dropna(subset=['DATATYPE', 'Q1', 'Q2', 'Q3', 'Q4'], how='all')
    datadf['Q1'] = ([v.replace(',', '') if v else v for v in datadf['Q1']])
    datadf['Q2'] = ([v.replace(',', '') if v else v for v in datadf['Q2']])
    datadf['Q3'] = ([v.replace(',', '') if v else v for v in datadf['Q3']])
    datadf['Q4'] = ([v.replace(',', '') if v else v for v in datadf['Q4']])

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
    year = 2016
    cafef_stocks = data_access.get_stock_setting('CAFEF_SETTING', 'STOCK,NAME,LINK,TYPE')
    for st in cafef_stocks:
        if st['TYPE'] == 'CAN_DOI_KE_TOAN':
            can_doi_ke_toandf = update(st['STOCK'], year, st['LINK'].format(year))
            data_access.insert_or_replace(data_access.connection, 'CAFEF_CAN_DOI_KE_TOAN', can_doi_ke_toandf.columns.values, can_doi_ke_toandf.values.tolist())
        if st['TYPE'] == 'KET_QUA_KINH_DOANH':
            can_doi_ke_toandf = update(st['STOCK'], year, st['LINK'].format(year))
            data_access.insert_or_replace(data_access.connection, 'CAFEF_KET_QUA_KINH_DOANH',
                                          can_doi_ke_toandf.columns.values, can_doi_ke_toandf.values.tolist())

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
