from bs4 import BeautifulSoup
import dryscrape
import time
import requests
from data_model import data_access


def watToWaitComponentLoaded(session, waifor):
    """
    :param session:
    :param waifor: [(name=None, attrs={}), (name=None, attrs={})]
    :return:
    """
    soup = BeautifulSoup(session.body(), 'lxml')
    if all([soup.find(w[0], attrs=w[1]) for w in waifor]):
        return True
    else:
        return False


def watToWait(session):
    soup = BeautifulSoup(session.body(), 'lxml')
    column = soup.findAll('td', {'class': 'n'})
    tables = soup.find('table', attrs={'id': 'tblOverviewSummary'})
    if column and tables:
        return True
    else:
        return False


def parse_table(soup, table_id):
    tables = soup.find('table', attrs={'id': table_id})
    table_body = tables.find('tbody')
    rows = table_body.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])  # Get rid of empty values
    return data


def parse_by_id(soup, id):
    element = soup.find(attrs={'id': id})
    return element.text.strip()


def parse_title(soup):
    title = soup.find('title')
    return title.text.strip()


def parse_class(soup, classname):
    classes = soup.find_all(attrs={'class': classname})
    data = [c.text.strip() for c in classes]
    return data


def parse_tr_by_class(soup, classname):
    trs = soup.find_all('tr', attrs={'class': classname})
    data = []
    for tr in trs:
        tds = tr.find_all('td')
        cells = [td.text.strip() for td in tds]
        data.append([cell for cell in cells])
    return data


def convert_string_to_numeric(val_str):
    val_str = val_str.replace(',', '')
    val = 0
    if val_str.endswith('B'):
        val = float(val_str.replace('B', '')) * 1000000000
    else:
        val = float(val_str)
    return val




if __name__ == '__main__':
    stockcode = ('VIC', 'http://ezsearch.fpts.com.vn/Services/EzData/default2.aspx?language=VN&s=229')

    session = dryscrape.Session()
    session.visit(stockcode[1])

    # wait for full page loaded
    # time.sleep(5)
    session.wait_for(lambda: watToWait(session))

    response = session.body()
    # print(response)
    soup = BeautifulSoup(response, 'lxml')
    # print(soup.prettify())

    # use soup.findAll("table") instead of find() and decompose() and nothing gets destroyed/destructed.
    print('parse tblOverviewSummary')
    data = parse_table(soup, 'tblOverviewSummary')
    f = lambda A, n=3: [A[i:i + n] for i in range(0, len(A), n)]
    rows = []
    for d in data:
        for r in f(d, 2):
            row = [stockcode[0], r[0], convert_string_to_numeric(r[1]), 'TONGQUAN', 'N/A']
            rows.append(row)

    print('parse company name')
    company_name = parse_by_id(soup, 'lblCompanyName')
    print(company_name)

    title = parse_title(soup)
    print(title)

    # thong tin tai chinh co ban
    updateto = parse_by_id(soup, '_ctl0_CenterInfo1_lblBalanceDate')
    print(updateto)
    data = parse_tr_by_class(soup, ['DgrItemStyle', 'DgrAlternatingItemStyle'])
    for d in data:
        if all(e for e in d):
            row = [stockcode[0], d[0], convert_string_to_numeric(d[1]), 'CHISOTAICHINHCOBAN', updateto]
            rows.append(row)

    data_access.insert_or_replace(data_access.connection, 'FPTS', rows[0].keys(), rows)
    data_access.connection.commit()
    data_access.connection.close()
