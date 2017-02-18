import sqlite3
import decimal
import os


def adapt_decimal(d):
    return str(d)


def convert_decimal(s):
    return D(s)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0].lower()] = row[idx]
    return d


def insert_or_replace(connection, table_name, col_names, rows):
    """
    :param connection:
    :param table_name:
    :param col_names: list or tuple
    :param rows: list[list] or list[tuple]
    :return:
    """
    field_names = ','.join(c.upper() for c in col_names)
    placeholders = ','.join('?' for i in col_names)
    sql = '''
           insert or replace into {0} ({1})
           values ({2})
       '''.format(table_name, field_names, placeholders)
    cursor = connection.cursor()
    cursor.executemany(sql, rows)
    return


# def insert_or_replace(connection, table_name, data):
#     """
#     :param connection:
#     :param table_name:
#     :param data: list[dict]
#     :return:
#     """
#     rows = []
#     for c in data:
#         row = tuple(c.values())
#         rows.append(row)
#
#     field_name = ','.join(c.keys())
#     placeholders = ','.join('?' for i in c.keys())
#     sql = '''
#            insert or replace into {0} ({1})
#            values ({2})
#        '''.format(table_name, field_name, placeholders)
#     cursor = connection.cursor()
#     cursor.executemany(sql, rows)
#     return

D = decimal.Decimal
# Register the adapter
sqlite3.register_adapter(D, adapt_decimal)
# Register the converter
sqlite3.register_converter("decimal", convert_decimal)
connection = sqlite3.connect('%s/stock_data.db' % os.path.dirname(os.path.realpath(__file__)))
connection.row_factory = dict_factory

# connection.commit()
# connection.close()
