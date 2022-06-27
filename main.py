import logging
import mysql.connector
from mysql.connector import Error
import requests
import pandas as pd

# configuring Logger
log_format = '%(levelname)s %(asctime)s - %(message)s'
logging.basicConfig(filename='logs.log',
                    filemode='w',
                    format=log_format,
                    level=logging.INFO)
logger = logging.getLogger()


def connect_db(host, user, password, database):
    """making connection to mysql database"""
    connection = None
    logger.info('Connecting to database...')
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=password,
            database=database
        )
        print('DB connection successful')
        logger.info('DB connection successful')
    except Error as error:
        print(f'Error: {error}')
        logger.error(error)

    return connection


def execute_query(connection, query):
    """executing db manipulating queries, e.g. INSERT or UPDATE"""
    cursor = connection.cursor()
    logger.info(f'Executing given query: {query}')
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
        logger.info('Query executed successfully')
    except Error as error:
        print(f'Error: {error}')
        logger.error(error)


def read_query(connection, query):
    """reading data from db based on given query"""
    cursor = connection.cursor()
    result = None
    logger.info(f'Reading given query: {query}')
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        logger.info(f'Query read successfully')
        return result
    except Error as error:
        print(f'Error: {error}')
        logger.error(error)


def connect_to_endpoint(url):
    """
    :param url: endpoint
    :return: returns endpoint data in json format
    """
    logger.info(f'Connecting to given endpoint: {url}')
    response = requests.get(url)
    print(response.status_code)
    if response.status_code != 200:
        logger.error(f'{response.status_code}, {response.text}')
        raise Exception(response.status_code, response.text)
    logger.info(f'data downloaded')
    return response.json()


def main(db):
    """
    updating euro and usd unit prices in database
    :param db: mysql database connection
    """
    codes = ['USD', 'EUR']
    values = {}

    for code in codes:
        endpoint = f'http://api.nbp.pl/api/exchangerates/rates/A/{code}/today/'
        data = connect_to_endpoint(endpoint)
        rate = data['rates'][0]['mid']
        values[code] = rate

    eur_update = f'''
    update `mydb`.`product`
    set UnitPriceEuro = UnitPrice / {values['EUR']}
    '''

    usd_update = f'''
    update `mydb`.`product`
    set UnitPriceUSD = UnitPrice / {values['USD']}
    '''

    execute_query(db, eur_update)
    execute_query(db, usd_update)
    logger.info('Data updated')


def generate_excel_file(db):
    """
    generating excel file
    :param db: mysql database connection
    """
    logger.info('Generating xlsx file')
    query = '''
    select 
        ProductID
        , DepartmentID
        , Category
        , IDSKU
        , ProductName
        , Quantity
        , UnitPrice
        , UnitPriceUSD
        , UnitPriceEuro
        , Ranking
        , ProductDesc
        , UnitsInStock
        , UnitsInOrder
    from product
    '''
    df = pd.read_sql(query, db)
    writer = pd.ExcelWriter('data.xlsx')
    df.to_excel(writer, sheet_name='data')
    writer.save()
    logger.info('File generated')


db_connection = connect_db('localhost', 'michal', '', 'mydb')

try:
    main(db_connection)
finally:
    generate_file = input('Generate xlsx file? Y/N')
    if generate_file == 'Y':
        generate_excel_file(db_connection)
