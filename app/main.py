import threading
import time  # try to connect every 0.1s
import os  # get Env var
import csv  # cvs file
import codecs  # utf-8 | cp1251 encoding

import psycopg2

import subprocess
import shutil
import schedule


def timer():
    global time_start
    time_elapsed = time.time()
    result = time_elapsed - time_start
    time_start = time_elapsed
    return result


def connect_to_bd():
    # this file has to run after database deployment
    print("\nConnecting to database...")
    while True:
        try:
            conn = psycopg2.connect(dbname="zno_data",
                                    user="dreamTeam",
                                    password="dreamTeam",
                                    host="db")
            break
        except:
            time.sleep(0.1)
    print("Connected to database --> \t{};".format(timer()))
    return conn


def create_backup(_backup_file: str):
    _path = _backup_file[:_backup_file.rfind('/')]
    os.makedirs(_path, exist_ok=True)  # Create the backup directory if it doesn't exist
    open(_backup_file, 'w').close()
    dump_command = [
        'pg_dump',
        '--dbname=postgresql://{}:{}@{}:{}/{}'.format('dreamTeam', 'dreamTeam', 'db', '5432', 'zno_data'),
        '-F', 'c',
        '-b',
        '-v',
        '-f', _backup_file
    ]
    subprocess.run(dump_command, check=True)
    print('Backup created')


def restore_backup(_conn, _backup_file: str):
    if os.path.exists(_backup_file):
        restore_command = [
            'pg_restore',
            '--dbname=postgresql://{}:{}@{}:{}/{}'.format('dreamTeam', 'dreamTeam', 'db', '5432', 'zno_data'),
            '-F', 'c',
            '-v',
            _backup_file
        ]
        try:
            subprocess.run(restore_command, check=True)
            print(f'Database restored from reserve copy {_backup_file}')
        except Exception as e:
            print("Failed to restore from backup: ", e)

            create_bd(_conn)
            fill_db(_conn)
    else:
        print('Backup does not exist, proceed to recreate db')
        create_bd(_conn)
        fill_db(_conn)


def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)


def create_bd(_conn):
    print("\nStart creating tbl_ZNOresults...")
    cursor = _conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tbl_zno_results(
            OUTID char(36) PRIMARY KEY,
            Birth int2 NOT NULL,
            SEXTYPENAME char(10) NOT NULL,
            REGNAME char(30) NOT NULL,
            AREANAME char(30) NOT NULL,
            TERNAME char(40) NOT NULL,
            REGTYPENAME varchar(120) NULL,
            TerTypeName char(30) NULL,
            ClassProfileNAME char(40) NULL,
            ClassLangName char(10) NULL,
            EONAME varchar(300) NULL,
            EOTYPENAME char(60) NULL,
            EORegName char(30) NULL,
            EOAreaName char(50) NULL,
            EOTerName char(40) NULL,
            EOParent varchar(300) NULL,
            UkrTest char(30) NULL,
            UkrTestStatus char(20) NULL,
            UkrBall100 char(5) NULL,
            UkrBall12 char(5) NULL,
            UkrBall char(5) NULL,
            UkrAdaptScale char(5) NULL,
            UkrPTName varchar(300) NULL,
            UkrPTRegName varchar(300) NULL,
            UkrPTAreaName char(50) NULL,
            UkrPTTerName char(50) NULL,
            histTest char(40) NULL,
            HistLang char(20) NULL,
            histTestStatus char(20) NULL,
            histBall100 char(20) NULL,
            histBall12 char(5) NULL,
            histBall char(5) NULL,
            histPTName varchar(300) NULL,
            histPTRegName varchar(300) NULL,
            histPTAreaName varchar(300) NULL,
            histPTTerName char(50) NULL,
            mathTest char(50) NULL,
            mathLang char(40) NULL,
            mathTestStatus char(20) NULL,
            mathBall100 char(20) NULL,
            mathBall12 char(20) NULL,
            mathBall char(5) NULL,
            mathPTName varchar(300) NULL,
            mathPTRegName varchar(300) NULL,
            mathPTAreaName varchar(300) NULL,
            mathPTTerName varchar(300) NULL,
            physTest char(50) NULL,
            physLang char(50) NULL,
            physTestStatus char(40) NULL,
            physBall100 char(20) NULL,
            physBall12 char(20) NULL,
            physBall char(20) NULL,
            physPTName varchar(300) NULL,
            physPTRegName varchar(300) NULL,
            physPTAreaName varchar(300) NULL,
            physPTTerName varchar(300) NULL,
            chemTest varchar(300) NULL,
            chemLang char(50) NULL,
            chemTestStatus char(50) NULL,
            chemBall100 char(40) NULL,
            chemBall12 char(20) NULL,
            chemBall char(20) NULL,
            chemPTName varchar(300) NULL,
            chemPTRegName varchar(300) NULL,
            chemPTAreaName varchar(300) NULL,
            chemPTTerName varchar(300) NULL,
            bioTest varchar(300) NULL,
            bioLang char(50) NULL,
            bioTestStatus char(50) NULL,
            bioBall100 char(40) NULL,
            bioBall12 char(20) NULL,
            bioBall char(20) NULL,
            bioPTName varchar(300) NULL,
            bioPTRegName varchar(300) NULL,
            bioPTAreaName varchar(300) NULL,
            bioPTTerName varchar(300) NULL,
            geoTest varchar(300) NULL,
            geoLang char(50) NULL,
            geoTestStatus char(50) NULL,
            geoBall100 char(40) NULL,
            geoBall12 char(20) NULL,
            geoBall char(20) NULL,
            geoPTName varchar(300) NULL,
            geoPTRegName varchar(300) NULL,
            geoPTAreaName varchar(300) NULL,
            geoPTTerName varchar(300) NULL,
            engTest varchar(300) NULL,
            engTestStatus char(50) NULL,
            engBall100 char(50) NULL,
            engBall12 char(40) NULL,
            engDPALevel char(30) NULL,
            engBall char(30) NULL,
            engPTName varchar(300) NULL,
            engPTRegName varchar(300) NULL,
            engPTAreaName varchar(300) NULL,
            engPTTerName varchar(300) NULL,
            fraTest varchar(300) NULL,
            fraTestStatus char(50) NULL,
            fraBall100 char(50) NULL,
            fraBall12 char(40) NULL,
            fraDPALevel char(30) NULL,
            fraBall char(5) NULL,
            fraPTName varchar(200) NULL,
            fraPTRegName varchar(200) NULL,
            fraPTAreaName varchar(200) NULL,
            fraPTTerName varchar(200) NULL,
            deuTest varchar(200) NULL,
            deuTestStatus char(50) NULL,
            deuBall100 char(50) NULL,
            deuBall12 char(30) NULL,
            deuDPALevel char(30) NULL,
            deuBall char(30) NULL,
            deuPTName varchar(200) NULL,
            deuPTRegName varchar(200) NULL,
            deuPTAreaName varchar(200) NULL,
            deuPTTerName char(200) NULL,
            spaTest varchar(200) NULL,
            spaTestStatus char(50) NULL,
            spaBall100 char(50) NULL,
            spaBall12 char(40) NULL,
            spaDPALevel char(30) NULL,
            spaBall char(5) NULL,
            spaPTName varchar(200) NULL,
            spaPTRegName varchar(200) NULL,
            spaPTAreaName varchar(200) NULL,
            spaPTTerName char(200) NULL,
            UMLTest char(30) NULL,
            UMLTestStatus char(20) NULL,
            UMLBall100 char(5) NULL,
            UMLBall12 char(5) NULL,
            UMLBall char(5) NULL,
            UMLAdaptScale char(5) NULL,
            UMLPTName varchar(300) NULL,
            UMLPTRegName char(30) NULL,
            UMLPTAreaName char(50) NULL,
            UMLPTTerName char(40) NULL,
            UkrSubTest char(5) NULL,
            MathDpaLevel char(30) NULL,
            MathStTest char(40) NULL,
            MathStLang char(20) NULL,
            MathStTestStatus char(20) NULL,
            MathStBall12 char(5) NULL,
            MathStBall char(5) NULL,
            MathStPTName varchar(300) NULL,
            MathStPTRegName char(30) NULL,
            MathStPTAreaName char(50) NULL,
            MathStPTTerName char(40) NULL
        );
        """
    )
    print("tbl_zno_results CREATED --> \t{};".format(timer()))


# populating

def insert_to_db(cursor, filename):
    # file's headers
    inputFile = codecs.open(filename, 'r', encoding='cp1251')
    column_names = inputFile.readline().replace(';', ', ')[:-1].rstrip(',')
    column_names = '(' + column_names.replace('\"', '') + ')'
    column_num = column_names.count(',') + 1

    # file's main data(records) processing
    reader = csv.reader(inputFile, delimiter=';')
    i: int = 0  # iter
    limit: int = 50  # max values to insert
    for row in reader:
        record = ';'.join(row).replace('\"', '').replace(
            '\'', '\'\'').replace('null', '0')
        record = record.split(';', column_num)[0:column_num]
        insert_data = '(\'' + '\', \''.join(record) + '\')'
        cursor.execute("INSERT INTO tbl_zno_results{heads} VALUES {values};".format(
            heads=column_names, values=insert_data))

        if i > limit & limit > 0:
            break

        i += 1

    inputFile.close()


def fill_db(_conn):
    cursor = _conn.cursor()
    print("\nInserting to tbl_zno_results...")

    insert_to_db(cursor, 'Odata{}File.csv'.format(os.getenv('DB_DATA_YEAR1')))
    print("Inserting #1 completed --> \t{};".format(timer()))

    insert_to_db(cursor, 'Odata{}File.csv'.format(os.getenv('DB_DATA_YEAR2')))
    print("Inserting #2 completed --> \t{};".format(timer()))


def execute_querries(_conn):
    cursor = _conn.cursor()
    print("\nStart query executing...")
    # querry var=2
    cursor.execute("""
        SELECT REGNAME, AVG(CAST(REPLACE(UkrBall100, ',', '.') AS DECIMAL)) as AVG_UkrBall100 
        FROM tbl_zno_results 
        WHERE (
        (UkrTestStatus = \'Зараховано\') AND 
        (UkrBall100 is not null) AND
        (REGNAME is not null))
        GROUP BY REGNAME
        ORDER BY AVG_UkrBall100;
        """)

    querry_data = cursor.fetchall()

    with open('../app/querry_result.csv', 'w') as outputFile:
        writer = csv.writer(outputFile)
        writer.writerows(querry_data)

    print("Query completed --> \t{};".format(timer()))


time_start = time.time()
backup_file = '../app/backups/backup_file.dump'
schedule.every().day.at("02:00").do(create_backup, backup_file)

conn = connect_to_bd()

scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.start()

restore_backup(conn, backup_file)
create_backup(backup_file)
execute_querries(conn)

# for testing
# print("\nQuerry data:")
# with open('../app/querry_result.csv', 'r') as test_file:
#    reader = csv.reader(test_file)
#    for row in reader:
#        print(''.join(row).replace(',', ''))

print("\n--python script main.py is done--")

conn.commit()
conn.close()
