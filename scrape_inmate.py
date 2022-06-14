import requests
import argparse
import lxml.html
#import psycopg2
from psycopg2 import sql, connect
from datetime import datetime

parser = argparse.ArgumentParser(description='Scrape inmate information from Marion County, Indiana jail site')
parser.add_argument('-s', '--start', type=int, default='2203609', help='7 digit booking number to start with, first two are year')
parser.add_argument('-l', '--limit', type=int, default=10, help='Number of records to iterate')
parser.add_argument('-f', '--force-update', default=False, action='store_true', help='Force-update records already in database')

args = parser.parse_args()


create_db = """
create table inmate_information(
    page_text text,
    page_text_timestamp timestamp,
    first_name character varying(50),
    last_name character varying(50),
    inmate_id character varying(30),
    booking_number character varying(20),
    dob date,
    release_date date
)

"""

def get_inmate(booking_number, requests_session):

    URL = "http://inmateinfo.indy.gov/IML"
    page = requests_session.post(URL, data={
        'flow_action':	"searchbyid",
        'quantity':	"10",
        'systemUser_identifiervalue':	str(booking_number).zfill(7),
        'searchtype':	"PIN",
        'systemUser_includereleasedinmate':	"",
        'systemUser_includereleasedinmate2':	"Y",
        'systemUser_firstName':	"",
        'systemUser_lastName':	"",
        'systemUser_dateOfBirth':	"",
        'releasedA':	"checkbox",
        'identifierbox':	"PIN",
        'identifier':	str(booking_number).zfill(7),
        'releasedB':	"checkbox",
    })
    tree = lxml.html.fromstring(page.text)
    # Inmate Name (last, first middle)
    name_select = '#row1 > td:nth-child(1)'
    booking_number_select = '#row1 > td:nth-child(2)'
    permanent_id_select = '#row1 > td:nth-child(3)'
    dob_select = '#row1 > td:nth-child(4)'
    release_date_select = '#row1 > td:nth-child(5)'
    sysid_select = '.underlined'

    data = {}

#    try:
    no_record = tree.cssselect('.bodysmall > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > font:nth-child(1)')
    if len(no_record) > 0 and no_record[0].text_content() == 'No records found.':
        print('No records found.')
        #conn_gis.execute('INSERT INTO inmate_information ()')
        return None
    data['name'] = str(tree.cssselect(name_select)[0].text_content()).strip()
    data['first_name'] = str(tree.cssselect(name_select)[0].text_content()).split(',')[1].strip()
    data['last_name'] = str(tree.cssselect(name_select)[0].text_content()).split(',')[0].strip()
    data['booking_number'] = str(tree.cssselect(booking_number_select)[0].text_content()).strip()
    data['permanent_id'] = str(tree.cssselect(permanent_id_select)[0].text_content()).strip()
    try:
        data['dob'] = datetime.strptime(str(tree.cssselect(dob_select)[0].text_content()), '%m/%d/%Y').date()
    except ValueError as e:
        data['dob'] = None
    try:
        data['release_date'] =  datetime.strptime(str(tree.cssselect(release_date_select)[0].text_content()), '%m/%d/%Y').date()
    except ValueError as e:
        data['release_date'] = None
    data['sysid'] = str(tree.cssselect(sysid_select)[0].get('href').split("'")[1]).strip()
#    except Exception as e:
#        print(e)
    return data


def extract_inmate_details(booking_number):

    fields = ['full_name', 'sex','height','dob','weight','hair_color','eye_color',
        'race', 'ethnicity','state_id','police_id','fbi_id','ice_id',
        'marital_status','citizen','country_of_birth', 'commitment_date',
        'release_date', 'next_court_date'
        ]
    fields_lookup = {}
    fields_lookup['full_name_select'] = '.bodywhite'
    fields_lookup['sex_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(2)'
    fields_lookup['height_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(2) > td:nth-child(2)'
    fields_lookup['dob_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(4)'
    fields_lookup['weight_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(2) > td:nth-child(4)'
    fields_lookup['hair_color_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(4)'
    fields_lookup['race_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(2) > td:nth-child(4)'
    fields_lookup['eye_color_select'] = 'tr.bodysmall:nth-child(2) > td:nth-child(1) > div:nth-child(2) > table:nth-child(1) > tr:nth-child(4) > td:nth-child(4)'
    fields_lookup['ethnicity_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(3) > td:nth-child(4)'
    fields_lookup['state_id_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(4) > td:nth-child(2)'
    fields_lookup['police_id_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(5) > td:nth-child(2)'
    fields_lookup['fbi_id_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(6) > td:nth-child(2)'
    fields_lookup['ice_id_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(7) > td:nth-child(2)'
    fields_lookup['marital_status_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(4) > td:nth-child(4)'
    fields_lookup['citizen_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(5) > td:nth-child(4)'
    fields_lookup['country_of_birth_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(3) > tr:nth-child(6) > td:nth-child(4)'
    fields_lookup['commitment_date_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) >  tr:nth-child(1) > td:nth-child(1) > table:nth-child(5) > tr:nth-child(4) > td:nth-child(4)'
    fields_lookup['release_date_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(5) > tr:nth-child(5) > td:nth-child(4)'
    fields_lookup['next_court_date_select'] = 'body > table:nth-child(4) > tr:nth-child(4) > td:nth-child(3) > table:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > table:nth-child(15) > tr:nth-child(2) > td:nth-child(2)'
    cursor_gis.execute("select page_text from inmate_information where booking_number = '%s'", (booking_number,))
    page = cursor_gis.fetchone()[0]
    tree = lxml.html.fromstring(page)

    values = {}
    try:
        for field in fields:
            values[field] = tree.cssselect(fields_lookup['{}_select'.format(field,)])[0].text_content().strip()
    except (KeyError, IndexError):
        print('Index error while looking for specific field: {}'.format(field,))
    values['booking_number'] = booking_number
    empty_keys = [k for k,v in values.items() if (not v or v == '')]
    for k in empty_keys:
        del values[k]
    try:
        print(values['full_name'], values['commitment_date'])
    except (KeyError, IndexError):
        print('Unable to extract name or commitment_date')
    # hot shit via stack overflow: https://stackoverflow.com/a/59855303/2731298
    sql_query = sql.SQL("UPDATE inmate_information SET {data} WHERE booking_number = '{booking_number}'").format(
        data=sql.SQL(', ').join(
            sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in values.keys()
        ),
        booking_number=sql.Placeholder('booking_number')
    )
    cursor_gis.execute(sql_query, values)

def save_inmate_page(sysid, inmate_id, booking_number, requests_session):
    URL = 'http://inmateinfo.indy.gov/IML'
    data = {
        'flow_action': 'edit',
        'sysID': sysid,
        'imgSysID': '0',
    }
    page = requests_session.post(URL, data)
    tree = lxml.html.fromstring(page.text)
    try:
        booking_number_confirmation = tree.cssselect('body > table:nth-child(4)')[0].text_content()
        #print(booking_number_confirmation)
    except Exception as e:
        print('Here we are with an Exception {}'.format(e,))
    cursor_gis.execute('insert into inmate_information (inmate_id, booking_number, sysid, page_text_timestamp, page_text) values (%s, %s, %s, %s, %s)', (inmate_id, booking_number, sysid, datetime.now(), page.text) )


conn_string_gis = "host='localhost' dbname='gis' user='chris' password='chris'"
conn_gis = connect(conn_string_gis)
conn_gis.autocommit = True
cursor_gis = conn_gis.cursor()

with requests.Session() as requests_session:
    for booking_number in range(args.start,args.start+args.limit):
        cursor_gis.execute("select sysid, inmate_id from inmate_information where booking_number = '%s' and sysid != ''", (booking_number,))
        print('Booking Number: {}'.format(booking_number,))
        if cursor_gis.rowcount == 0:
            print('Fetching initial details...')
            data = get_inmate(booking_number, requests_session)
            if data is None:
                cursor_gis.execute("INSERT INTO inmate_information (booking_number, does_not_exist) VALUES ('%s', True)", (booking_number,))
                continue
        else:
            print('Already have booking_number and sysid in the db.')
            result = cursor_gis.fetchone()
        #    print(result)
            data = {}
            data['sysid'] = result[0]
            data['permanent_id'] = result[1]
            data['booking_number'] = booking_number
        cursor_gis.execute("select count(*) from inmate_information where booking_number = '%s' and page_text_timestamp is null ", (booking_number,))
        result = cursor_gis.fetchone()[0]
        if result == 0:
            print('Saving booking details page...')
            save_inmate_page(data['sysid'], data['permanent_id'], data['booking_number'], requests_session)
        else:
            print('Already having booking details saved.')
        cursor_gis.execute("select count(*) from inmate_information where booking_number = '%s' and dob is not null", (booking_number,))
        result = cursor_gis.fetchone()[0]
        if result == 0:
            print('Extracting details...')
            extract_inmate_details(booking_number)
        elif args.force_update:
            print('Already have analysis in db - FORCE UPDATING.')
        else:
            print('Already have analysis in db, skipping.')

cursor_gis.close()
conn_gis.close()
