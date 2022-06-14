from psycopg2 import sql, connect
import lxml.html

conn_string_gis = "host='localhost' dbname='gis' user='chris' password='chris'"
conn_gis = connect(conn_string_gis)
conn_gis.autocommit = False
cursor_gis = conn_gis.cursor()

#booking_number = '1904965'
cursor_gis.execute("select page_text, booking_number from inmate_information where page_text != '' and bail_table_processed = False")
for data in cursor_gis.fetchall():
    page = data[0]
    tree = lxml.html.fromstring(page)
    booking_number = data[1]
    try:
        inmate_id = tree.xpath('/html/body/table[2]/tr[4]/td[3]/table/tr[3]/td/table/tr/td/table/tr/td/table[2]/tr[3]/td[2]')[0].text_content()
        rows = tree.xpath('/html/body/table[2]/tr[4]/td[3]/table/tr[3]/td/table/tr/td/table/tr/td/table[6]')
        print('Inmate id: {}, Booking id: {}'.format(inmate_id, booking_number))
    except IndexError:
        continue

    mapping = {}
    for j,row in enumerate(rows[0].getchildren()[1:]):
        mapping['booking_number'] = booking_number
        mapping['inmate_id'] = inmate_id
        if 'There is no Bond Information for this Inmate.' in row[0].text_content().strip():
            continue
        if row[0].text_content().strip() == 'Case #:': # Start of a case row
            mapping['case_number'] = row[1].text_content().strip()
            mapping['amount'] = row[3].text_content().strip()
            mapping['percent'] = row[5].text_content().strip()
            mapping['additional'] = row[7].text_content().strip()
            mapping['total'] = row[9].text_content().strip()
            if mapping['amount'] == '':
                del mapping['amount']
            if mapping['percent'] == '':
                del mapping['percent']
            if mapping['total'] == '':
                del mapping['total']
            if mapping['additional'] == '':
                del mapping['additional']
        elif 'Bond' in row[0].text_content().strip():
            mapping['bond_type'] = row[1].text_content().strip()
            mapping['status'] = row[3].text_content().strip()
            mapping['posted_by'] = row[5].text_content().strip()
            mapping['post_date'] = row[7].text_content().strip()
            if mapping['post_date'] == '':
                del mapping['post_date']
            print(mapping)
            sql_query = sql.SQL("INSERT INTO inmate_information_bail ({columns}) VALUES ({data})").format(
                columns=sql.SQL(', ').join(
                    sql.Composed([sql.Identifier(k)]) for k in mapping.keys()
                ),
                data=sql.SQL(', ').join(
                    sql.Composed([sql.Placeholder(k)]) for k in mapping.keys()
                ),
                ex_data=sql.SQL(', ').join(
                    sql.Composed([sql.SQL("EXCLUDED."), sql.Identifier(k)]) for k in mapping.keys()
                ),
            )
            cursor_gis.execute(sql_query, mapping)
            cursor_gis.execute("update inmate_information set bail_table_processed = True where booking_number = %s", (booking_number,))
            conn_gis.commit()

        elif 'Grand' in row[0].text_content().strip():
            pass
        #    mapping['grand_total'] = row[1].text_content().strip()

conn_gis.close()
