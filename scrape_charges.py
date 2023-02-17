from psycopg2 import sql, connect
import lxml.html

conn_string_gis = "host='localhost' dbname='gis' user='chris' password='chris'"
conn_gis = connect(conn_string_gis)
conn_gis.autocommit = False
cursor_gis = conn_gis.cursor()

#booking_number = 2203618
cursor_gis.execute("select page_text, booking_number from inmate_information where page_text != '' and charges_table_processed = False order by random() limit 10000")
for data in cursor_gis.fetchall():
    page = data[0]
    tree = lxml.html.fromstring(page)
    booking_number = data[1]
    try:
        inmate_id = tree.xpath('/html/body/table[2]/tr[4]/td[3]/table/tr[3]/td/table/tr/td/table/tr/td/table[2]/tr[3]/td[2]')[0].text_content()
        rows = tree.xpath('/html/body/table[2]/tr[4]/td[3]/table/tr[3]/td/table/tr/td/table/tr/td/table[7]')
        print('Inmate id: {}, Booking id: {}'.format(inmate_id, booking_number))
    except IndexError:
        continue

    for row in rows[0].getchildren()[2:]:
        mapping = {
                'inmate_id': inmate_id,
                'booking_number': booking_number,
                'case_number': row[0].text_content(),
                'offense_date': row[1].text_content(),
                'code': row[2].text_content(),
                'description': row[3].text_content(),
                'grade': row[4].text_content(),
                'degree': row[5].text_content().strip()
            }
        if mapping['offense_date'] == '':
            del mapping['offense_date']
        print(mapping)
        sql_query = sql.SQL("INSERT INTO inmate_information_charges ({columns}) VALUES ({data})").format(
    #    sql_query = sql.SQL("INSERT INTO charges ({columns}) VALUES ({data}) ON CONFLICT (booking_number, inmate_id) DO UPDATE SET ({columns}) = ROW({ex_data})").format(
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
    cursor_gis.execute("update inmate_information set charges_table_processed = True where booking_number = %s", (booking_number,))
    conn_gis.commit()
conn_gis.close()
