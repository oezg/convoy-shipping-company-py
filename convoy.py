import json
import sqlite3
import pandas as pd
from xml.etree import ElementTree


def main(file_name=None):
    if file_name is None:
        file_name = input("Input file name\n")
    if file_name.endswith('.xlsx'):
        main(excel_to_csv(file_name))
    elif file_name.endswith('[CHECKED].csv'):
        main(checked_csv_to_db(file_name))
    elif file_name.endswith('.csv'):
        main(csv_to_checked_csv(file_name))
    elif file_name.endswith('.s3db'):
        main(db_to_xml_json(file_name))


def excel_to_csv(file_name: str) -> str:
    csv_name = file_name.split('.')[0] + '.csv'
    dataframe = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)
    dataframe.to_csv(csv_name, index=False, header=True)
    display_result(dataframe.shape[0], csv_name, 'line', 'imported to')
    return csv_name


def csv_to_checked_csv(csv_name: str) -> str:
    count = 0

    def leave_only_digits(cell_content: str) -> str:
        nonlocal count
        if isinstance(cell_content, int) or cell_content.isdigit():
            return str(cell_content)
        count += 1
        return ''.join(c for c in cell_content if c.isdigit())

    checked_name = csv_name[:-4] + '[CHECKED]' + csv_name[-4:]
    pd.read_csv(csv_name).applymap(leave_only_digits).to_csv(checked_name, index=False, header=True)
    display_result(count, checked_name, 'cell', 'corrected in')
    return checked_name


def checked_csv_to_db(checked_name: str) -> str:
    db_name = checked_name[:-len("[CHECKED].csv")] + '.s3db'
    dataframe = pd.read_csv(checked_name)
    headers = dataframe.columns.tolist() + ['score']
    columns = [f"{header} INTEGER {'PRIMARY KEY' if header == 'vehicle_id' else 'NOT NULL'}" for header in headers]
    rows = [score(vehicle) for vehicle in dataframe.itertuples(index=False, name='Vehicle')]
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS convoy ({', '.join(columns)});")
        cur.executemany(f"INSERT INTO convoy VALUES ({', '.join('?' for _ in headers)});", rows)
        conn.commit()
    display_result(dataframe.shape[0], db_name, 'record', 'inserted into')
    return db_name


def db_to_xml_json(db_name: str) -> str:
    json_name, xml_name = map(lambda ext: db_name[:-len('.s3db')] + ext, ('.json', '.xml'))
    with sqlite3.connect(db_name) as conn:
        dataframe = pd.read_sql('SELECT * FROM convoy;', conn)
    json_data = dataframe.query('score > 3').drop(columns=['score'])
    xml_data = dataframe.query('score <= 3').drop(columns=['score'])
    with open(xml_name, 'wb') as xml_file:
        xml_str = xml_data.to_xml(index=False, root_name='convoy', row_name='vehicle', xml_declaration=False)
        xml_file.write(ElementTree.tostring(ElementTree.fromstring(xml_str), short_empty_elements=False))
    with open(json_name, 'w') as json_file:
        json.dump({'convoy': json_data.to_dict(orient='records')}, json_file, indent=4)
    display_result(json_data.shape[0], json_name, 'vehicle', 'saved into')
    display_result(xml_data.shape[0], xml_name, 'vehicle', 'saved into')
    return json_name


def score(vehicle) -> tuple:
    points = 1
    if vehicle.maximum_load >= 20:
        points += 2
    if vehicle.fuel_consumption * 4.50 <= 230:
        points += 1
    if vehicle.fuel_consumption * 4.50 <= 2 * vehicle.engine_capacity:
        points += 1
    if vehicle.fuel_consumption * 4.50 <= vehicle.engine_capacity:
        points += 1
    return tuple([*vehicle, points])


def display_result(count: int, file_name: str, noun: str, verb: str) -> None:
    print(f"{count} {noun}{'' if count == 1 else 's'} w{'as' if count == 1 else 'ere'} {verb} {file_name}")


if __name__ == '__main__':
    main()
