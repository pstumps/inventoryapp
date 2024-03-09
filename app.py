from flask import Flask, request, jsonify
import os
import psycopg2
from dotenv import load_dotenv
import datetime

load_dotenv()

CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, product TEXT, quantity INTEGER, lastmodified TIMESTAMP);"
)

DELETE_TABLE = (
    "DROP TABLE %s;"
)

ADD_COLUMN = (
    "ALTER TABLE %s ADD COLUMN %s %s;"
)

DELETE_COLUMN = (
    "ALTER TABLE %s DROP COLUMN %s;"
)

INSERT_DATA = (
    "INSERT INTO %s(%s) VALUES ('%s') RETURNING id;"
)

DELETE_DATA = (
    "DELETE FROM %s WHERE %s = '%s';"
)

ADD_THRESHOLD_PRODUCT = (
    "INSERT INTO low_quantity_threshold (foreign_product_id, tablename, column_name, product) VALUES (%s, %s, %s, %s);"
)

DELETE_THRESHOLD_PRODUCT = (
    "DELETE FROM low_quantity_threshold WHERE tablename = %s AND product = %s;"
)

UPDATE_DATA = (
    "UPDATE %s SET %s = '%s' WHERE id = %s RETURNING id;"
)

UPDATE_QUANTITY = (
    "UPDATE {} SET quantity = %s, lastmodified = %s WHERE product = %s RETURNING id;"
)

FREQUENCY_TABLE_ENTRY = (
    "INSERT INTO updated_frequently (foreign_product_id, tablename, product, updated_date) VALUES (%s, %s, %s, %s);"
)

INSERT_QUANTITY_THRESHOLD = (
    "INSERT INTO low_quantity_threshold (qty_threshold) VALUES (%s) WHERE tablename = %s, column_name = %s, product = %s;"
)

UPDATE_QUANTITY_THRESHOLD_TABLE = (
    "UPDATE low_quantity_threshold SET current_qty = %s WHERE tablename = %s AND column_name = %s AND product = %s;"
)

GET_ALERTS = (
    "SELECT * FROM low_quantity_alerts;"
)

ADD_ALERTS = (
    "INSERT INTO low_quantity_alerts (phone_number, email_address, day_of_the_week, set_time) VALUES %s, %s, %s, %s;"
)

UPDATE_ALERTS = (
    "UPDATE low_quantity_alerts SET phone_number = %s, email_address = %s, day_of_the_week = %s, set_time = %s WHERE id = %s;"
)

dbconn = psycopg2.connect(host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'), port=os.getenv('DB_PORT'))

app = Flask(__name__)

@app.post('/api/createtable')
def create_table():
    data = request.get_json()
    table_name = data['table_name']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(F'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = \'{table_name}\');')
            tableexists = curr.fetchone()[0]
            if tableexists:
                return jsonify({'message': 'Table already exists'}), 200
            else:
                curr.execute(CREATE_TABLE % table_name)
    return jsonify({'message': 'Table created successfully'}), 201

#to do: retrieve all tables

@app.delete('/api/deletetable')
def delete_table():
    data = request.get_json()
    table_name = data['table_name']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(DELETE_TABLE % table_name)
    return jsonify({'message': 'Table deleted successfully'}), 200

@app.post('/api/addcolumn')
def add_column():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    data_type = data['data_type'] # TEXT, NUMERIC, INTEGER, TIMESTAMP
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(ADD_COLUMN % (table_name, column_name, data_type))
    return jsonify({'message': F'Column {column_name} added successfully'}), 201

@app.delete('/api/deletecolumn')
def delete_column():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    if column_name == 'id' or column_name == 'product' or column_name == 'quantity' or column_name == 'lastmodified':
        return jsonify({'message': F'Cannot delete {column_name} column'}), 400
    else:
        with dbconn:
            with dbconn.cursor() as curr:
                curr.execute(DELETE_COLUMN % (table_name, column_name))
        return jsonify({'message': F'Column {column_name} deleted successfully'}), 200

@app.post('/api/insertdata')
def insert_data():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    item_name = data['item_name']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(INSERT_DATA % (table_name, column_name, item_name))
            id = curr.fetchone()[0]
            curr.execute(ADD_THRESHOLD_PRODUCT, (id, table_name, column_name, item_name))
    return jsonify({'id':id,'message': F'Data added successfully'}), 201

@app.delete('/api/deletedata')
def delete_data():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    item_name = data['item_name']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(DELETE_DATA % (table_name, column_name, item_name))
            curr.execute(DELETE_THRESHOLD_PRODUCT, (table_name, item_name))
    return jsonify({'message': F'Data deleted successfully'}), 201

#"UPDATE %s SET %s = '%s' WHERE id = %s RETURNING id;"
@app.put('/api/updatedata')
def update_data():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    item_name = data['item_name']
    id = data['id']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(UPDATE_DATA % (table_name, column_name, item_name, id))
    return jsonify({'id': id,'message': F'Data updated successfully'}), 201

#"UPDATE {} SET quantity = %s, lastmodified = %s WHERE product = %s RETURNING id;"
#"UPDATE low_quantity_threshold SET current_qty = %s WHERE tablename = %s, column_name = %s, product = %s;"
@app.put('/api/updatequantity')
def update_quantity():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    quantity = data.get('quantity', None)
    item_name = data['item_name']
    with dbconn:
        with dbconn.cursor() as curr:
            if isinstance(quantity, int):
                current_timestamp = datetime.datetime.now()
                curr.execute(UPDATE_QUANTITY.format(table_name), (quantity, current_timestamp, item_name))
                updated_row = curr.fetchone()
                if updated_row:
                    foreign_product_id = updated_row[0]
                    current_date = datetime.date.today()
                    curr.execute(FREQUENCY_TABLE_ENTRY, (foreign_product_id, table_name, item_name, current_date))
                    curr.execute(UPDATE_QUANTITY_THRESHOLD_TABLE, (quantity, table_name, column_name, item_name))
                else:
                    return jsonify({'message': F'No rows were updated for product {item_name}'}), 400
            else:
                return jsonify({'message': 'Quantity must be an integer'}), 400
    return jsonify({'message': F'Quantity updated successfully'}), 201

#"INSERT INTO low_quantity_threshold (qty_threshold) VALUES (%s) WHERE tablename = %s, column_name = %s, product = %s;"
@app.post('/api/addqtythreshold')
def add_qty_threshold():
    data = request.get_json()
    table_name = data['table_name']
    column_name = data['column_name']
    item_name = data['item_name']
    qty_threshold = data['qty_threshold']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(INSERT_QUANTITY_THRESHOLD, (qty_threshold, table_name, column_name, item_name))
    return jsonify({'message': F'Quantity threshold added successfully'}), 201

@app.post('/api/addalerts')
def add_alerts():
    data = request.get_json()
    phone_number = data['phone_number']
    email_address = data['email_address']
    day_of_the_week = data['day_of_the_week']
    set_time = data['set_time']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(ADD_ALERTS, (phone_number, email_address, day_of_the_week, set_time))
    return jsonify({'message': F'Alerts added successfully'}), 201

@app.put('/api/updatealerts')
def update_alerts():
    data = request.get_json()
    phone_number = data['phone_number']
    email_address = data['email_address']
    day_of_the_week = data['day_of_the_week']
    set_time = data['set_time']
    id = data['id']
    with dbconn:
        with dbconn.cursor() as curr:
            curr.execute(UPDATE_ALERTS, (phone_number, email_address, day_of_the_week, set_time, id))
    return jsonify({'message': F'Alerts updated successfully'}), 201

#to do: retrieve all data alerts