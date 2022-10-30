import airflow
from airflow.models.baseoperator import BaseOperator
import requests
import psycopg2


class ResponseOperator(BaseOperator):
    def __init__(self,
                table: str, 
                sort_field: str, 
                sort_direction:str,
                limit: int,
                offset: int,
                **kwargs) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.limit = limit
        self.offset = offset
        self.response = []

    def execute(self):
        response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{self.table}?sort_field={self.sort_field}" \
            "&sort_direction={self.sort_direction}&limit={self.limit}&offset={self.offset}", 
            headers={
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": "machin-ivan",
            "X-Cohort": "5"
            }
        ).json()
        self.response = response


class LoadOperator(BaseOperator):
    def __init__(self,
                lines: list,
                columns: str,
                table_name: str,
                **kwargs) -> None:
        super().__init__(**kwargs)
        self.lines = lines
        self.columns = columns
        self.table_name = table_name

    def execute(self):
        conn = psycopg2.connect(host="localhost",
                                database="de",
                                port=15432,
                                user="jovyan",
                                password="jovyan")
        cursor = conn.cursor()
        conn.autocommit = True

        sql = ''

        if self.table_name == 'stg.deliveries':
            for line in self.lines:
                tmp = f'''INSERT INTO {self.table_name} {self.columns}
                            VALUES('{line["order_id"]}', '{line["order_ts"]}', '{line["delivery_id"]}', '{line["courier_id"]}',
                            '{line["address"]}', '{line["delivery_ts"]}', '{line["rate"]}', '{line["sum"]}', '{line["tip_sum"]}');
                            '''
                sql += tmp
        else:
            for line in self.lines:
                tmp = f'''INSERT INTO {self.table_name} {self.columns}
                            VALUES('{line["_id"]}', '{line["name"]}');
                            '''
                sql += tmp

        cursor.execute(sql)

        cursor.close()
        conn.close()


class MigrateOperator(BaseOperator):
    def __init__(self,
                sql: str,
                **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self):
        conn = psycopg2.connect(host="localhost",
                                database="de",
                                port=15432,
                                user="jovyan",
                                password="jovyan")
        cursor = conn.cursor()
        conn.autocommit = True

        cursor.execute(self.sql)

        cursor.close()
        conn.close()