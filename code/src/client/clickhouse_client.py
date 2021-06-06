from clickhouse_driver import Client

from config import DBConfig


class CHClient:
    def __init__(self, host=DBConfig.db_host, user=DBConfig.db_user, pwd=DBConfig.db_passwd, port=DBConfig.db_tcp_port,
                 db=DBConfig.db_in_use):
        self.client = Client(host=host, user=user, database=db, password=pwd, port=port)

    def get_client(self):
        return self.client
