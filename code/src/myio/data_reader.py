import logging as log
import os
import warnings
import config
import joblib

from client.clickhouse_client import CH, CHClient

log.getLogger().setLevel(log.INFO)

warnings.filterwarnings('ignore')

conn = CH(host=config.db_host, http_port=config.db_http_port, passwd=config.db_passwd).get_conn()
tcp_client = CHClient().get_client()


class DBReader:
    @classmethod
    def tcp_model_cached_read(self, cached_file_path: str, sql: str, cached=True):
        if os.path.exists(cached_file_path):
            # df = pd.read_csv(cached_file_path, sep=',')
            df = joblib.load(cached_file_path)
            log.info('loaded raw dataset from local cache')
        else:
            df = tcp_client.query_dataframe(sql)
            log.info('loaded raw dataset from database')
            if cached:
                joblib.dump(df, cached_file_path, compress=True)
                log.info('cached raw dataset into local directory')
                # df.to_csv(cached_file_path, index=False, header=True)
        return df

    @classmethod
    def insert_dataframe(self, df, sql: str):
        tcp_client.insert_dataframe(query=sql, dataframe=df)
        log.info('loaded raw dataset from database')


if __name__ == '__main__':
    df = DBReader.tcp_model_cached_read("dascvdvd",
                                        "select * from and_ds.materialized_whole_mag_representativeness_distribution where check_item=='lastname_first_initial_popularity';",
                                        cached=False)
    print(df.shape)
    print(df.head())
