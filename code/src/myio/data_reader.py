import logging as log
import os
import warnings

import joblib

from client.clickhouse_client import CHClient

log.getLogger().setLevel(log.INFO)

warnings.filterwarnings('ignore')

tcp_client = CHClient().get_client()


class DBReader:
    @classmethod
    def tcp_model_cached_read(cls, cached_file_path: str, sql: str, cached=True):
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
