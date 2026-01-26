#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target-table', default='green_taxi_trips', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for inserting')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Ingest NYC Green Taxi data (parquet) into PostgreSQL database."""
    
    # 官方 NYC TLC 数据源（parquet 格式）
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    
    print(f'Downloading from {url}...')
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # 读取 parquet 文件
    df = pd.read_parquet(url)
    print(f'Total rows: {len(df)}')

    # 创建表结构
    df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
    print('Table created')

    # 分批插入
    for i in tqdm(range(0, len(df), chunksize)):
        df_chunk = df[i:i+chunksize]
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print('Done!')

if __name__ == '__main__':
    run()