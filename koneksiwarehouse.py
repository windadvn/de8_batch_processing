#!/usr/bin/python3

import os
import json
import psycopg2

from sqlalchemy import create_engine
#if __name__ == '__main__':
def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf

def conn():
    conf = config('warehouse')
    try:
        conn = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd'], 
                                port=conf['port']
                                )
        print(f"[INFO] Berhasil Terkoneksi ke Warehouse .....")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}:{conf['port']}/{conf['db']}")
        return conn, engine
    except:
        print(f"[INFO] Gagal Koneksi ke Warehouse .....")
