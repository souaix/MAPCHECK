from flask import Flask, jsonify, request
import json

import sys
import os
from ftplib import FTP
import datetime
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


sys.path.append('/home/cim')
# connector
import connect.connect as cc

eng_mes = cc.connect('MES', 'MES_Production')
eng_cim = cc.connect('CIM_ubuntu', 'MAPCHECK')


app = Flask(__name__)
@app.route('/map_check/', methods=['POST'],strict_slashes=False)

def mapcheck():
    mo = request.get_json()[0]

    sql ='''
        select BASELOTNO,BARCODE,FILE_TYPE,AVIMAP_FILENAME from
        (SELECT PRODUCTNO,BASELOTNO FROM TBLWIPLOTBASIS WHERE MONO =\''''+mo+'''\') as WIPBS
        INNER JOIN
        (select * from TBLPRDOP where opno ='S093_RW') as PRDOP
        ON WIPBS.PRODUCTNO=PRDOP.PRODUCTNO
        INNER JOIN
        (select * from TBLPRDOPATTRIB where ATTRIBNO ='EXEFORXML' and ATTRIBVALUE='Y') as ATTR
        on PRDOP.serialno = ATTR.SERIALNO
        INNER JOIN
        (select * from TBLPRDOPATTRIB where attribno = 'EQP_AUTOPROGRAM_CO' AND ATTRIBVALUE ='PROG00012') AS ATTCO
        ON PRDOP.SERIALNO=ATTCO.SERIALNO    
        LEFT JOIN
        (select * from OPENQUERY(ERP,'select * from MES.VIEW_AVIMAP_FILE WHERE MO_NO =\'\''''+mo+'''''\') ) AS avimap
        ON WIPBS.BASELOTNO COLLATE Chinese_Taiwan_Stroke_CI_AS = avimap.LOTNO
        '''

    df_PROG00012 = pd.read_sql(sql,engine_mes)
    df_PROG00012.to_sql()
    df_PROG00012.to_sql('PROG0012', con=con_cim, if_exists='append', index=False)
#     result = [{'Message':'Not found data','Qty':'None'}]
#     return jsonify(result)

if __name__ == '__main__':

    from gevent import pywsgi

    server = pywsgi.WSGIServer(('10.21.98.21',5000),app)
    server.serve_forever()
    