#!/usr/bin/env python
# coding: utf-8
from flask import Flask, jsonify, request

from gevent import monkey
monkey.patch_all()

from gevent.pywsgi import WSGIServer
from multiprocessing import cpu_count, Process
from bottle import Bottle

import json
import sys
import os
from ftplib import FTP
import datetime
import pandas as pd
#app = Bottle()

sys.path.append('/home/cim')
# connector
import connect.connect as cc

import json
import time

import asyncio

# sys.path.append('C:\\Users\\User\\Desktop\\python')
import connect.connect as cc

eng_cim_iot = cc.connect('CIM_ubuntu', 'iot')
eng_mes = cc.connect('MES', 'MES_Production')
eng_cim = cc.connect('CIM_ubuntu', 'mapcheck')




app = Flask(__name__)

@app.route('/mapcheck_prd/', methods=['POST'],strict_slashes=False)

def mapcheck_prd():

    # mo = request.get_json()[0]['mo']
    lotno = request.get_json()[0]['lotno']
    # lotno_ = lotno.replace('-','')

    opnos=['S093_RW','S081','S081','S074_DS','S074_GD','S074_RW','S110_RW']
    programid=['PROG00012','PROG00028','PROG00015','PROG00011','PROG00011','PROG00011','PROG00020']
    BR=['EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO']
    FILETYPE=['2ND_AVI','5S_MAP','TRACELOG','1ST_AVI','1ST_AVI','1ST_AVI','CUTRACELOG']

    opno_str=''
    for i in opnos:
        opno_str = opno_str + "'"+str(i)+"',"
    opno_str = opno_str[:-1]


    #check EXEFORXML Y
    sql ='''
        select ATTR.SERIALNO,PRDOP.OPNO from
        (SELECT PRODUCTNO,BASELOTNO FROM TBLWIPLOTBASIS WHERE BASELOTNO =\''''+lotno+'''\') as WIPBS
        INNER JOIN
        (select * from TBLPRDOP where opno in ('''+opno_str+''') )as PRDOP
        ON WIPBS.PRODUCTNO=PRDOP.PRODUCTNO
        INNER JOIN
        (select * from TBLPRDOPATTRIB where ATTRIBNO ='EXEFORXML' and ATTRIBVALUE='Y') as ATTR
        on PRDOP.serialno = ATTR.SERIALNO        
        '''
    
    df_check_xml = pd.read_sql(sql,eng_mes)

    for i,v in enumerate(opnos):
        df_check_xml_opno = df_check_xml[df_check_xml['OPNO']==v]
        df_check_xml_opno.reset_index(drop=True,inplace=True)
        #EXEFORXML=Y => check PROG00012
        if(len(df_check_xml_opno)>0):        

            serialno = df_check_xml_opno['SERIALNO'][0]

            sql ="select * from TBLPRDOPATTRIB where attribno = '"+BR[i]+"' AND ATTRIBVALUE like '"+programid[i]+"%' AND SERIALNO ='"+serialno+"'"                    
            df_PROGRAM = pd.read_sql(sql,eng_mes)     
            #PROGRAM existed => check AVIMAP
            if(len(df_PROGRAM)>0):
                if(FILETYPE[i] in ['2ND_AVI','1ST_AVI']):
                    sql="select * from OPENQUERY(RIS,'select * from MES.VIEW_AVIMAP_FILE WHERE LOTNO=''"+lotno+"'' AND FILE_TYPE=''"+FILETYPE[i]+"''')"

                elif(FILETYPE[i] in ['5S_MAP']):
                    sql="select * from OPENQUERY(RIS,'select * from MES.VIEW_5SMAP_FILE WHERE LOTNO=''"+lotno+"''')"

                elif(FILETYPE[i] in ['TRACELOG']):
                    sql="select * from OPENQUERY(RIS,'select * from MES.VIEW_TRACELOG_FILE WHERE LOTNO=''"+lotno+"''')"

                elif(FILETYPE[i] in ['CUTRACELOG']):
                    sql="select * from OPENQUERY(RIS,'select * from MES.VIEW_TRACELOG_FILE WHERE CU_LOG_FILENAME IS NOT NULL AND LOTNO=''"+lotno+"''')"

                df_MAPCHECK = pd.read_sql(sql,eng_mes)         
                #AVIMAP => return ok
                if(len(df_MAPCHECK)>0):
                    result = [{'Result':'PASS','Message':programid[i]+' PASS'}]    
                                
                #else => return MAPCHECK fail
                else:
                    result = [{'Result':'FAIL','Message':lotno+' in '+v+': cannot find '+FILETYPE[i]+', please check'}]
                    break
                    #PROGRAM is not existed
            else:
                result = [{'Result':'PASS','Message':'its have no '+programid[i]+' rule'}]

        #EXEFORXML<>Y => don't need check        
        else:
            result = [{'Result':'PASS','Message':v+',its have no EXEFORXML rule'}]
            # break

    return jsonify(result)


server_mc = WSGIServer(('10.21.98.21',7777),app,log=None)
server_mc.start()

########
@app.route('/mapcheck_test/', methods=['POST'],strict_slashes=False)

def mapcheck_test():

    # mo = request.get_json()[0]['mo']
    lotno = request.get_json()[0]['lotno']
    # lotno_ = lotno.replace('-','')

    opnos=['S093_RW','S081','S081','S074_DS','S074_GD','S074_RW','S110_RW']
    programid=['PROG00012','PROG00028','PROG00015','PROG00011','PROG00011','PROG00011','PROG00020']
    BR=['EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO','EQP_AUTOPROGRAM_CO']
    FILETYPE=['2ND_AVI','5S_MAP','TRACELOG','1ST_AVI','1ST_AVI','1ST_AVI','CUTRACELOG']

    opno_str=''
    for i in opnos:
        opno_str = opno_str + "'"+str(i)+"',"
    opno_str = opno_str[:-1]


    #check EXEFORXML Y
    sql ='''
        select ATTR.SERIALNO,PRDOP.OPNO from
        (SELECT PRODUCTNO,BASELOTNO FROM TBLWIPLOTBASIS WHERE BASELOTNO =\''''+lotno+'''\') as WIPBS
        INNER JOIN
        (select * from TBLPRDOP where opno in ('''+opno_str+''') )as PRDOP
        ON WIPBS.PRODUCTNO=PRDOP.PRODUCTNO
        INNER JOIN
        (select * from TBLPRDOPATTRIB where ATTRIBNO ='EXEFORXML' and ATTRIBVALUE='Y') as ATTR
        on PRDOP.serialno = ATTR.SERIALNO        
        '''
    
    df_check_xml = pd.read_sql(sql,eng_mes)

    for i,v in enumerate(opnos):
        df_check_xml_opno = df_check_xml[df_check_xml['OPNO']==v]
        df_check_xml_opno.reset_index(drop=True,inplace=True)
        #EXEFORXML=Y => check PROG00012
        if(len(df_check_xml_opno)>0):        

            serialno = df_check_xml_opno['SERIALNO'][0]

            sql ="select * from TBLPRDOPATTRIB where attribno = '"+BR[i]+"' AND ATTRIBVALUE like '"+programid[i]+"%' AND SERIALNO ='"+serialno+"'"                    
            df_PROGRAM = pd.read_sql(sql,eng_mes)     
            #PROGRAM existed => check AVIMAP
            if(len(df_PROGRAM)>0):
                if(FILETYPE[i] in ['2ND_AVI','1ST_AVI']):
                    sql="select * from OPENQUERY(RIS_TEST,'select * from MES.VIEW_AVIMAP_FILE WHERE LOTNO=''"+lotno+"'' AND FILE_TYPE=''"+FILETYPE[i]+"''')"

                elif(FILETYPE[i] in ['5S_MAP']):
                    sql="select * from OPENQUERY(RIS_TEST,'select * from MES.VIEW_5SMAP_FILE WHERE LOTNO=''"+lotno+"''')"

                elif(FILETYPE[i] in ['TRACELOG']):
                    sql="select * from OPENQUERY(RIS_TEST,'select * from MES.VIEW_TRACELOG_FILE WHERE LOTNO=''"+lotno+"''')"

                elif(FILETYPE[i] in ['CUTRACELOG']):
                    sql="select * from OPENQUERY(RIS_TEST,'select * from MES.VIEW_TRACELOG_FILE WHERE CU_LOG_FILENAME IS NOT NULL AND LOTNO=''"+lotno+"''')"

                df_MAPCHECK = pd.read_sql(sql,eng_mes)         
                #AVIMAP => return ok
                if(len(df_MAPCHECK)>0):
                    result = [{'Result':'PASS','Message':programid[i]+' PASS'}]    
                                
                #else => return MAPCHECK fail
                else:
                    result = [{'Result':'FAIL','Message':lotno+' in '+v+': cannot find '+FILETYPE[i]+', please check'}]
                    break
                    #PROGRAM is not existed
            else:
                result = [{'Result':'PASS','Message':'its have no '+programid[i]+' rule'}]

        #EXEFORXML<>Y => don't need check        
        else:
            result = [{'Result':'PASS','Message':v+',its have no EXEFORXML rule'}]
            # break

    return jsonify(result)

server_mct = WSGIServer(('10.21.98.21',7770),app,log=None)
server_mct.start()

def server_forever():
    server_mc.start_accepting()
    server_mct.start_accepting()

    server_mc._stop_event.wait()
    server_mct._stop_event.wait()




if __name__ == '__main__':

	print('----')
	for i in range(cpu_count()):
		p = Process(target=server_forever)
		p.start()

	
