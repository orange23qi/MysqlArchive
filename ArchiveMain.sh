#!/bin/bash

/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/CreateOrderId/MysqlArchive_CreateOrderId.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Userorder/MysqlArchive_Userorder.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Garencieres/MysqlArchive_Garencieres.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Oplogger/MysqlArchive_Oplogger.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Eltreum/MysqlArchive_Eltreum.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Eltreum/Sync_Eltreum_dic_.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Gunbuster/MysqlArchive_Gunbuster.py >/dev/null 2>&1
/usr/bin/python /data/mysql/scripts/MysqlArchiveScripts/Oms/MysqlArchive_Oms.py >/dev/null 2>&1
