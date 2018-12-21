#!/bin/bash

SourceDbName='gunbuster'
ArchiveDbName='eznearline_gunbuster'
SourceUser='chenqi'
SourcePwd='chenqi323'
SourceHost='192.168.199.112'
SourcePort='3306'
ArchiveUser='chenqi'
ArchivePwd='chenqi323'
ArchiveHost='192.168.199.112'
ArchivePort='3306'
MysqlDiff=`which mysqldiff`

${MysqlDiff} --server1=${SourceUser}:${SourcePwd}@${SourceHost}:${SourcePort} --server2=${ArchiveUser}:${ArchivePwd}@${ArchiveHost}:${ArchivePort} --difftype=sql --changes-for=server2 --skip-table-options -q --force ${SourceDbName}:${ArchiveDbName} |grep -v 'PASS'
