import pymysql
import json

def Upload_Raw_Data(Username,Password,Database,CheckID,TableName,Data,AutoFlag=0):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get column info
    columnName = ', '.join('{}'.format(k) for k in Data[0].keys())
    columnValue = ', '.join('%({})s'.format(k) for k in Data[0].keys())
    #upload data to entitytable
    sql="insert into {0}({1}) values({2})"\
        .format(TableName,columnName,columnValue)
    print("\nmysql>"+sql)
    cursor.executemany(sql,Data)
    conn.commit()
    #update checktable
    autoIncrementID=[]
    if(AutoFlag!=0):
        sql = "select LAST_INSERT_ID()"
        cursor.execute(sql)
        beginID=cursor.fetchone()[0]
        endID=beginID+len(Data)
        autoIncrementID=list(range(beginID,endID))
    #close connect
    cursor.close()
    conn.close()
    return autoIncrementID

def Get_Raw_Data(Username,Password,Database,CheckID,TableName,ColumnNeed,CheckTable=None,KeyColunm=None):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get column info
    columnNeed = ', '.join('{}'.format(k) for k in ColumnNeed)
    #obtain data from table
    if CheckTable == None:
        sql="select {0} from {1} where CheckID = {2}"\
            .format(columnNeed,TableName,CheckID)
    else:
        sql="select {0} from {1} where {2} in (select {2} from {3} where CheckID = {4})"\
            .format(columnNeed,TableName,KeyColunm,CheckTable,CheckID)
    print("\nmysql>"+sql)
    cursor.execute(sql)
    result=cursor.fetchall()
    data=[]
    for value in result:
        value=list(value)
        newDict = dict(zip(ColumnNeed, value))
        data.append(newDict)
    #close connect
    cursor.close()
    conn.close()
    return data

def Update_Raw_Data(Username,Password,Database,CheckID,TableName,Data,ColunmKey):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get column that need update
    columnUpdate= ', '.join('{0}=%({0})s'.format(k) for k in Data[0].keys())
    columnKey = ' and '.join('{0}=%({0})s'.format(k) for k in ColunmKey)
    #update data of table
    sql="update {0} set {1} where {2}"\
        .format(TableName,columnUpdate,columnKey)
    print("\nmysql>"+sql)
    cursor.executemany(sql,Data)
    conn.commit()
    #close connect
    cursor.close()
    conn.close()

def Get_Table_Column(Username,Password,Database,TableName,Flag=0):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #obtain column info
    sql="select COLUMN_NAME,DATA_TYPE,EXTRA\
        from information_schema.columns \
        where table_name = %s;"
    ret=cursor.execute(sql,[TableName])
    #generate string for sql
    columnName= []
    if Flag==0:
        for _ in range(ret):
            curColumn=cursor.fetchone()
            columnName.append(curColumn[0])
    elif Flag==1:
        for _ in range(ret):
            curColumn=cursor.fetchone()
            columnName.append(curColumn)
    #close connect
    cursor.close()
    conn.close()
    return columnName