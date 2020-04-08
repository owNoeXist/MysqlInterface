import pymysql
import json

def Upload_Raw_Data(Username,Password,Database,CheckID,TableName,Data,KeyColunm=None,CheckTable=None):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get column info
    columnInfo=Get_Table_Column(cursor,TableName,1)
    #upload data to entitytable
    sql="insert into {0}{1} values{2}"\
        .format(TableName,columnInfo[0],columnInfo[1])
    print("mysql>"+sql)
    cursor.executemany(sql,Data)
    conn.commit()
    #update checktable
    if CheckTable!=None:
        sql = "select LAST_INSERT_ID()"
        cursor.execute(sql)
        newID=cursor.fetchone()[0]
        checkData=[]
        for id in range(newID,newID+len(Data)):
            checkData.append((CheckID,id))
        sql="insert into {0}(CheckID,{1}) values(%s,%s)"\
            .format(CheckTable,KeyColunm)
        print("mysql>"+sql)
        cursor.executemany(sql,checkData)
        conn.commit()
    #close connect
    cursor.close()
    conn.close()

def Get_Raw_Data(Username,Password,Database,CheckID,TableName,CheckTable=None,KeyColunm=None):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get columnname
    #columnName=Get_Table_Column(cursor,TableName,1)
    #checktable
    if CheckTable == None:
        sql="select * from {0} where CheckID = {1}"\
            .format(TableName,CheckID)
        print("mysql>"+sql)
        cursor.execute(sql)
        result=cursor.fetchall()
        return [result]
    #entity table
    else:
        sql="select * from {0} where CheckID = {1}"\
            .format(CheckTable,CheckID)
        print("mysql>"+sql)
        cursor.execute(sql)
        checkresult=cursor.fetchall()
        sql="select * from {0} where {1} in (\
                select {1} from {2} where CheckID = {3})"\
            .format(TableName,KeyColunm,CheckTable,CheckID)
        print("mysql>"+sql)
        cursor.execute(sql)
        result=cursor.fetchall()
        return [result,checkresult]
    #close connect
    cursor.close()
    conn.close()

def Update_Raw_Data(Username,Password,Database,CheckID,TableName,Data,KeyColunm,CheckTable=None):
    #connect database
    conn = pymysql.connect(
        host="127.1.1.1",
        user=Username,
        password=Password,
        database=Database,
        charset="utf8")
    cursor = conn.cursor()
    #get column that need update
    columnUpdate=Get_Table_Column(cursor,TableName,2)
    #update data of table
    sql="update {0} set {1} where {2}=%s"\
        .format(TableName,columnUpdate[0],KeyColunm)
    print("mysql>"+sql)
    newData=[]
    for data in Data[0]:
        newData.append((data[1:]+data[0:1]))
    cursor.executemany(sql,newData)
    conn.commit()
    #updata checktable
    if CheckTable!=None:
        columnUpdate=Get_Table_Column(cursor,CheckTable,2)
        sql="update {0} set Redundance=%s where CheckID=%s and {1}=%s"\
            .format(CheckTable,KeyColunm)
        print("mysql>"+sql)
        newData=[]
        for data in Data[1]:
            newData.append(((1,)+data[0:2]))
        cursor.executemany(sql,newData)
        conn.commit()
    #close connect
    cursor.close()
    conn.close()

def Get_Table_Column(Cursor,TableName,Flag=0):
    #obtain column info
    sql="select COLUMN_NAME,DATA_TYPE,EXTRA\
        from information_schema.columns \
        where table_name = %s;"
    ret=Cursor.execute(sql,[TableName])
    #generate string for sql
    if Flag==0:
        columnName= "("
        for _ in range(ret):
            curColumn=Cursor.fetchone()
            columnName=columnName + curColumn[0] +','
        columnName=columnName[:-1]+")"
        return [columnName]
    elif Flag==1:
        columnName= "("
        columnType= "("
        for _ in range(ret):
            curColumn=Cursor.fetchone()
            if("auto_increment" in curColumn[2]):
                continue
            columnName=columnName + curColumn[0] +','
            columnType=columnType+'%s,'
        columnName=columnName[:-1]+")"
        columnType=columnType[:-1]+")"
        return [columnName,columnType]
    elif Flag==2:
        columnNew=""
        for _ in range(ret):
            curColumn=Cursor.fetchone()
            if "auto_increment" in curColumn[2] or "foreign key" in curColumn[2]:
                continue
            columnNew=columnNew+curColumn[0]+"=%s,"
        columnNew=columnNew[:-1]
        return [columnNew]