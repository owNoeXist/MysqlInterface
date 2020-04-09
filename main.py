from MysqlInterface import Get_Table_Column,Upload_Raw_Data,Get_Raw_Data,Update_Raw_Data

username="nocan"
password="yw2321597"
database="TEST"
checkID=3
entityTable="Driver"
checkTable="DriverCheck"
keyColumn="DriverID"
data=[{"ModuleID":12,
        "DriverName":"test1",
        "DriverDir":"/sys/bus/usb/test1",
        "Bus":"usb",
        "OtherInfo":"this is test1"},
      {"ModuleID":13,
        "DriverName":"test2",
        "DriverDir":"/sys/bus/pci/test2",
        "Bus":"pci",
        "OtherInfo":"this is test2"}]


def printtable(table,column):
    print("\n{0:=^60}".format(table))
    print("{0:20}{1:20}{2:30}".format("Column Name","Column Type","Extra"))
    for oneColumn in column:
        print("{:20}{:20}{:20}".format(oneColumn[0],oneColumn[1],oneColumn[2]))
    print("{:=^60}".format(""))

def ShowTable():
    column=Get_Table_Column(username,password,database,entityTable,1)
    printtable(entityTable,column)
    column=Get_Table_Column(username,password,database,checkTable,1)
    printtable(checkTable,column)

def Upload():
    #entitytable
    autoIncrementID=Upload_Raw_Data(username,password,database,checkID,entityTable,data,1)
    print("autoIncrementID:{0}".format(autoIncrementID))
    #checktable
    checkdata=[]
    for id in autoIncrementID:
        checkdata.append({"CheckID":checkID,"DriverID":id})
    autoIncrementID=Upload_Raw_Data(username,password,database,checkID,checkTable,checkdata)

def Get():
    column=Get_Table_Column(username,password,database,entityTable)
    entityRawData=Get_Raw_Data(username,password,database,checkID,entityTable,column,checkTable,keyColumn)
    for oneData in entityRawData:
        print(oneData)
    column=Get_Table_Column(username,password,database,checkTable)
    checkRawData=Get_Raw_Data(username,password,database,checkID,checkTable,column)
    for oneData in checkRawData:
        print(oneData)
    return [entityRawData,checkRawData]

def Update():
    Update_Raw_Data(username,password,database,checkID,checkTable,checkRawData,["CheckID","DriverID"])
    Get()


if __name__=="__main__":
    #show column of table
    ShowTable()

    #upload raw data to database
    Upload()

    #Get data from database
    rawData=Get()
    checkRawData=rawData[1]

    #analyze data
    checkRawData[0]["Redundance"]=0
    checkRawData[1]["Redundance"]=1

    #update data in database
    Update()
