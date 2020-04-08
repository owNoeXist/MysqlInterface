from MysqlInterface import Upload_Raw_Data,Get_Raw_Data,Update_Raw_Data

if __name__=="__main__":
    username="nocan"
    password="xxxx"
    database="TEST"
    checkID=3
    #---------------------------------------------------------------------------------------
    data=[(22,"TEST","/sys/test","usb","None"),(33,"TEST1","/sys/test1","pci","None")]
    Upload_Raw_Data(username,password,database,checkID,"Driver",data,"DriverID","DriverCheck")
    #---------------------------------------------------------------------------------------
    result=Get_Raw_Data(username,password,database,checkID,"Driver","DriverCheck","DriverID")
    print("Driver:")
    for tuples in result[0]:
        print(tuples)
    print("DriverCheck:")
    for tuples in result[1]:
        print(tuples)
    #---------------------------------------------------------------------------------------
    Update_Raw_Data(username,password,database,checkID,"Driver",result,"DriverID","DriverCheck")
    result=Get_Raw_Data(username,password,database,checkID,"Driver","DriverCheck","DriverID")
    print("Driver:")
    for tuples in result[0]:
        print(tuples)
    print("DriverCheck:")
    for tuples in result[1]:
        print(tuples)