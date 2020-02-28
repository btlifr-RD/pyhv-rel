import datetime
import impala.dbapi as ipdb
import re
import pymysql
import time

class Analysis(object):

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False
    def is_time(self,s):
        try:
            datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
            return True
        except ValueError:
            return False

if __name__ =='__main__':
    an = Analysis()
    bigdatavin=[]
    errlog=[]
    #alllog=[]
    rowtup=()
    file=open("interface-np.log", "r")  #打开配置文件
    hive_line = file.readline()
    hive_line=hive_line.strip('\n')
    hive_con=re.split(',',hive_line)
    mysql_line=file.readline()
    mysql_line=mysql_line.strip('\n')
    mysql_con=re.split(',',mysql_line)
    file.close()
    filevin=open("vin-np.log", "r")
    vin_line=filevin.readlines()
    for i in range(len(vin_line)):
        vin_line[i]=vin_line[i].strip('\n')

    t1=time.time()
    for carvin in vin_line:
        conn = ipdb.connect(host=str(hive_con[0]),port=int(hive_con[1]), user=str(hive_con[2]),password=str(hive_con[3]),database=str(hive_con[4]),auth_mechanism='PLAIN')
        cursor = conn.cursor()
        hvsql="select count(*) from "+hive_con[5]+" where vin = '"+carvin+"'"
        cursor.execute(hvsql)
        t2=time.time()
        datacount=cursor.fetchall()
        tlife=t2-t1
        print(tlife,datacount,carvin)
        rowtup=(tlife,datacount,carvin)
        filelog = open('alllog.log', 'a+')
        filelog.write('\n')
        filelog.write(str(rowtup))
        filelog.flush()
        filelog.close()
        #alllog.append(rowtup)
        datacou=datacount[0]
        if int(datacou[0])>40000000:
            bigdatavin=(carvin,)
            filelog = open('biglog.log', 'a+')
            filelog.write('\n')
            filelog.write(str(bigdatavin))
            filelog.flush()
            filelog.close()
            continue
        t1=time.time()
        hvsql="select dateTime,carSpeed,totalCurrent,speedUpRecord,brakingState,record,maxTempValue,miniTempValue,\
                       gears,SOC,insulationResistance,VIN,totalVolt,maxTempProbeNo,miniTempProbeNo,maxVoltage,\
                       maxPressMonomerNo,miniVolmtageValue,miniPressMonomerNo,chargeTempStr,chargeDevVoltStr,\
                       alrmCellUniformityBad,alrmBreakSys,alrmEnergyUnderVolt,alrmOverCharge,alrmEnergyOverVolt,\
                       alrmSocChange,alrmDCDCStatus,alrmDCDCTemp,alrmDriveControlTemp,alrmDriverMachine,\
                       alrmSOCHigh,alrmElecHighTemp,alrmHVoltLockStatus,alrmInsulation,alrmLowSOC,alrmCellUnderVolt,\
                       alrmCellOverVolt,alrmTemp from (select *,row_number() over (partition by dateTime order by dateTime\
                       ) as dt from "+hive_con[5]+" where vin = '"+carvin+"' ) t where t.dt=1 "

        cursor.execute(hvsql)
        newdate=[]
        ins=0
        tstar=datetime.datetime(2010,1,1,00,00,00,00)
        tend=datetime.datetime(2030,1,1,00,00,00,00)

        for rowData in cursor.fetchall():
             rowList=list(rowData)

             nlist = list(set(range(len(rowList))).difference(set([0,11,19,20])))
             isnum = 1
             for j in nlist:
                 if an.is_number(rowList[j])==False:
                     isnum = 0
                     break #continue
             if isnum ==1:
                 if an.is_time(rowList[0]) and (tstar <= datetime.datetime.strptime(rowList[0], '%Y-%m-%d %H:%M:%S')<= tend):
                    if (0 <= float(rowList[1])<= 300) and (-50<= float(rowList[6])<= 150) and (-50 <= float(rowList[7]) <= 150) and (0 <= float(rowList[9]) <= 100) and (0 <= float(rowList[12]) <= 1000) and (0 <= float(rowList[15]) <= 10) and (-1 <= float(rowList[17]) <= 10):
                        newRow=tuple(rowList)
                        newdate.append(newRow)
                        ins += 1

        t2=time.time()
        tlife=t2-t1
        print(tlife,ins)
        if ins>0:
            rowtup=(tlife,ins,carvin)
            filelog = open('alllog.log', 'a+')
            filelog.write('\n')
            filelog.write(str(rowtup))
            filelog.flush()
            filelog.close()
        else:
            errlog=(tlife,ins,carvin)
            filelog = open('errlog.log', 'a+')
            filelog.write('\n')
            filelog.write(str(errlog))
            filelog.flush()
            filelog.close()
            continue


        conn.close()

        mysqlconn=pymysql.connect(host=str(mysql_con[0]), port=int(mysql_con[1]),user=str(mysql_con[2]), password=str(mysql_con[3]),db=str(mysql_con[4]),charset='utf8')
        mqcursor=mysqlconn.cursor()
        sqldel="DROP IF EXISTS TABLE "+carvin
        mqcursor.execute(sqldel)
        mysqlconn.commit()
        sqlcre="CREATE TABLE "+carvin+" (timecolm datetime NOT NULL, \
            speed FLOAT NULL,\
            electricity FLOAT NULL,\
            acceletatorMileage FLOAT NULL,\
            pedalState FLOAT NULL,\
            roadLength FLOAT NULL,\
            hieghtTemperature FLOAT NULL,\
            lowTemperature FLOAT NULL,\
            gearState FLOAT NULL,\
            soc FLOAT NULL,\
            insulation FLOAT NULL,\
            vinNumber CHAR (17) NULL,\
            voltage FLOAT NULL,\
            monHighTemperatureIndex INT NULL,\
            monLowTemperatureIndex INT NULL,\
            monHighVoltage FLOAT NULL,\
            monHighVoltageIndex INT NULL,\
            monLowVoltage FLOAT NULL,\
            monLowVoltageIndex INT NULL,\
            monTemperatureStr text NULL,\
            monVoltageStr text NULL,\
            consistencyFlag FLOAT NULL,\
            brakeSystemFlage FLOAT NULL,\
            lowVoltageFlag FLOAT NULL,\
            overChargeFlag FLOAT NULL,\
            overVoltageFlag FLOAT NULL,\
            changeSocFlag FLOAT NULL,\
            dCDCStateFlage FLOAT NULL,\
            dCDCtempFlage FLOAT NULL,\
            driveMotorConTemperatureFlage FLOAT NULL,\
            driveMotorFlage FLOAT NULL,\
            highSocFlag FLOAT NULL,\
            hieghTemperatureAlarmFlag FLOAT NULL,\
            highVoltageInterlockFlage FLOAT NULL,\
            insulationAlarmFlag FLOAT NULL,\
            lowSocFlag FLOAT NULL,\
            monomerLowVoltageFlag FLOAT NULL,\
            monomerHighVoltageFlag FLOAT NULL,\
            temperatureDiffAlarmFlag FLOAT NULL );"
        mqcursor.execute(sqlcre)
        mysqlconn.commit()
        t1=time.time()
        sql = "insert into "+carvin+" (\
        timecolm,speed,electricity,acceletatorMileage,\
        pedalState,roadLength,hieghtTemperature,\
        lowTemperature,gearState,soc,insulation,vinNumber,\
        voltage,monHighTemperatureIndex,monLowTemperatureIndex,\
        monHighVoltage,monHighVoltageIndex,monLowVoltage,\
        monLowVoltageIndex,monTemperatureStr,monVoltageStr,\
        consistencyFlag,brakeSystemFlage,lowVoltageFlag,\
        overChargeFlag,overVoltageFlag,changeSocFlag,dCDCStateFlage,\
        dCDCtempFlage,driveMotorConTemperatureFlage,driveMotorFlage,\
        highSocFlag,hieghTemperatureAlarmFlag,highVoltageInterlockFlage,\
        insulationAlarmFlag,lowSocFlag,monomerLowVoltageFlag,\
        monomerHighVoltageFlag,temperatureDiffAlarmFlag) values\
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        mqcursor.executemany(sql, newdate)
        mqcursor.close()
        mysqlconn.commit()
        mysqlconn.close()
        t2=time.time()
        tlife=t2-t1
        print(tlife)
        #rowtup=(tlife,)
        filelog = open('alllog.log', 'a+')
        filelog.write('\n')
        filelog.write(str(tlife))
        filelog.flush()
        filelog.close()





