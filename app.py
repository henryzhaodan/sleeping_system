'''
@更新
@date 2021/01/30
@add function：导出睡眠报告，将睡眠误差结果存入数据库

@author cecilia
@date 2020/01/23
@function 一键化数据存储和误差分析（darma和sleep）
'''

import pymysql
from DBUtils.PooledDB import PooledDB
import pandas as pd
from pylab import *
import matplotlib.pyplot as plt
import numpy as np
import os


def connect_mysql():
    mysql_host = 'rm-bp1puzeu213v3gcc90o.mysql.rds.aliyuncs.com'  # '192.168.187.131'    '192.168.194.20'
    user = 'dotdev'
    password = 'Dev!2020'
    db = 'healthcare'
    port = 3306
    pool = PooledDB(pymysql, 1, host=mysql_host, user=user, passwd=password, db=db, port=port)
    return pool


def connect_mysql2():  # 用于连接sleepsession表
    mysql_host = 'rm-bp1puzeu213v3gcc90o.mysql.rds.aliyuncs.com'  # '192.168.187.131'    '192.168.194.20'
    user = 'dothenry'
    password = 'ore$2020'
    db = 'test_db'
    port = 3306
    pool2 = PooledDB(pymysql, 1, host=mysql_host, user=user, passwd=password, db=db, port=port)
    return pool2


def get_information(i):
    sql = "select * from darmainformation"
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
        if i == -1:
            print("查询用户个数成功,当前的用户数为：", len(res))
            return len(res)
        else:
            res = res[i]
            tel = res[4]
            deviceNo = res[3]
            userName = res[2]
            userId = res[1]
            return tel, deviceNo, userName, userId
    except Exception as e:
        conn.rollback()
        if i == -1:
            print("查询用户个数失败")
        else:
            print("查询用户关联信息")
        return -1


def liveData(tel, date, name):
    sql = """SELECT* FROM livedata
    where username='""" + str(tel) + "' and Datatime like '" + str(date) + "%'"
    all_data = []
    column = ['time', 'breathe', 'heartRate', 'motion']
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.commit()
        print(date, "sleep tracker 数据库查询成功,共查询到", len(result), '条实时数据')
        if len(result) == 0:
            # print("当前日期无sleep的实时数据")
            return ''
        else:
            for i in result:
                if i[5] == 0 or i[6] == 0:
                    continue
                data = [i[16], i[5], i[6], i[7]]
                all_data.append(data)
            if len(all_data) == 0:
                print(name, "无数据或心跳呼吸数据持续为0")
                return ''
            else:
                data_to_csv = pd.DataFrame(columns=column, data=all_data)

                if not os.path.exists('./data/sleep_livedata/'):
                    os.makedirs('./data/sleep_livedata/')
                data_to_csv.to_csv('./data/sleep_livedata/' + str(name) + "_" + str(date) + '(sleep).csv',
                                   encoding='utf_8_sig', index=False)
                return data_to_csv
    except Exception as e:
        conn.rollback()
        print("sleep数据库查询失败")
        return ''


def darmaData(deviceNo, date, userName):
    sql = """SELECT*
    FROM darma_data
    where deviceNo='""" + str(deviceNo) + "' and timestamp like '" + str(date) + "%'"
    all_data = []
    column = ['time', 'breathe', 'heartRate', 'motion']
    try:
        cursor.execute(sql)
        conn.commit()
        result = cursor.fetchall()
        # print("darma数据库查询成功")
        if len(result) == 0:
            print("当前日期无darma的实时数据")
            return ''
        else:
            for i in result:
                if i[3] == '0' or i[4] == '0' or i[3] == '-100' or i[4] == '65436' or i[5] == '-100':
                    continue
                data = [i[2], i[3], i[4], i[5]]
                all_data.append(data)
            data_to_csv = pd.DataFrame(columns=column, data=all_data)

            if not os.path.exists('./data/darma_livedata/'):
                os.makedirs('./data/darma_livedata/')
            data_to_csv.to_csv('./data/darma_livedata/' + str(userName) + "_" + str(date) + '(darma).csv',
                               encoding='utf_8_sig', index=False)
            print(date, "darma 数据库查询成功,共查询到", len(all_data), '条darma的实时数据')
            return data_to_csv
    except Exception as e:
        conn.rollback()
        print("darma数据库查询失败")
        return ''


def errorAnalysis(liveData, darmaData, isShow):
    if len(liveData) != 0 and len(darmaData) != 0:
        # print("开始误差分析")
        all_data = pd.merge(liveData, darmaData, on='time')
        all_data['heart_error'] = all_data[['heartRate_y', 'heartRate_x']].apply(
            lambda x: round(abs(int(x[0]) - int(x[1])) / int(x[0]), 4), axis=1)
        all_data['breath_error'] = all_data[['breathe_y', 'breathe_x']].apply(
            lambda x: round(abs(int(x[0]) - int(x[1])) / int(x[0]), 4), axis=1)
        all_data['motion_error'] = all_data[['motion_y', 'motion_x']].apply(
            lambda x: round(abs(int(x[0]) - int(x[1])), 4), axis=1)

        heart_error_avg = round(np.mean(all_data['heart_error']), 4)
        breath_error_avg = round(np.mean(all_data['breath_error']), 4)
        motion_error_avg = round(np.mean(all_data['motion_error']), 4)
        print('心跳总误差为：', heart_error_avg, '呼吸总误差为：', breath_error_avg,
              '体动差值为：', motion_error_avg)

        if isShow == True:
            mpl.rcParams['font.sans-serif'] = ['SimHei']  # 正常显示中文
            plt.subplot(311)
            plt.plot(all_data['time'], all_data['heart_error'], 'b')
            num = int(len(list(all_data['time'])) / 2)
            plt.xticks(list(all_data['time'])[::num], all_data['time'][::num])  # 间隔显示横轴的日期
            plt.title('哈哈 heart_error')

            plt.subplot(312)
            plt.plot(all_data['time'], all_data['breath_error'], 'g')
            num = int(len(list(all_data['time'])) / 2)
            plt.xticks(list(all_data['time'])[::num], all_data['time'][::num])
            plt.title('breath_error')

            plt.subplot(313)
            plt.plot(all_data['time'], all_data['motion_error'], 'y')
            num = int(len(list(all_data['time'])) / 2)
            plt.xticks(list(all_data['time'])[::num], all_data['time'][::num])
            plt.title('motion_error')
            plt.tight_layout()  # 自适应每个子图的间距
            plt.show()
        return heart_error_avg, breath_error_avg, motion_error_avg
    else:
        print("不满足误差分析条件，当天darma或sleep数据缺失")
        return -100, -100, -100


def queryDarmaReport(name, deviceNo, date):
    sql = """ select * 
    from darmasession
    where deviceNo='""" + str(deviceNo) + """' and time='""" + str(date) + """'"""
    column = ['name', 'time', 'sleepScore', 'sleepEfficiency', 'sleepLevel']
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
        if len(res) != 0:
            res = res[0]
            sleepScore = res[2]
            sleepEfficiency = res[3]
            sleepLevel = res[4]
            data = (name, date, sleepScore, sleepEfficiency, sleepLevel)
            allDarmaReport.append(data)
            darmaReport = pd.DataFrame(columns=column, data=allDarmaReport)

            if not os.path.exists('./data/darma_report_data/'):
                os.makedirs('./data/darma_report_data/')
            darmaReport.to_csv('./data/darma_report_data/' + str(name) + '_' + str(date) + '(report).csv',
                               encoding='utf_8_sig', index=False)
            print(date, "darma睡眠报告查询成功,darma_sleepScore:", sleepScore, "darma_sleepEfficiency:", sleepEfficiency,
                  "darma_sleepLevel:", sleepLevel)
        else:
            print(str(date), "当前日期没有生成darma报告")
    except Exception as e:
        print(str(date), "darma报告获取失败")
        conn.rollback()


def querySleepReport(id, date, userName):
    # date='2020-12-27' #调试用
    sql = """select * from sleepsession where userId= """ + str(id) + """ and sleepSessionDate like '""" + str(
        date) + "%'"
    column = ['name', 'startTime', 'endTime', 'sleepScore', 'sleepEfficiency']
    try:
        cursor2.execute(sql)
        res = cursor2.fetchall()
        all_data = []
        if len(res) != 0:
            for i in res:
                sql2 = """ select  recordDate from sleeprecord where sleepSessionId=""" + str(
                    i[0]) + " order by recordDate desc limit 1"
                try:
                    cursor2.execute(sql2)
                    endTime = cursor2.fetchall()[0][0]
                    data = (userName, i[5], endTime, i[16], i[17])
                    all_data.append(data)
                    print("sleep tracker睡眠报告查询成功,StartTime:", i[5], "endTime:", endTime, "sleep_tracker_sleepScore:",
                          i[16], "sleep_tracker_sleepEfficiency:", i[17])
                except Exception as e:
                    print("sleep报告获取失败")
                    conn.rollback()

            sleepReport = pd.DataFrame(columns=column, data=all_data)
            if not os.path.exists('./data/sleep_report_data/'):
                os.makedirs('./data/sleep_report_data/')
            sleepReport.to_csv('./data/sleep_report_data/' + str(userName) + '_' + str(date) + '(report).csv',
                               encoding='utf_8_sig', index=False)
        else:
            print(str(date), "当前日期没有生成sleep报告")

    except Exception as e:
        print(str(date), "sleep报告获取失败")
        conn.rollback()


def errorAnalysisData_toDatabase(time, userId, userName, heart_error, breath_error, motion_error):
    sql2 = """select * from darma_and_sleep_erroranalysis where userId=""" + str(userId) + """ and date='""" + str(
        time) + """'"""
    print(sql2)
    try:
        cursor.execute(sql2)
        res = cursor.fetchall()
        if len(res) == 0:
            data = (userId, userName, str(time), str(heart_error), str(breath_error), str(motion_error))
            sql = """INSERT INTO darma_and_sleep_erroranalysis
                                     (userId,userName,date,heart_error,breath_error,motion_error)
                                     VALUES""" + str(data)
            try:
                cursor.execute(sql)
                conn.commit()
                print(userName, "误差分析数据插入成功")
            except Exception as e:
                conn.rollback()
                print(userName, "误差分析数据插入失败")
        else:
            print("误差分析结果数据库存在重复数据,不进行插入操作")
    except Exception as e:
        conn.rollback()
        print("查询误差分析结果数据库是否存在重复数据失败")


def showAllErrorAnalysisData():
    column = ["userId", "userName", "date", "heart_error", "breath_error", "motion_error"]
    sql = """ select * from darma_and_sleep_erroranalysis"""
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
        res = pd.DataFrame(columns=column, data=res)
        print("\n", "总体误差分析结果为：")
        print(res)
    except Exception as e:
        conn.rollback()
        print("总体误差分析结果数据查询失败")


if __name__ == '__main__':
    pool = connect_mysql()  # 打开数据库连接
    conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
    cursor = conn.cursor()

    pool2 = connect_mysql2()  # 连接sleepsession表
    conn2 = pool2.connection()
    cursor2 = conn2.cursor()

    isShow = False  # 是否进行图形化误差分析展示

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    number = get_information(-1)  # 用户个数
    allErrorAnalysisData = []
    column1 = ['time', 'name', 'heart_error_avg', 'breath_error_avg', 'motion_error_avg']
    column2 = ['name', 'time', 'sleepScore', 'sleepEfficiency', 'sleepLevel']
    if number != -1:
        for i in range(number):
            print('\n')
            allDarmaReport = []
            tel, deviceNo, userName, userId = get_information(i)  # 获取用户信息
            print("tel:", tel, "deviceNo:", deviceNo, "userName:", userName, "userId:", userId)
            live = liveData(tel, today, userName)  # 获得sleep实时数据，并导出为csv文件
            darma = darmaData(deviceNo, today, userName)  # 获得darma数据，并导出为csv文件

            heart_error_avg, breath_error_avg, motion_error_avg = errorAnalysis(live, darma, isShow)  # 误差分析
            if heart_error_avg != -100:
                data1 = (today, userName, heart_error_avg, breath_error_avg, motion_error_avg)
                errorAnalysisData_toDatabase(today, userId, userName, heart_error_avg, breath_error_avg,
                                             motion_error_avg)  # 保存误差结果到数据库
                allErrorAnalysisData.append(data1)

            queryDarmaReport(userName, deviceNo, yesterday)  # darma睡眠报告
            querySleepReport(userId, yesterday, userName)  # 我司体征检测睡眠报告

        errorAnalysisData = pd.DataFrame(columns=column1, data=allErrorAnalysisData)
        print(errorAnalysisData)

        if not os.path.exists('./data/error_analysis_data/'):
            os.makedirs('./data/error_analysis_data/')
        errorAnalysisData.to_csv('./data/error_analysis_data/' + str(today) + '(errorAnalysis).csv',
                                 encoding='utf_8_sig', index=False)

        showAllErrorAnalysisData()
