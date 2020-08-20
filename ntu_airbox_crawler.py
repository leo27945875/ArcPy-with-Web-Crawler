import requests
import gzip
import json
import time
import pandas as pd
import args
from crawler import Crawler


class NTU_AirboxCrawler(Crawler):
    def __init__(self, url, header = None, db = None, dbInfo = None,
                 isToDatabase = False, isCommit = False):
        super().__init__(db, dbInfo, isToDatabase, isCommit)
        self.url = url
        self.header = header
        self.insertBaseSQL = "insert into airbox(building_id, datetime, device_id, T, PM25, PM10, PM1, RH, C_PM25, C_Method, version) values{}"
        self.features = ['timestamp', 'device_id', 's_t0', 's_d0', 's_d1', 's_d2', 's_h0', 'c_d0', 'c_d0_method']
        self.lastVersion = None


    def __call__(self):
        try:
            self.ConnectDatabase()
            self.GetAllPM25Data()

        except Exception as e:
            print(f'-------\nError(airbox):\n  {e}\n-------\n')

        else:
            self.CommitDatabase()

        finally:
            self.CloseDatabase()


    def TransformTimeFormSQL(self, t):
        return t.replace('T', ' ').replace('Z', '')


    def TransformTimeFormGIS(self, t):
        t = time.strptime(t, r'%Y-%m-%d %H:%M:%S')
        t = time.strftime(r'%d/%m/%Y %H:%M:%S', t)

        return t


    def GetAllPM25Data(self):
        print('-'*50)
        print('Start scraping Airbox data ...')
        while True:
            try:
                resp = requests.get(self.url)
                break

            except Exception as e:
                print(f'-------\nError in GetAllPM25Data:\n  {e}\n-------')
                print('Restarting ...\n--------\n')
                time.sleep(0.5)

        content = gzip.decompress(resp.content).decode('utf-8')
        content = eval(content)
        self.lastVersion = self.TransformTimeFormSQL(content['version'])
        print(f'\nVersion: {self.lastVersion} (UTC+0)\n')
        allData = self.TidyData(content)

        if self.getAllDataEvents:
            self.InvokeGetAllDataEvent(allData)

        print('-'*50)


    def GetOneAirboxData(self, uid, feed):
        data = {'building_id': uid}
        data.update({key: (feed[key] if key != 'timestamp' else self.TransformTimeFormSQL(feed[key])) for key in self.features})
        data['version'] = self.TransformTimeFormSQL(self.lastVersion)
        self.SaveToDatabase(data)

        if self.getOneDataEvents:
            self.InvokeGetOneDataEvent(data)

        return data


    def TidyData(self, content):
        datas = []
        uids = args.buildingUIDs
        feeds = content['feeds']
        for i, uid in enumerate(uids):
            datas.append(self.GetOneAirboxData(uid, feeds[i]))

        return datas


    def SaveToDatabase(self, data):
        data = list(data.values())
        data = list(map(lambda x: None if x == 'N/A' else x, data))
        data = f"('{data[0]}', '{data[1] or 'unknown'}', '{data[2] or 'unknown'}', {data[3] or 'NULL'}, {data[4] or 'NULL'}, {data[5] or 'NULL'}, {data[6] or 'NULL'}, {data[7] or 'NULL'}, {data[8] or 'NULL'}, '{data[9] or 'unknown'}', '{data[10] or 'unknown'}')"
        newdata = data.replace("'unknown'", 'NULL')
        print(newdata)
        super().SaveToDatabase(newdata)


def Main():

    global crawlerA

    urlAirBox = 'https://pm25.lass-net.org/data/last-all-airbox.json.gz'
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.64"}
    msSQL_Info = dict(driver = '{SQL Server}',
                      server = 'MSI\MSSQL2019',
                      database = 'NTU',
                      trusted_connection = 'yes')
    crawlerA = NTU_AirboxCrawler(urlAirBox,
                                 header,
                                 db = 'MSSQL',
                                 dbInfo = msSQL_Info,
                                 isToDatabase = True,
                                 isCommit = True)
    crawlerA()


if __name__ == "__main__":
    Main()
