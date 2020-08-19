import requests
import json
import time
import args
from crawler import Crawler


class NTU_BuildingCrawler(Crawler):
    def __init__(self, url, header = None, db = None, dbInfo = None,
                 isToDatabase = False, isCommit = False):
        super().__init__(db, dbInfo, isToDatabase, isCommit)
        self.url = url
        self.header = header
        self.insertBaseSQL = 'insert into building(uid, name, type, floor, basement, area, birth_year) values{}'
        self.uids = None


    def __call__(self):
        try:
            self.ConnectDatabase()
            self.GetBuildingUIDs()
            self.GetAllBuildingsData()

        except Exception as e:
            print(f'-------\nError(building):\n  {e}\n-------\n')

        else:
            self.CommitDatabase()

        finally:
            self.CloseDatabase()


    def GetBuildingUIDs(self):
        self.uids = args.buildingUIDs

    def GetBuildingType(self, departs):
        n = {'學術單位': 0,
             '行政單位': 0,
             '學生宿舍': 0,
             '圖書館': 0,
             '運動設施': 0,
             '其他': 0,
             '教學大樓': 0}
        if departs:
            for depart in departs:
                if '學術單位' in depart['buildType1C']:
                    n['學術單位'] += 1
                elif '行政單位' in depart['buildType1C']:
                    n['行政單位'] += 1
                elif '學生宿舍' in depart['buildType1C']:
                    n['學生宿舍'] += 1
                elif '圖書館' in depart['buildType1C']:
                    n['圖書館'] += 1
                elif '運動設施' in depart['buildType1C']:
                    n['運動設施'] += 1
                elif '其他' in depart['buildType1C']:
                    n['其他'] += 1
                elif '教學大樓' in depart['buildType1C']:
                    n['教學大樓'] += 1

            n = [[key, value] for key, value in n.items()]
            buildType = sorted(n, key = lambda x: x[1], reverse = True)[0][0]

            return buildType
        else:
            return None


    def GetOneBuildingData(self, uid):
        payload = {'topic':'',
                   'type':'build',
                   'uid':uid,
                   'id':'',
                   'buildId':''}
        while True:
            try:
                resp = requests.post(self.url ,
                                     data = payload,
                                     params = self.header)
                resp = json.loads(resp.text)
                self.temp = resp
                break

            except Exception as e:
                print(f'-------\nError in GetOneBuildingData:\n  {e}\n-------\nRestarting ...\n---------\n')
                time.sleep(0.5)

        building = {}
        building['uid'] = uid
        building['building_name'] = None
        building['type'] = None
        building['floor'] = None
        building['basement'] = None
        building['area'] = None
        building['birth_year'] = None

        if 'buildingBound' in resp:
            bb  = resp['buildingBound']
            name = bb['name']
            buildType = self.GetBuildingType(resp['depart'])
            floor = int(bb['floor']) if bb['floor'] else None
            basement = int(bb['basement']) if bb['basement'] is not None else None
            area = float(bb['area'].replace(',', '')) if bb['area'] else None
            year = bb['year'] or None
            if year:
                year = year.split('/')[0]
                year = year.replace('(by inference)', '')
                if year.isdigit():
                    year = int(year)
                else:
                    year = None

            newData = f"('{uid}', '{name or 'unknown'}', '{buildType or 'unknown'}', {floor if type(floor) == int else 'NULL'}, {basement if type(basement) == int else 'NULL'}, {area or 'NULL'}, {year or 'NULL'})"
            newData = newData.replace("'unknown'", 'NULL')
            self.SaveToDatabase(newData)
            print(newData)

            building['building_name'] = name
            building['type'] = buildType
            building['floor'] = floor
            building['basement'] = basement
            building['area'] = area
            building['birth_year'] = year

        else:
            newData = f"('{uid}', NULL, NULL, NULL, NULL ,NULL, NULL)"
            self.SaveToDatabase(newData)
            print(newData)

        if self.getOneDataEvents:
            self.InvokeGetOneDataEvent(building)

        return building


    def GetAllBuildingsData(self):
        print('\n'+'-'*50)
        print("Start scraping the data of buildings in NTU...\n")
        print('-'*50+'\n')
        data = []
        for uid in self.uids:
            data.append(self.GetOneBuildingData(uid))

        if self.getAllDataEvents:
            self.InvokeGetAllDataEvent(data)


def Main():

    global crawlerB

    uids = ['AT2007', 'AT1035']
    urlBuilding = 'https://map.ntu.edu.tw/ntu.htm'
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.64"}
    msSQL_Info = dict(driver = '{SQL Server}',
                      server = 'MSI\MSSQL2019',
                      database = 'NTU',
                      trusted_connection = 'yes')
    crawlerB = NTU_BuildingCrawler(urlBuilding,
                                   header,
                                   db = 'MSSQL',
                                   dbInfo = msSQL_Info,
                                   isToDatabase = False,
                                   isCommit = False)
    crawlerB(uids)


if __name__ == "__main__":
    Main()








