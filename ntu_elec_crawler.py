import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from crawler import Crawler


class NTU_ElecCrawler(Crawler):
    def __init__(self, url, header = None, db = None, dbInfo = None,
                 isToDatabase = False, isCommit = False):
        super().__init__(db, dbInfo, isToDatabase, isCommit)
        self.url = url
        self.header = header
        self.form = {'ctg': 'all',
                     'yr': '',
                     'mn': '',
                     'ok': '%BDT%A9w'}
        self.insertBaseSQL = "insert into power_consumption(building_id, year, month, consumption) values{}"
        self.uids = None       # Means the uids that will be scraped from [self.url].
        self.uidsToUse = None  # Means the uids in [The_Building_UID_In_NTU.txt].
        self.GetBuildingUIDsToUse()


    def __call__(self, startYear = 100, endYear = None):
        try:
            self.ConnectDatabase()
            self.GetAllElecData(startYear, endYear)

        except Exception as e:
            print(f'-------\nError(elec):\n  {e}\n-------\n')

        else:
            self.CommitDatabase()

        finally:
            self.CloseDatabase()


    def GetBuildingUIDsToUse(self):
        buildingUIDs = []
        with open('The_Building_UID_In_NTU.txt', 'r') as f:
            for line in f:
                buildingUIDs.append(line.replace('\n', ''))

        self.uidsToUse = buildingUIDs


    def SumEverydayElecValue(self, column):
        nNone = 0
        notNoneValue = []
        for i, v in enumerate(column):
            if v == '---':
                nNone += 1
            else:
                notNoneValue.append(float(v))

        s = sum(notNoneValue)
        n = len(notNoneValue)
        if n:
            mean = s/n
            s += mean*nNone

        if s:
            return s
        else:
            return None


    def GetOneMonthElecData(self, year, month):
        self.form['yr'] = year
        self.form['mn'] = month
        resp = requests.post(url = self.url,
                             data = self.form,
                             params = self.header)
        resp.encoding = 'big5'
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('div') \
                    .find('div') \
                    .find('table') \
                    .find('tr') \
                    .find('td') \
                    .find('table') \
                    .find_all('tr')

        temp = []
        for i, row in enumerate(table):
            if i == 2:
                self.uids = row.text.split()[1:]

            if i >= 4:
                temp.append(row.text.split()[1:])

        values = []
        for i in range(len(temp[0])):
            column = []
            for j in range(len(temp)):
                column.append(temp[j][i])

            values.append(self.SumEverydayElecValue(column))

        dataMonthly = []
        for i, uid in enumerate(self.uids):
            if uid in self.uidsToUse:
                oneData = {}
                oneData['uid']   = uid
                oneData['elec']  = values[i]
                oneData['year']  = year
                oneData['month'] = month
                dataMonthly.append(oneData)

                newData = f"('{uid}', {year}, {month}, {values[i] if values[i] else 'NULL'})"
                self.SaveToDatabase(newData)

                if self.getOneDataEvents:
                    self.InvokeGetOneDataEvent(oneData)

        return dataMonthly


    def SortData(self, data):
        data = sorted(data, key = lambda x: x['month'])
        data = sorted(data, key = lambda x: x['year'])
        data = sorted(data, key = lambda x: x['uid'])

        return data


    def GetAllElecData(self, startYear = 100, endYear = None):
        data = []
        yearNow  = datetime.now().year-1911
        monthNow = datetime.now().month
        if endYear:
            if endYear > yearNow:
                print("Warning! The endYear > yearNow. Set endYear = yearNow automatically!")
                endYear = yearNow

            if endYear != yearNow:
                yearNow = endYear
                monthNow = 12

        if not startYear:
            startYear = yearNow-9

        print('\n'+'-'*50)
        print(f"Start scraping the data of elec from {startYear} to {yearNow} in NTU...\n")
        print('-'*50+'\n')
        for year in range(startYear, yearNow+1):
            if year == yearNow:
                for month in range(1, monthNow+1):
                    while True:
                        try:
                            print(f'Scraping elec data in {1911+year}/{month}...  ', end='')
                            t0 = time.time()
                            data.extend(self.GetOneMonthElecData(year, month))
                            print(f'required time: {round(time.time()-t0, 4)}(s)')
                            break

                        except Exception as e:
                            print(f'-------\nError in GetAllElecData:\n  {e}\n-------\nRestarting...')
                            time.sleep(0.5)

            else:
                for month in range(1, 13):
                    while True:
                        try:
                            print(f'Scraping elec data in {1911+year}/{month}...  ', end='')
                            t0 = time.time()
                            data.extend(self.GetOneMonthElecData(year, month))
                            print(f'required time: {round(time.time()-t0, 4)}(s)')
                            break

                        except Exception as e:
                            print(f'-------\nError in GetAllElecData:\n  {e}\n-------\nRestarting...')
                            time.sleep(0.5)

        if self.getAllDataEvents:
            self.InvokeGetAllDataEvent(data)


def Main():

    global crawlerE

    urlElec = "http://140.112.166.97/power/fn4/report7.aspx"
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.64"}
    msSQL_Info = dict(driver = '{SQL Server}',
                      server = 'MSI\MSSQL2019',
                      database = 'ntu_elec',
                      trusted_connection = 'yes')
    crawlerE = NTU_ElecCrawler(url = urlElec,
                               header = header,
                               db = 'MSSQL',
                               dbInfo = msSQL_Info,
                               isToDatabase = True,
                               isCommit = False)
    crawlerE()

    return 0


if __name__ == "__main__":
    Main()
















