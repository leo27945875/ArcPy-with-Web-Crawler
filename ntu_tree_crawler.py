import requests
import json
import time
from crawler import Crawler


class NTU_TreeCrawler(Crawler):
    def __init__(self, urlAll, urlTree, urlGet, header = None, db = None, dbInfo = None,
                 isToDatabase = False, isCommit = False):
        super().__init__(db, dbInfo, isToDatabase, isCommit)
        self.urlAll  = urlAll
        self.urlTree = urlTree
        self.urlGet  = urlGet
        self.header  = header
        self.insertBaseSQL = "insert into tree(treeID, name, growthFrom, treeCrownHeight, treeHeight, twd97CoordinateX, twd97CoordinateY, TCO2) values('{}', '{}', '{}', {}, {}, {}, {}, {});"
        self.treeTypes = None

    def __call__(self):
        try:
            self.ConnectDatabase()
            self.GetTreeTypes()
            self.GetAllTreesdata()

        except Exception as e:
            print(f'-------\nError(tree):\n  {e}\n-------\n')

        else:
            self.CommitDatabase()

        finally:
            self.CloseDatabase()

    def CalculateTCO2(self, tree):
        growthFrom = tree['growthFrom']
        treeHeight = float(tree['treeHeight']) if tree['treeHeight'] else None
        treeName = tree['name']
        if growthFrom:
            if '灌木' in growthFrom:
                gi = 0.5
            elif growthFrom == '喬木':
                if treeHeight:
                    if treeHeight < 10:
                        gi = 1.5
                    elif treeHeight >= 10:
                        gi = 1.0
                    else:
                        return None

                else:
                    return None

            elif growthFrom == '海棗莖單生':
                gi = 0.66
            elif growthFrom == '藤本':
                gi = 0.4
            elif growthFrom == '蕨類' or growthFrom == '草本':
                gi = 0.3
            elif growthFrom == '叢生':
                if treeName:
                    if treeName == '短節泰山竹':
                        gi = 1.0
                    elif treeName == '金絲竹':
                        gi = 1.0
                    elif treeName == '凍子椰子':
                        gi = 0.66
                    elif treeName == '雪佛里椰子':
                        gi = 0.66
                    elif treeName == '拔蕉':
                        gi = 0.5
                    else:
                        return None

                else:
                    return None

            else:
                return None

        else:
            return None

        if tree['treeCrownHeight'] and gi:
            ai = float(tree['treeCrownHeight'])
            return ai*gi
        else:
            return None

    def SaveToDatabase(self, tree):
        if tree['response'] == 1 and self.isToDatabase:
            sql = self.insertBaseSQL.format(tree['treeID'] or 'unknown',
                                            tree['name'] or 'unknown',
                                            tree['growthFrom'] or 'unknown',
                                            tree['treeCrownHeight'] or 'NULL',
                                            tree['treeHeight'] or 'NULL',
                                            tree['twd97Coordinate'][0] or 'NULL',
                                            tree['twd97Coordinate'][1] or 'NULL',
                                            tree['TCO2'] or 'NULL')
            sql = sql.replace("'unknown'", 'NULL')
            self.cursor.execute(sql)

    def GetOneTreeData(self, treeID):
        data = {'treeId': '18704', 'co': treeID}
        while True:
            try:
                resp = requests.post(self.urlGet, data = data, params = self.header)
                break

            except Exception as e:
                print(f'-------\nError in GetOneTreeData:\n  {e}\n-------')
                print('Restarting ...\n--------\n')
                time.sleep(0.5)

        try:
            resp.encoding = 'utf-8'
            tree = {}
            tree['treeID'] = treeID
            tree['name'] = None
            tree['growthFrom'] = None
            tree['treeCrownHeight'] = None
            tree['treeHeight'] = None
            tree['twd97Coordinate'] = [None, None]
            tree['TCO2'] = None
            tree['response'] = None
            try:
                resp = json.loads(resp.text)

            except Exception:
                print(f"\n{treeID} no response!")
                tree['response'] = 0

            else:
                if resp:
                    tree['name'] = resp['treeDb']['chineseTreeName'].replace(' ', '') or None
                    tree['growthFrom'] = resp['treeDb']['growthFrom'].replace(' ', '') or None
                    tree['treeCrownHeight'] = resp['tree']['treeCrownHeight'] or None
                    tree['treeHeight'] = resp['tree']['treeHeight'] or None
                    tree['twd97Coordinate'] = [resp['tree']['twd97CoordinateX'] or None,
                                               resp['tree']['twd97CoordinateY'] or None]
                    tree['TCO2'] = self.CalculateTCO2(tree)
                    tree['response'] = 1

                else:
                    print(f"\n{treeID} no response!")
                    tree['response'] = 0

            if self.getOneDataEvents:
                self.InvokeGetOneDataEvent(tree)

        except Exception as e:
            print(f'-------\nError in GetOneTreeData:\n  {e}\n-------')
            print(f'Tree data: {tree}')

        return tree

    def GetTreeTypes(self):
        print("Getting the types of trees in NTU ...", end = '')
        while True:
            try:
                resp = requests.get(self.urlAll)
                break

            except Exception as e:
                print(f'-------\nError in GetTreeTypes:\n  {e}\n-------')
                print('Restarting ...\n--------\n')
                time.sleep(0.5)

        treeTypesData = json.loads(resp.text)
        treeTypesData = treeTypesData['rows']
        treeTypes = []
        for treeTypeData in treeTypesData:
            treeTypes.append(treeTypeData['chineseTreeName'])

        self.treeTypes = set(treeTypes)
        print('Done.')

    def GetAllTreesdata(self):
        print("Getting the data of all trees ...")
        try:
            treeIDs = set()
            data = []
            for treeType in self.treeTypes:
                print('-'*50+f"\nCrawling {treeType}...", end = '')
                t0 = time.time()
                payload = {'searchKeyword': treeType, 'co': ''}
                while True:
                    try:
                        resp = requests.post(self.urlTree, data = payload, params = self.header)
                        break

                    except Exception as e:
                        print(f'-------\nError in GetAllTreesdata:\n  {e}\n-------')
                        print('Restarting ...\n--------\n')
                        time.sleep(0.5)

                resp.encoding = 'utf-8'
                resp = json.loads(resp.text)
                n = 0
                for treeData in resp:
                    treeID = treeData[1]
                    if treeID not in treeIDs:
                        treeIDs.add(treeID)
                        tree = self.GetOneTreeData(treeID)
                        data.append(tree)
                        self.SaveToDatabase(tree)
                        n += 1

                print(f' count: {n} ; required time: {round(time.time()-t0, 4)}(s)')

            print('-'*50+"\nStop crawling !\n"+'-'*50)

            if self.getAllDataEvents:
                self.InvokeGetAllDataEvent(data)

        except Exception as e:
            print(f'-------\nError in GetAllTreesdata:\n  {e}\n-------')
            print(f'Tree data: {tree}')


def Main():

    global crawlerT

    # Define basic parameters:
    urlAll = 'https://map.ntu.edu.tw/ntutree/permitAll/treeDb/listAll?_dc=1595382955236'
    urlTree = 'https://map.ntu.edu.tw/ntutree/permitAll/listTreeCoTreeName'
    urlGet = 'https://map.ntu.edu.tw/ntutree/permitAll/treeRegistration/get'
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.64"}

    # Define crawler and start crawling:
    msSQL_Info = dict(driver='{SQL Server}',
                      server='MSI\MSSQL2019',
                      database='ntu_tree',
                      trusted_connection='yes')
    crawlerT = NTU_TreeCrawler(
        urlAll, urlTree, urlGet, header, 'MSSQL', msSQL_Info, False, False)
    crawlerT()

    return 0


if __name__ == "__main__":
    Main()
