class Crawler(object):
    def __init__(self, db, dbInfo, isToDatabase, isCommit):
        self.dbType = db
        self.dbInfo = dbInfo
        self.isToDatabase = isToDatabase
        self.isCommit = isCommit
        self.isDatabaseClose = True
        self.cursor = None
        self.insertBaseSQL = ''
        self.data = None
        self.getOneDataEvents = []
        self.getAllDataEvents = []

    def ConnectDatabase(self):
        if self.isToDatabase:
            while True:
                try:
                    print(f'Connecting to {self.dbType}...')
                    if self.dbType == 'MySQL':
                        import pymysql
                        self.db = pymysql.connect(**self.dbInfo)
                        print('Connected to MySQL!')
                    elif self.dbType == 'MSSQL':
                        import pyodbc
                        self.db = pyodbc.connect(**self.dbInfo)
                        print('Connected to MSSQL!')
                    else:
                        raise Exception(
                            f"Don't support {self.dbType} ! Please try another database.")

                    self.cursor = self.db.cursor()
                    self.isDatabaseClose = False
                    break

                except Exception as e:
                    print(f'-------\nError:\n  {e}\n-------\nRestarting...')

        else:
            print("No database is connected !")

    def SaveToDatabase(self, newData):
        if self.isToDatabase:
            sql = self.insertBaseSQL.format(newData)
            self.cursor.execute(sql)


    def CommitDatabase(self):
        if self.isToDatabase and self.isCommit:
            self.db.commit()
            print("Data commit finished !")


    def CloseDatabase(self):
        if self.isToDatabase:
            self.db.close()
            self.isDatabaseClose = True
            print("Database closed !")


    def InvokeGetOneDataEvent(self, oneData):
        for event in self.getOneDataEvents:
            event(oneData)


    def InvokeGetAllDataEvent(self, allData):
        for event in self.getAllDataEvents:
            event(allData)


    def SaveToPickle(self, filename):
        import pickle
        import copy

        toPickle = copy.deepcopy(self)

        del toPickle.db
        del toPickle.cursor

        with open(filename, 'wb') as f:
            pickle.dump(toPickle, f)
