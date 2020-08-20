import args
import json
import copy
import time
import threading
from arcgis.gis import GIS
from ntu_elec_crawler import NTU_ElecCrawler


# Get the service definition:
with open(args.serviceDefinitionsJSON, 'r', encoding = 'utf-8') as f:
    serviceDefs = json.load(f)
    rootID = serviceDefs['featServiceItemID']
    lyrDefs = list(map(lambda x: x.split('|'), serviceDefs['LayerIDMap'].split(';')))
    tblDefs = list(map(lambda x: x.split('|'), serviceDefs['standaloneTableIDMap'].split(';')))

# Objects that can let us control things in ArcGIS server:
gis   = None
items = None

# Login ArcGIS server:
def LoginArcGIS_Server():

    global gis, items

    while True:
        try:
            gis  = GIS(**args.userData)
            break

        except Exception as e:
            print(f'--------\nError in LoginArcGIS_Server (getting gis):\n  {e}\n--------')
            print('Retrying ...')

    while True:
        try:
            items = gis.content.get(rootID)
            break

        except Exception as e:
            print(f'--------\nError in LoginArcGIS_Server (getting item):\n  {e}\n--------')
            print('Retrying ...')


# Get the item (means FeatureLayer or Table object) that matchs the name:
def GetItem(itemName):
    item = None
    for i, lyrDef in enumerate(lyrDefs):
        if itemName in lyrDef:
            item = items.layers[i]

    for i, tblDef in enumerate(tblDefs):
        if itemName in tblDef:
            item = items.tables[i]

    if item:
        return item
    else:
        raise Exception(f'Error in GetLayerInfo: [{featureName}] not found!')


# Get the last elec data in this month and insert it into the elec table:
def UpdateElecData():
    itemE    = GetItem(args.elecTblName)
    crawlerE = NTU_ElecCrawler(url = args.urlElec,
                               header = args.header,
                               db = args.databaseName,
                               dbInfo = args.SQL_Info,
                               isToDatabase = args.mainControl['isToDatabase'],
                               isCommit = args.mainControl['isCommit'])

    print('Getting new elec data ...')
    yearNow  = time.localtime().tm_year-1911
    monthNow = time.localtime().tm_mon
    adds = crawlerE.GetOneMonthElecData(yearNow, monthNow)

    print(f'Updating data in {args.elecTblName} (year = {yearNow}, month = {monthNow}) ...')
    features = itemE.query(where = f"{args.elecFeatures[2]} = {yearNow} and {args.elecFeatures[3]} = {monthNow}").features
    if features:
        for i, add in enumerate(adds):
            for key, value in add.items():
                features[i].set_value(key, value)

        itemE.edit_features(updates = features)
    else:
        template = itemE.query(where = "OBJECTID = 1").features[0]
        addFeatures = []
        for add in adds:
            newData = copy.deepcopy(template)
            for key, value in add.items():
                newData.set_value(key, value)

            addFeatures.append(newData)

        itemE.edit_features(adds = addFeatures)

    print(f'Successfully updated {args.elecTblName} (year = {yearNow}, month = {monthNow})!')


# Update elec data everyday:
def UpdateElecDataLoop():
    UpdateElecData()
    oldDay = time.localtime().tm_mday
    while True:
        try:
            nowDay = time.localtime().tm_mday
            if nowDay != oldDay:
                UpdateElecData()
                oldDay = nowDay

        except Exception as e:
            print(f'--------\nError in UpdateElecDataEvent:\n  {e}\n--------')
            print('Re-login ArcGIS server ...')
            LoginArcGIS_Server()
            time.sleep(3)


# All the update events:
def UpdateLoop():
    # Threadings:
    threads = {}
    threads['UpdateElecDataLoop'] = threading.Thread(target = UpdateElecDataLoop, name = 'UpdateElecDataLoop')

    # Main processes:
    print('Login ArcGIS server ...')
    LoginArcGIS_Server()

    print('Start update loop!\n'+'='*50)
    for thread in threads.values():
        thread.start()


if __name__ == "__main__":
    UpdateLoop()













