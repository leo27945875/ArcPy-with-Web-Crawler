import os


# URL for tree data:
urlAll  = 'https://map.ntu.edu.tw/ntutree/permitAll/treeDb/listAll?_dc=1595382955236'
urlTree = 'https://map.ntu.edu.tw/ntutree/permitAll/listTreeCoTreeName'
urlGet  = 'https://map.ntu.edu.tw/ntutree/permitAll/treeRegistration/get'


# URL for elec data:
urlElec     = "http://140.112.166.97/power/fn4/report7.aspx"


# URL for building data:
urlBuilding = 'https://map.ntu.edu.tw/ntu.htm'


# URL for airbox data:
urlAirBox = 'https://pm25.lass-net.org/data/last-all-airbox.json.gz'


# SQL Server settings:
msSQL_Info = dict(driver = '{SQL Server}',
                  server = 'MSI\MSSQL2019',
                  database = 'NTU',
                  trusted_connection = 'yes')


# Crawler data settings:
mainControl = dict(isGetTreeData = True,
                   isGetBuildingData = True,
                   isGetElecData = True,
                   isGetAirboxData = True,
                   isToDatabase = False,
                   isCommit = False,
                   isToArcGIS = True,
                   isUploadArcGIS = True)
header = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.64"}
elecDataYearRange = [100, None]
buildingUIDs = []
with open('The_Building_UID_In_NTU.txt', 'r') as f:
    for line in f:
        buildingUIDs.append(line.replace('\n', ''))


# ArcGIS env settings:
project = 'NTUProject'
root = os.path.join('D:\\Download\\CAE_Internship\\ArcGIS\\Projects', project)
projectPath = os.path.join(root, f'{project}.aprx')
webDir = os.path.join(root, 'Web')
if not os.path.exists(webDir):
    os.mkdir(webDir)

mapName = 'Scene'
basemap = 'Human Geography Dark Map'
gdb = None


# Layers which must be prepared first:
buildingMultiPatchLyr = 'NTU_Building_3D'


# Names of layers to be created settings:
treeLyrName = 'NTU_Trees'
treeLyr3DName = treeLyrName+'_3D'

elecTblName = 'NTU_Elec'

buildingTblName = 'NTU_Building'
buildingGeoLyrName = 'NTU_Building_3D_Template'
buildingTemplateFields = [
                          {'field_name': 'Data' , 'field_type': 'FLOAT'},
                         ]
building3D_LyrName = buildingTblName+'_3D_with_Data'

airboxTblName = 'NTU_Airbox'


# Symbology settings:
buildingColorAttr = {
    'defaultFeild': 'EUI',
    'breakCount': 20,
    'colorRamp': '紫紅 (連續)'
}


# Web service settings:
userData = {
    'portal_url': 'https://learngis2.maps.arcgis.com/',
    'username': '0118805_LearnArcGIS',
    'password': 'leo870604'
}

service = "NTU_SDG_Project"
sddraftFilename = service + ".sddraft"
sddraftOutputFilename = os.path.join(webDir, sddraftFilename)
sdFilename = service + ".sd"
sdOutputFilename = os.path.join(webDir, sdFilename)

serverType = 'HOSTING_SERVER'
serviceType = 'FEATURE'
sharingDraftAttr = {
    'summary': "Test",
    'tags': "Test",
    'description': "Test",
    'overwriteExistingService': True
}

uploadAttr = {
    'in_sd_file': sdOutputFilename,
    'in_server': "HOSTING_SERVER",
    'in_startupType': "STARTED",
    'in_public': "PUBLIC",
    'in_override': "OVERRIDE_DEFINITION"
}

serviceDefinitionFeatures = ['soap_svc_url',
                             'rest_svc_url',
                             'mapServiceItemID',
                             'featServiceItemID',
                             'cached_service',
                             'featureServiceURL',
                             'mapServiceURL',
                             'LayerIDMap',
                             'standaloneTableIDMap',
                             'vectorTileServiceID',
                             'vectorTileServiceURL']
serviceDefinitions = {feature: '' for feature in serviceDefinitionFeatures}
serviceDefinitionsJSON = os.path.join(webDir, f'{service}_ServiceDefinition.json')












