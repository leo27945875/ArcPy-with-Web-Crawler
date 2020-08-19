import arcpy
import os
import json
import copy
from pprint import pprint
import args

# ArcGIS env settings:
aprx = arcpy.mp.ArcGISProject(args.projectPath)
map3D = aprx.listMaps(args.mapName)[0]
gdb = aprx.defaultGeodatabase
arcpy.env.workspace = gdb
arcpy.env.overwriteOutput = True
twd97 = arcpy.SpatialReference(3826)
wgs84 = arcpy.SpatialReference(4326)
map3D.addBasemap(args.basemap)


# Reset some variable in args:
args.gdb = gdb


# Some newest data:
elecNewestData = None
airboxNewestData = None


# Tree data settings:
treeLyrName = args.treeLyrName
treeFeatures = ['treeID', 'name', 'growthFrom', 'treeCrown', 'treeHeight', 'TCO2', 'response', 'z'] # Don't change !
if not arcpy.Exists(treeLyrName):
    arcpy.management.CreateFeatureclass('', treeLyrName, 'Point', spatial_reference = twd97)
    arcpy.management.AddField(treeLyrName, treeFeatures[0], 'TEXT' , field_length = 20, field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[1], 'TEXT' , field_length = 20, field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[2], 'TEXT' , field_length = 20, field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[3], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[4], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[5], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[6], 'SHORT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(treeLyrName, treeFeatures[7], 'FLOAT', field_is_nullable = "NULLABLE")
    print(f"\nCreated feature class, [{treeLyrName}]!\n")
else:
    print(f"\nFeature class, [{treeLyrName}], has already existed! Needn't create new feature class!\n")


# Building data settings:
buildingMultiPatchLyrPath = os.path.join(gdb, args.buildingMultiPatchLyr)
buildingGeoLyrName = args.buildingGeoLyrName
if not arcpy.Exists(buildingGeoLyrName):
    arcpy.management.CopyFeatures(buildingMultiPatchLyrPath, buildingGeoLyrName)
    for buildingTemplateField in args.buildingTemplateFields:
        arcpy.management.AddField(buildingGeoLyrName, **buildingTemplateField)

    print(f"\nCreated feature class, [{buildingGeoLyrName}]!\n")
else:
    print(f"\nFeature class, [{buildingGeoLyrName}], has already existed! Needn't create new feature class!\n")


buildingTblName = args.buildingTblName
building3D_LyrName = args.building3D_LyrName
buildingFeatures = ['uid', 'building_name', 'type', 'floor', 'basement', 'area', 'birth_year', 'height'] # Don't change !
if not arcpy.Exists(buildingTblName):
    arcpy.management.CreateTable('', buildingTblName)
    arcpy.management.AddField(buildingTblName, buildingFeatures[0], 'TEXT' , field_length = 20, field_is_nullable = "NON_NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[1], 'TEXT' , field_length = 40, field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[2], 'TEXT' , field_length = 10, field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[3], 'SHORT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[4], 'SHORT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[5], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[6], 'SHORT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(buildingTblName, buildingFeatures[7], 'FLOAT', field_is_nullable = "NULLABLE")
    print(f"\nCreated feature class, [{buildingTblName}]!\n")
else:
    print(f"\nFeature class, [{buildingTblName}], has already existed! Needn't create new feature class!\n")


# Elec data settings:
elecTblName = args.elecTblName
elecFeatures = ['uid', 'elec', 'year', 'month'] # Don't change !
if not arcpy.Exists(elecTblName):
    arcpy.management.CreateTable('', elecTblName)
    arcpy.management.AddField(elecTblName, elecFeatures[0], 'TEXT', field_length = 20, field_is_nullable = "NON_NULLABLE")
    arcpy.management.AddField(elecTblName, elecFeatures[1], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(elecTblName, elecFeatures[2], 'SHORT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(elecTblName, elecFeatures[3], 'SHORT', field_is_nullable = "NULLABLE")
    print(f"\nCreated feature class, [{elecTblName}]!\n")
else:
    print(f"\nFeature class, [{elecTblName}], has already existed! Needn't create new feature class!\n")


# Airbox data setting:
airboxTblName = args.airboxTblName
airboxFeatures = ['uid', 'datetime', 'PM25', 'version'] # Don't change !
if not arcpy.Exists(airboxTblName):
    arcpy.management.CreateTable('', airboxTblName)
    arcpy.management.AddField(airboxTblName, airboxFeatures[0], 'TEXT' , field_length = 20, field_is_nullable = "NON_NULLABLE")
    arcpy.management.AddField(airboxTblName, airboxFeatures[1], 'DATE' , field_is_nullable = "NULLABLE")
    arcpy.management.AddField(airboxTblName, airboxFeatures[2], 'FLOAT', field_is_nullable = "NULLABLE")
    arcpy.management.AddField(airboxTblName, airboxFeatures[3], 'DATE' , field_is_nullable = "NULLABLE")
    print(f"\nCreated feature class, [{airboxTblName}]!\n")
else:
    print(f"\nFeature class, [{airboxTblName}], has already existed! Needn't create new feature class!\n")


# Getting tree data events: -----------------------------------------------------------------------------
def AddTreeLayerToMap():
    treeLyr3DName = args.treeLyr3DName
    arcpy.ddd.FeatureTo3DByAttribute(treeLyrName, treeLyr3DName, 'z')
    lyr = arcpy.management.MakeFeatureLayer(treeLyr3DName, treeLyr3DName).getOutput(0)
    sym = lyr.symbology
    sym.renderer.symbol.color = {'RGB': [30, 130, 48, 0]}
    lyr.symbology = sym
    d = lyr.getDefinition('V2')
    d.useRealWorldSymbolSizes = True
    lyr.setDefinition(d)
    map3D.addLayer(lyr)


def GetOneTreeDataEvent(tree):
    try:
        with arcpy.da.InsertCursor(treeLyrName, ['SHAPE@XY']+treeFeatures) as treeCursor:
            tree = copy.deepcopy(tree)
            coord = tree.pop('twd97Coordinate')
            if coord[0] and coord[1]:
                newData = [coord]
                newData.extend(list(tree.values()))
                newData.append(0.)
                treeCursor.insertRow(newData)

    except Exception as e:
        print(f'\n----------\nError in GetOneTreeDataEvent:\n  {e}\n')
        print(f'Tree data: {tree}\n----------\n')


def GetAllTreeDataEvent(trees):
    try:
        print('-'*50+'\nFinished making tree layer!\n'+'-'*50)

    except Exception as e:
        print(f'\n----------\nError in GetAllTreeDataEvent:\n  {e}\n')


# Getting building data events: --------------------------------------------------------------------------
def AddBuildingLayerToMap():
    lyr = map3D.addDataFromPath(os.path.join(gdb, building3D_LyrName))
    sym = lyr.symbology
    sym.updateRenderer("GraduatedColorsRenderer")
    if args.buildingColorAttr['defaultFeild']:
        sym.renderer.classificationField = args.buildingColorAttr['defaultFeild']
    else:
        sym.renderer.classificationField = 'EUI'

    sym.renderer.breakCount = args.buildingColorAttr['breakCount']
    sym.renderer.colorRamp = aprx.listColorRamps(args.buildingColorAttr['colorRamp'])[0]
    lyr.symbology = sym


def Create3D_BuildingLayerWithBuildingData():
    print(f'Joining {buildingGeoLyrName} and {buildingTblName} ...')
    joinLyr = arcpy.management.AddJoin(buildingGeoLyrName, "UID", buildingTblName, "uid")

    if arcpy.Exists(building3D_LyrName):
        arcpy.management.Delete(building3D_LyrName)

    arcpy.management.CopyFeatures(joinLyr, building3D_LyrName)
    arcpy.management.DeleteField(building3D_LyrName, [ f'{buildingTblName}_uid',
                                                       f'{buildingTblName}_OBJECTID'])
    for field in arcpy.ListFields(building3D_LyrName):
        arcpy.management.AlterField(building3D_LyrName, field.name, field.aliasName)

    print(f'Finished join! (Created {building3D_LyrName}.)')
    print('-'*50)


def GetOneBuildingDataEvent(building):
    try:
        with arcpy.da.InsertCursor(buildingTblName, buildingFeatures) as buildingCursor:
            newData = list(building.values())
            newData.append(building['floor']*3. if type(building['floor']) == int else None)
            buildingCursor.insertRow(newData)

    except Exception as e:
        print(f'\n----------\nError in GetOneBuildingDataEvent:\n  {e}\n')
        print(f'Building data: {building}\n----------\n')


def GetAllBuildingDataEvent(buildings):
    try:
        print('-'*50+'\nFinished making building table!\n'+'-'*50)
        Create3D_BuildingLayerWithBuildingData()

    except Exception as e:
        print(f'\n----------\nError in GetAllBuildingDataEvent:\n  {e}\n')


# Getting elec data events: -----------------------------------------------------------------------------
def AddElecTableToMap():
    map3D.addDataFromPath(os.path.join(gdb, elecTblName))


def InsertNewestEUI_DataToBuildingLayer():
    print('Inserting newest EUI data into building_3D_Layer ...')
    elecToShow = {}
    with arcpy.da.SearchCursor(elecTblName, elecFeatures[:2], f"{elecFeatures[2]} = {elecNewestData['year']} and {elecFeatures[3]} = {elecNewestData['month']}") as cursor:
        for row in cursor:
            elecToShow[row[0]] = row[1]

    with arcpy.da.UpdateCursor(building3D_LyrName, ['UID', args.buildingTemplateFields[0]['field_name'], buildingFeatures[5]]) as cursor:
        for row in cursor:
            try:
                eui = elecToShow[row[0]]/row[2] if type(elecToShow[row[0]]) == float and row[2] else None
                newData = [row[0], eui, row[2]]
                cursor.updateRow(newData)

            except KeyError as e:
                print(f'Building UID not found: [{row[0]}] is not in [{elecTblName}].')


def GetOneElecDataEvent(elec):

    global elecNewestData

    try:
        with arcpy.da.InsertCursor(elecTblName, elecFeatures) as elecCursor:
            elecNewestData = elec
            newData = list(elec.values())
            elecCursor.insertRow(newData)

    except Exception as e:
        print(f'\n----------\nError in GetOneElecDataEvent:\n  {e}\n')
        print(f'Tree data: {elec}\n----------\n')


def GetAllElecDataEvent(elecs):
    try:
        print('-'*50+'\nFinished making elec table!\n'+'-'*50)
        InsertNewestEUI_DataToBuildingLayer()

    except Exception as e:
        print(f'\n----------\nError in GetAllElecDataEvent:\n  {e}\n')


# Getting building data events: --------------------------------------------------------------------------
def AddAirboxTableToMap():
    map3D.addDataFromPath(os.path.join(gdb, airboxTblName))


def InsertNewestAirbox_DataToBuildingLayer():
    print('Inserting newest airbox data into building_3D_Layer ...')
    airboxToShow = {}
    with arcpy.da.SearchCursor(airboxTblName, [airboxFeatures[0], airboxFeatures[2]], f"{airboxFeatures[3]} = date'{airboxNewestData['version']}'") as cursor:
        for row in cursor:
            airboxToShow[row[0]] = (row[1] if row[1] != 'N\A' else None)

    with arcpy.da.UpdateCursor(building3D_LyrName, ['UID', args.buildingTemplateFields[1]['field_name']]) as cursor:
        for row in cursor:
            try:
                newData = [row[0], airboxToShow[row[0]]]
                cursor.updateRow(newData)

            except KeyError as e:
                print(f'Building UID not found: [{row[0]}] is not in [{airboxTblName}].')


def GetOneAirboxDataEvent(airbox):

    global airboxNewestData

    try:
        with arcpy.da.InsertCursor(airboxTblName, airboxFeatures) as airboxCursor:
            airboxNewestData = airbox
            newData = [airbox['building_id'], airbox['timestamp'], airbox['c_d0'], airbox['version']]
            newData = list(map(lambda x: None if x == 'N/A' else x, newData))
            airboxCursor.insertRow(newData)

    except Exception as e:
        print(f'\n----------\nError in GetOneAirboxDataEvent:\n  {e}\n')
        print(f'Airbox data: {airbox}\n----------\n')


def GetAllAirboxDataEvent(airboxes):
    try:
        print('-'*50+'\nFinished making airbox table!\n'+'-'*50)
        InsertNewestAirbox_DataToBuildingLayer()

    except Exception as e:
        print(f'\n----------\nError in GetAllAirboxDataEvent:\n  {e}\n')


# Functions to upload GIS service: -------------------------------------------------------------------------
def AddAllDataToMap():
    AddTreeLayerToMap()
    AddBuildingLayerToMap()
    AddElecTableToMap()
    AddAirboxTableToMap()


def SaveArcGIS():
    aprx.save()
    print(f'[{args.project}] saved!')


def UploadGIS_Service():
    try:
        print('-'*50)
        print("Sign into ArcGIS portal ...")
        callbackUserData = arcpy.SignInToPortal(**args.userData)
        args.userData.update(callbackUserData)

        print("Uploading GIS layers ...")
        sharingDraft = map3D.getWebLayerSharingDraft(args.serverType, args.serviceType, args.service)
        sharingDraft.summary = args.sharingDraftAttr['summary']
        sharingDraft.tags = args.sharingDraftAttr['tags']
        sharingDraft.description = args.sharingDraftAttr['description']
        sharingDraft.overwriteExistingService = args.sharingDraftAttr['overwriteExistingService']
        sharingDraft.exportToSDDraft(args.sddraftOutputFilename)
        arcpy.server.StageService(args.sddraftOutputFilename, args.sdOutputFilename)

        outServiceDefinitions = arcpy.server.UploadServiceDefinition(**args.uploadAttr)
        for i in range(outServiceDefinitions.outputCount):
            args.serviceDefinitions[args.serviceDefinitionFeatures[i]] = outServiceDefinitions.getOutput(i)

        print( 'Successfully uploaded GIS service!')
        print( 'Service definitions:')
        pprint(args.serviceDefinitions)

        with open(args.serviceDefinitionsJSON, 'w', encoding = 'utf-8') as f:
            json.dump(args.serviceDefinitions, f, ensure_ascii = False)

    except Exception as e:
        print(f'\n----------\nError in UploadGIS_Service:\n  {e}\n')

    print('-'*50)

# -----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    UploadGIS_Service()
    pass




















