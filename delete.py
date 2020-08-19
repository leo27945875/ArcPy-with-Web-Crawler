import arcpy
import args

def Delete(lyrName):
    aprx = arcpy.mp.ArcGISProject(args.projectPath)
    gdb = aprx.defaultGeodatabase
    lyrPath = gdb+'\\'+lyrName
    if arcpy.Exists(lyrPath):
        arcpy.management.Delete(lyrPath)
        print(f'[{lyrPath}] deleted !')
    else:
        print(f"There is no [{lyrPath}]. Needn't delete it!")

if __name__ == '__main__':
    toDeletes = ['NTU_Airbox', 'NTU_Elec', 'NTU_Building', 'NTU_Building_3D_Template', 'NTU_Building_3D_with_Data', 'NTU_Trees', 'NTU_Trees_3D']
    for toDelete in toDeletes:
        Delete(toDelete)