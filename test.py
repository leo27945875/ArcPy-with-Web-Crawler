from arcgis.gis import GIS

id_ = '42035fc212994c2e94c1e7fb60febc6a&sublayer=0'
portal = 'https://learngis2.maps.arcgis.com/'
user = '0118805_LearnArcGIS'
pw = 'leo870604'

gis = GIS(portal, user, pw)
buildingItem = gis.content.get(id_)
buildingLyr = buildingItem.layers[0]
feature = buildingLyr.query()
print(feature.features)