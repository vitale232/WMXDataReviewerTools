import os

import arcpy


def main():
    sde_file = r'Database Connections\dev_elrs_ad_Lockroot.sde'
    db_connection = arcpy.ArcSDESQLExecute(sde_file)
    versions = arcpy.da.ListVersions(sde_file)
    print(versions)
    print(dir(versions[0]))
    print(versions[0].name)
    print(versions[0].description)
    sql = (
        'SELECT DISTINCT SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG ' +
        'FROM ELRS.elrs.LRSN_MILEPOINT ' +
        'WHERE DOT_ID = {dot_id} '.format(dot_id=100495) +
        'GROUP BY SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG'
    )
    # sql = (
    #     # 'SELECT MIN(DOT_ID), MIN(COUNTY_ORDER), MIN(DIRECTION), COUNT(1) ' +
    #     'SELECT DOT_ID, COUNTY_ORDER, DIRECTION, COUNT(*) '
    #     'FROM ELRS.ELRS.LRSN_MILEPOINT ' +
    #     'WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
    #     'GROUP BY DOT_ID, COUNTY_ORDER, DIRECTION '
    #     'HAVING COUNT( * )>1;'
    # )
    # sql = """
    # SELECT COUNTY_ORDER FROM ELRS.elrs.LRSN_MILEPOINT WHERE DOT_ID = '113417';
    # """
    # sql = "SELECT NEXT VALUE FOR S_RIS_ROADWAY_REGISTRY AS DOT_ID"
    # sql = "SELECT @@servername"
        # '''AND (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)
        # GROUP BY SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG;'''
    # sql = '''SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 1=1'''
    # sql = """
	# DECLARE @CurrentSDEVersion VARCHAR(100)

	# 	SELECT distinct @CurrentSDEVersion =  v.owner + '.' + v.name  FROM [SDE].[SDE_versions] v
	# 	inner join
	# 		[SDE].[SDE_states] la on v.state_id=la.state_id
	# 	inner join
    #         [SDE].[SDE_states] ls on ls.lineage_name = la.lineage_name;
    
    # PRINT @CurrentSDEVersion;
    # """
    # print(sql)
    
    response = db_connection.execute(sql)
    print(response)
    return

if __name__ == '__main__':
    main()
