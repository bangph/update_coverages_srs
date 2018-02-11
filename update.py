# Update coverage's in petascopedb with new SRS
# Autho: Bang Pham Huu - b.phamhuu@jacobs-university.de
import psycopg2

try:
    conn = psycopg2.connect("dbname='petascopedb' user='postgres' password='postgres'")
except:
    print "I am unable to connect to the database."

cur = conn.cursor()


def execute_query(coverage_id, new_srs_name, query):
    new_srs_name_template = "$new_srs_name"
    coverage_id_template = "$coverage_id"
    query = query.replace(new_srs_name_template, new_srs_name)
    query = query.replace(coverage_id_template, coverage_id)
    try:
        cur.execute(query)
        conn.commit()
    except:
        print "I can't SELECT from bar"


def update_coverage_srs(coverage_id, new_srs_name):
    # update envelope's srs name
    query = "update envelope_by_axis \
    set srs_name = '$new_srs_name' \
    where envelope_by_axis_id = ( \
      select b.envelope_by_axis_id from coverage as a inner join envelope as b \
      on a.envelope_id = b.envelope_id \
      where coverage_id = '$coverage_id' \
    )"

    execute_query(coverage_id, new_srs_name, query)

    # update axis extents
    query = "update axis_extent set srs_name = '$new_srs_name' \
            where envelope_by_axis_id = ( \
            select b.envelope_by_axis_id from coverage as a inner join envelope as b \
            on a.envelope_id = b.envelope_id \
            where coverage_id = '$coverage_id')"
    execute_query(coverage_id, new_srs_name, query)

    # update general grid srs
    query = "update general_grid set  srs_name = '$new_srs_name' \
             where general_grid_id = ( \
             select a.general_grid_id from general_grid_domain_set as a \
             inner join coverage as b \
             on a.general_grid_domain_set_id  = b.domain_set_id \
             where coverage_id = '$coverage_id')"
    execute_query(coverage_id, new_srs_name, query)

    # update axes's srs names
    query = "update axis set srs_name = '$new_srs_name' \
            where axis_id in ( \
            select geo_axis_id from geo_axis where general_grid_id = ( \
            select general_grid_id from general_grid \
            where general_grid_id = ( \
            select a.general_grid_id from general_grid_domain_set as a \
            inner join coverage as b \
            on a.general_grid_domain_set_id  = b.domain_set_id \
            where coverage_id = '$coverage_id') ) )"
    execute_query(coverage_id, new_srs_name, query)


def update_crs_by_file(file_name, srs_name):
    i = 0
    with open(file_name) as f:
        for line in f:
            coverage_id = line.strip().lower()
            print str(i + 1) + ". Updating coverage '" + coverage_id + "'..."
            update_coverage_srs(coverage_id, srs_name)
            i += 1

update_crs_by_file("polarNorthComplete", 'http://access.planetserver.eu:8081/def/crs/PS/0/Mars-stereographic-north')
print "\n\n\n\n"
update_crs_by_file("polarSouthComplete", 'http://access.planetserver.eu:8081/def/crs/PS/0/Mars-stereographic-south')

"""select envelope_by_axis_id, srs_name from envelope_by_axis
where envelope_by_axis_id = (
  select b.envelope_by_axis_id from coverage as a inner join envelope as b
  on a.envelope_id = b.envelope_id
  where coverage_id = 'test_mean_summer_airtemp'  
)


select envelope_by_axis_id, srs_name from axis_extent
where envelope_by_axis_id = (
  select b.envelope_by_axis_id from coverage as a inner join envelope as b
  on a.envelope_id = b.envelope_id
  where coverage_id = 'test_mean_summer_airtemp'  
)


select general_grid_id, srs_name from general_grid
where general_grid_id = (
  select a.general_grid_id from general_grid_domain_set as a
   inner join coverage as b
   on a.general_grid_domain_set_id  = b.domain_set_id  
  where coverage_id = 'test_mean_summer_airtemp'  
)

# return list

select axis_id, srs_name from axis where axis_id in (
	select geo_axis_id from geo_axis where general_grid_id = (
		select general_grid_id from general_grid
		where general_grid_id = (
		  select a.general_grid_id from general_grid_domain_set as a
		   inner join coverage as b
		   on a.general_grid_domain_set_id  = b.domain_set_id  
		  where coverage_id = 'test_mean_summer_airtemp'  
		)
	)
)
"""
