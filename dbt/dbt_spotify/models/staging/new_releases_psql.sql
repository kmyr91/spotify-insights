with source_data as (
select 
	json_column::json ->> 'id' as id,
	json_column::json ->> 'name' as album_name
-- 	json_column::json ->> '{artists,0,name}' as artists
from new_releases
)
select * from source_data