{{
  config(
    as_columnstore=false
  )
}}

WITH source_data AS (
SELECT
    nr.id,
    jc.album_type,
    total_tracks,
    jc.name AS release_name,
    jc.artists,
    jc.available_markets,
    jc.type AS release_type,
    jc.release_date,
    jc.release_date_precision,
    a.id AS artists_id,
    a.name AS artist_name,
    a.type AS artist_type

FROM dbo.new_releases nr
CROSS APPLY OPENJSON(nr.json_column)
    WITH (
        album_type NVARCHAR(50),
        total_tracks INT,
        artists NVARCHAR(MAX) AS JSON,
        available_markets NVARCHAR(MAX) AS JSON,
        release_date NVARCHAR(MAX),
        release_date_precision NVARCHAR(MAX),
        [name] NVARCHAR(MAX),
        [type] NVARCHAR(MAX)
    ) AS jc
CROSS APPLY OPENJSON(jc.artists)
    WITH (
        [id] NVARCHAR(MAX),
        [name] NVARCHAR(400),
        [type] NVARCHAR(400)
    ) AS a
)

SELECT *
FROM source_data