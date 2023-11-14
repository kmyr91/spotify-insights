WITH source_data AS (
SELECT
    nr.id,
    jc.album_type,
    total_tracks,
    jc.artists,
    jc.available_markets,
    jc.release_date,
    jc.release_date_precision,
    a.id AS artists_id,
    a.name AS artist_name
FROM dbo.new_releases nr
CROSS APPLY OPENJSON(nr.json_column)
    WITH (
        album_type NVARCHAR(50),
        total_tracks INT,
        artists NVARCHAR(MAX) AS JSON,
        available_markets NVARCHAR(MAX) AS JSON,
        release_date NVARCHAR(MAX),
        release_date_precision NVARCHAR(MAX)
        -- artists NVARCHAR(MAX)
    ) AS jc
CROSS APPLY OPENJSON(jc.artists)
    WITH (
        [id] NVARCHAR(MAX),
        [name] NVARCHAR(400)
    ) AS a
)

SELECT *
FROM source_data