WITH source_data AS (
    SELECT
        tt.id,
        tt.name,
        tt.popularity,
        jc.artists,
        a.id AS artist_id,
        a.name AS artist_name
    FROM dbo.top_tracks tt
    CROSS APPLY OPENJSON(tt.json_column)
        WITH (
            name NVARCHAR(400),
            popularity INT,
            artists NVARCHAR(MAX) AS JSON
        ) AS jc
    CROSS APPLY OPENJSON(jc.artists)
        WITH (
            [id] NVARCHAR(MAX),
            [name] NVARCHAR(400)
        ) AS a
)

SELECT *
FROM source_data
