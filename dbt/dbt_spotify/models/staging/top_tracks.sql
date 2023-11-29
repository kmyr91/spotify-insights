WITH source_data AS (
    SELECT
        tt.id,
        JSON_VALUE(tt.json_column, '$.name') AS track_name,
        JSON_VALUE(tt.json_column, '$.popularity') AS popularity,
        a.id AS artist_id,
        a.name AS artist_name
    FROM dbo.top_tracks tt
    CROSS APPLY OPENJSON(tt.json_column, '$.artists') WITH (
        [id] NVARCHAR(MAX) '$.id',
        [name] NVARCHAR(400) '$.name'
    ) AS a
)

SELECT *
FROM source_data;
