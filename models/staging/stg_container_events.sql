
{{ config(materialized='view') }}

SELECT *
FROM {{ source('raw', 'CONTAINER_EVENTS') }}


WHERE EVENT_ID IS NOT NULL