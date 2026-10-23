{{ config(materialized='incremental', unique_key='id') }}

select id, customer_id, kind
from {{ source('app', 'raw_events') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
