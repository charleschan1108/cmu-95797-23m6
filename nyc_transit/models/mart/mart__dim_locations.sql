SELECT
    {{ dbt_utils.star(ref('taxi_zone_lookup')) }} -- star is select all columns in the table
from {{ ref('taxi_zone_lookup') }}