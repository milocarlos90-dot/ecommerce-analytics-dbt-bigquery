select
    {{ generate_staging_select('thelook_ecommerce', 'events') }}
from {{ source('thelook_ecommerce', 'events') }}
