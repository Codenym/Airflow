select distinct
    upper(contributor_name)             as name,
    upper(contributor_address_1)        as address_1,
    upper(contributor_address_2)        as address_2,
    upper(contributor_address_city)     as address_city,
    upper(contributor_address_state)    as address_st,
    upper(contributor_address_zip_code) as address_zip_code,
    upper(contributor_address_zip_ext)  as address_zip_ext,
    upper(contributor_employer)         as employer,
    upper(contributor_occupation)       as occupation
from
    $form8872_schedule_a_landing

