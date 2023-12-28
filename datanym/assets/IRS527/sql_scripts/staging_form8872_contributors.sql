select distinct
    upper(contributor_name)             as name,
    address_id                          as address_id,
    upper(contributor_employer)         as employer,
    upper(contributor_occupation)       as occupation
from
    $landing_form8872_schedule_a
        left join $curated_addresses as cus_add
                  on ((cus_add.address_1 = contributor_address_1) or (cus_add.address_1 is null and contributor_address_1 is null)) and
                     ((cus_add.address_2 = contributor_address_2) or (cus_add.address_2 is null and contributor_address_2 is null)) and
                     ((cus_add.city = contributor_address_city) or (cus_add.city is null and contributor_address_city is null)) and
                     ((cus_add.state = contributor_address_state) or (cus_add.state is null and contributor_address_state is null)) and
                     ((cus_add.zip_code = contributor_address_zip_code) or (cus_add.zip_code is null and contributor_address_zip_code is null)) and
                     ((cus_add.zip_ext = contributor_address_zip_ext) or (cus_add.zip_ext is null and contributor_address_zip_ext is null))
