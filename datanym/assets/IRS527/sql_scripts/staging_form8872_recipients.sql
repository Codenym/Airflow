select distinct
    uuid()                             as recipient_id,
    upper(reciepient_name)             as name,
    upper(reciepient_address_1)        as address_1,
    upper(reciepient_address_2)        as address_2,
    upper(reciepient_address_city)     as address_city,
    upper(reciepient_address_st)       as address_st,
    upper(reciepient_address_zip_code) as address_zip_code,
    upper(reciepient_address_zip_ext)  as address_zip_ext,
    upper(reciepient_employer)         as employer,
    upper(recipient_occupation)        as occupation
from
    $landing_form8872_schedule_b

