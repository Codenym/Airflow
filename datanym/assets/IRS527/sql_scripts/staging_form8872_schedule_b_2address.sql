select
    form_id_number
    sched_b_id,
    org_name,
    ein,
    recipient_name,
    a.id as recipient_address_id,
    recipient_employer,
    expenditure_amount,
    recipient_occupation,
    expenditure_date,
    expenditure_purpose
from $staging_form8872_schedule_b_1cast
         left join $curated_addresses a
                   on
                           (a.state = recipient_address_state or
                            (a.state is null and recipient_address_state is null)) and
                           (a.zip_code = recipient_address_zip_code or
                            (a.zip_code is null and recipient_address_zip_code is null)) and
                           (a.city = recipient_address_city or
                            (a.city is null and recipient_address_city is null)) and
                           (a.address_1 = recipient_address_1 or
                            (a.address_1 is null and recipient_address_1 is null)) and
                           (a.address_2 = recipient_address_2 or
                            (a.address_2 is null and recipient_address_2 is null)) and
                           (a.zip_ext = recipient_address_zip_ext or
                            (a.zip_ext is null and recipient_address_zip_ext is null))
