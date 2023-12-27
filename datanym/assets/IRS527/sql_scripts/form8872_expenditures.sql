select
    sched_b_id               as expenditure_id,
    form_id_number,
    recipient_id,
    cast(case
             when expenditure_amount = '' then null
                                          else expenditure_amount
             end as numeric) as expenditure_amount,
    case
        when expenditure_date = '' then NULL
                                   else strptime(expenditure_date, '%Y%m%d')::date
        end                  as expenditure_date,
    expenditure_purpose
from
    $form8872_schedule_b_landing
        left join $form8872_recipients r on (name = upper(reciepient_name) or (name is null and reciepient_name is null)) and
                                           (address_1 = upper(reciepient_address_1) or (address_1 is null and reciepient_address_1 is null)) and
                                           (address_2 = upper(reciepient_address_2) or (address_2 is null and reciepient_address_2 is null)) and
                                           (address_city = upper(reciepient_address_city) or (address_city is null and reciepient_address_city is null)) and
                                           (address_st = upper(reciepient_address_st) or (address_st is null and reciepient_address_st is null)) and
                                           (address_zip_code = upper(reciepient_address_zip_code) or
                                            (address_zip_code is null and reciepient_address_zip_code is null)) and
                                           (address_zip_ext = upper(reciepient_address_zip_ext) or
                                            (address_zip_ext is null and reciepient_address_zip_ext is null)) and
                                           (employer = upper(reciepient_employer) or (employer is null and reciepient_employer is null)) and
                                           (occupation = upper(recipient_occupation) or (occupation is null and recipient_occupation is null))

