drop table if exists form8872_expenditures;
CREATE TABLE form8872_expenditures
    (
        expenditure_id      varchar(9) primary key,
        form_id_number      varchar(9),
        recipient_id        integer,
        expenditure_amount  numeric,
        expenditure_date    date,
        expenditure_purpose varchar(750)
--         foreign key (form_id_number) references form8872 (form_id_number),
--         foreign key (recipient_id) references form8872_recipients (recipient_id)
    )
;

insert into
    form8872_expenditures (expenditure_id, form_id_number, recipient_id, expenditure_amount, expenditure_date,
                           expenditure_purpose)
select
    sched_b_id                          as expenditure_id,
    form_id_number,
    recipient_id,
    cast(case when expenditure_amount = '' then null else expenditure_amount end as numeric) as expenditure_amount,
    to_date(expenditure_date,'YYYYMMDD')      as expenditure_date,
    expenditure_purpose
from
    landing.form8872_schedule_b_landing
        left join form8872_recipients r
                  on (name = upper(reciepient_name) or (name is null and reciepient_name is null)) and
                     (address_1 = upper(reciepient_address_1) or
                      (address_1 is null and reciepient_address_1 is null)) and
                     (address_2 = upper(reciepient_address_2) or
                      (address_2 is null and reciepient_address_2 is null)) and
                     (address_city = upper(reciepient_address_city) or
                      (address_city is null and reciepient_address_city is null)) and
                     (address_state = upper(reciepient_address_st) or
                      (address_state is null and reciepient_address_st is null)) and
                     (address_zip_code = upper(reciepient_address_zip_code) or
                      (address_zip_code is null and reciepient_address_zip_code is null)) and
                     (address_zip_ext = upper(reciepient_address_zip_ext) or
                      (address_zip_ext is null and reciepient_address_zip_ext is null)) and
                     (employer = upper(reciepient_employer) or
                      (employer is null and reciepient_employer is null)) and
                     (occupation = upper(recipient_occupation) or
                      (occupation is null and recipient_occupation is null))
;
