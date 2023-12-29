-- CALL load_aws_credentials('codenym')
with contributors as (select *
                      from $curated_form8872_entities r
                               left join $curated_addresses a
                                         using (address_id))

select sched_b_id                 as sched_b_id,
       form_id_number             as form_id_number,
       cont.form8872_entity_id    as form8872_entity_id,
       cont.address_id            as address_id,
       cast(case
                when expenditure_amount = '' then null
                else expenditure_amount
           end as numeric)        as expenditure_amount,
       case
           when expenditure_date = '' then NULL
           else strptime(expenditure_date, '%Y%m%d'):: date
           end                    as expenditure_date,
       expenditure_purpose        as purpose
from $landing_form8872_schedule_b base
         left join contributors cont
                   on
                           (cont.state = upper(reciepient_address_st) or
                            (cont.state is null and reciepient_address_st is null)) and
                           (cont.zip_code = upper(reciepient_address_zip_code) or
                            (cont.zip_code is null and reciepient_address_zip_code is null)) and
                           (cont.city = upper(reciepient_address_city) or
                            (cont.city is null and reciepient_address_city is null)) and
                           (cont.address_1 = upper(reciepient_address_1) or
                            (cont.address_1 is null and reciepient_address_1 is null)) and
                           (cont.name = upper(reciepient_name) or
                            (cont.name is null and reciepient_name is null)) and
                           (cont.employer = upper(reciepient_employer) or
                            (cont.employer is null and reciepient_employer is null)) and
                           (cont.occupation = upper(recipient_occupation) or
                            (cont.occupation is null and recipient_occupation is null)) and
                           (cont.address_2 = upper(reciepient_address_2) or
                            (cont.address_2 is null and reciepient_address_2 is null)) and
                           (cont.zip_ext = upper(reciepient_address_zip_ext) or
                            (cont.zip_ext is null and reciepient_address_zip_ext is null))