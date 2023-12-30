with contributors as (select *
                      from $curated_form8872_entities r
                               left join $curated_addresses a
                                         using (address_uuid))
select sched_a_id                                               as sched_a_id,
       form_id_number                                           as form_id_number,
       cont.form8872_entity_uuid                                  as form8872_entity_id,
       cont.address_uuid                                          as address_uuid,
       cast(case
                when contribution_amount = ''
                    then null
                else contribution_amount end as numeric)        as contribution_amount,
       case
           when contribution_date = '' then NULL
           else strptime(contribution_date, '%Y%m%d')::date end as contribution_date
from $landing_form8872_schedule_a base
         left join contributors cont
                   on (cont.state = upper(contributor_address_state) or
                       (cont.state is null and contributor_address_state is null)) and
                      (cont.zip_code = upper(contributor_address_zip_code) or
                       (cont.zip_code is null and contributor_address_zip_code is null)) and
                      (cont.city = upper(contributor_address_city) or
                       (cont.city is null and contributor_address_city is null)) and
                      (cont.address_1 = upper(contributor_address_1) or
                       (cont.address_1 is null and contributor_address_1 is null)) and
                      (cont.name = upper(contributor_name) or (cont.name is null and contributor_name is null)) and
                      (cont.employer = upper(contributor_employer) or
                       (cont.employer is null and contributor_employer is null)) and
                      (cont.occupation = upper(contributor_occupation) or
                       (cont.occupation is null and contributor_occupation is null)) and
                      (cont.address_2 = upper(contributor_address_2) or
                       (cont.address_2 is null and contributor_address_2 is null)) and
                      (cont.zip_ext = upper(contributor_address_zip_ext) or
                       (cont.zip_ext is null and contributor_address_zip_ext is null))


