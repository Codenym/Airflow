with contributors as (select *
                      from $curated_form8872_entities r
                               left join $curated_addresses a
                                         using (address_id))
select sched_a_id                                                                                   as contribution_id,
       form_id_number,
       cont.form8872_entity_id,
       cont.address_id,
       cast(case when contribution_amount = '' then null else contribution_amount end as numeric)   as contribution_amount,
       cast(case when agg_contribution_ytd = '' then null else agg_contribution_ytd end as numeric) as agg_contribution_ytd,
       case
           when contribution_date = '' then NULL
           else strptime(contribution_date, '%Y%m%d')::date end                                        as contribution_date
from
    $landing_form8872_schedule_a base
    left join contributors cont
on (name = upper (contributor_name) or (name is null and contributor_name is null)) and
    (employer = upper (contributor_employer) or
    (employer is null and contributor_employer is null)) and
    (occupation = upper (contributor_occupation) or
    (occupation is null and contributor_occupation is null)) and
    (address_1 = upper (contributor_address_1) or
    (address_1 is null and contributor_address_1 is null)) and
    (address_2 = upper (contributor_address_2) or
    (address_2 is null and contributor_address_2 is null)) and
    (city = upper (contributor_address_city) or
    (city is null and contributor_address_city is null)) and
    (state = upper (contributor_address_state) or
    (state is null and contributor_address_state is null)) and
    (zip_code = upper (contributor_address_zip_code) or
    (zip_code is null and contributor_address_zip_code is null)) and
    (zip_ext = upper (contributor_address_zip_ext) or
    (zip_ext is null and contributor_address_zip_ext is null))


