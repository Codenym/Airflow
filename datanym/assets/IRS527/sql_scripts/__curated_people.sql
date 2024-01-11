with organizations as
        (with eins8871 as (select ein as id,
    first (organization_name order by material_change_date desc
   , final_report_indicator desc) as organization_name
   , min (established_date) as established_date
from staging.form8871_a_2address
group by 1
    )
        , eins8872 as (
select ein as id, first (organization_name order by period_end_date desc
        , final_report_indicator desc) as organization_name, min (org_formation_date) as established_date
from staging.form8872_a_2address
where ein not in (select distinct ein from eins8871)
group by 1
    )
select id, '527' as entity_type, organization_name as name, established_date
from eins8871
union all
select id, organization_name as name, established_date


with people as
    (select distinct 'Person' as entity_type, custodian_name as name, custodian_address_id as address_id
    from staging.form8871_2cast
    union
    select distinct 'Person' as entity_type, contact_person_name as name, contact_address_id as address_id
    from staging.form8871_2cast
    union
    select distinct 'Person' as entity_type, custodian_name as name, custodian_address_id as address_id
    from staging.form8871_2cast
    union
    select distinct 'Person' as entity_type, contact_person_name as name, contact_address_id as address_id
    from staging.form8871_2cast
    union
    select distinct 'Person' as entity_type, entity_name as name, entity_address_id as address_id
    from curated.form8871_directors_2cast
    union
    select distinct 'Person' as entity_type, entity_name as name, entity_address_id as address_id
    from curated.form8871_related_entities_2cast
    union
    select distinct 'Person' as entity_type, contributor_name as name, contributor_address_id as address_id
    from curated.form8872_schedule_a_2cast
    union
    select distinct 'Person' as entity_type, recipient_name as name, recipient_address_id as address_id
    from curated.form8872_schedule_b_2cast)


