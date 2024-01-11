with eins8871 as (select ein as id,
    first (organization_name order by material_change_date desc
   , final_report_indicator desc) as organization_name,
    min(established_date) as established_date
from $staging_form8871_a
group by 1
    ), eins8872 as (
select ein as id, first (organization_name order by period_end_date desc
        , final_report_indicator desc) as organization_name,
    min(org_formation_date) as established_date
from $staging_form8872_a
where ein not in (select distinct ein from eins8871)
group by 1)
select id, organization_name, established_date
from eins8871
union all
select id, organization_name, established_date
from eins8872

