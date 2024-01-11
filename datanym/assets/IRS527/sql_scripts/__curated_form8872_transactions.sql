select uuid() as form8872_transaction_uuid,
       *
from (select 'Schedule A (contribution)' as category,
             sched_a_id                  as source_id,
             form_id_number              as form_id_number,
             form8872_entity_id          as form8872_entity_id,
             address_uuid                as address_id,
             contribution_amount         as amount,
             contribution_date           as transaction_date,
             null                        as purpose
      from $staging_form8872_contributions
      union all
      select 'Schedule B (expenditure)' as category,
             sched_b_id                 as source_id,
             form_id_number             as form_id_number,
             form8872_entity_id         as form8872_entity_id,
             address_uuid               as address_id,
             expenditure_amount         as amount,
             expenditure_date           as transaction_date,
             upper(purpose)                    as purpose
      from $staging_form8872_expenditures)

