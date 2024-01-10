select uuid() as form8872_entity_uuid,
       name,
       address_uuid,
       employer,
       occupation
from (select * from $staging_form8872_entities_schedule_a
union
select distinct * from $staging_form8872_entities_schedule_b)  

