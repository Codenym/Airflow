select trim(form_id_number)                                 as form_id_number,
       trim(entity_id)                                      as entity_id,
       upper(replace(trim(org_name), '  ', ' '))            as organization_name,
       trim(ein)                                            as ein,
       upper(replace(trim(entity_name), '  ', ' '))         as entity_name,
       upper(replace(trim(entity_relationship), '  ', ' ')) as entity_relationship,
       upper(replace(trim(entity_address_1), '  ', ' '))    as entity_address_1,
       upper(replace(trim(entity_address_2), '  ', ' '))    as entity_address_2,
       upper(replace(trim(entity_address_city), '  ', ' ')) as entity_address_city,
       upper(replace(trim(entity_address_st), '  ', ' '))   as entity_address_state,
       trim(entity_address_zip_code)                        as entity_address_zip_code,
       trim(entity_address_zip_ext)                         as entity_address_zip_code_ext
from $landing_form8871_related_entities