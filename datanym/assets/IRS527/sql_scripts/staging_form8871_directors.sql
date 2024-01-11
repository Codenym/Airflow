select trim(form_id_number)                                 as form_id_number,
       trim(director_id)                                    as director_id,
       upper(replace(trim(org_name), '  ', ' '))            as org_name,
       trim(ein)                                            as ein,
       upper(replace(trim(entity_name), '  ', ' '))         as entity_name,
       upper(replace(trim(entity_title), '  ', ' '))        as entity_title,
       upper(replace(trim(entity_address_1), '  ', ' '))    as entity_address_1,
       upper(replace(trim(entity_address_2), '  ', ' '))    as entity_address_2,
       upper(replace(trim(entity_address_city), '  ', ' ')) as entity_address_city,
       upper(replace(trim(entity_address_st), '  ', ' '))   as entity_address_st,
       trim(entity_address_zip_code)                        as entity_address_zip_code,
       trim(entity_address_zip_code_ext)                    as entity_address_zip_code_ext
from $landing_form8871_directors as base