select
    trim(form_id_number) as form_id_number,
    trim(director_id) as director_id,
    upper(trim(org_name)) as org_name,
    trim(ein) as ein,
    upper(trim(entity_name)) as entity_name,
    upper(trim(entity_title)) as entity_title,
    upper(trim(entity_address_1)) as entity_address_1,
    upper(trim(entity_address_2)) as entity_address_2,
    upper(trim(entity_address_city)) as entity_address_city,
    upper(trim(entity_address_st)) as entity_address_st,
    trim(entity_address_zip_code) as entity_address_zip_code,
    trim(entity_address_zip_code_ext) as entity_address_zip_code_ext
from
    $landing_form8871_directors as base