select
    form_id_number,
    entity_id,
    ein,
    organization_name,
    entity_name, -- ein -> entity table?
    entity_relationship,
    ent_add.id as entity_address_id
from
    $staging_form8871_related_entities_1cast base
        left join $curated_addresses as ent_add on
            ((ent_add.address_1 = entity_address_1) or
             (ent_add.address_1 is null and entity_address_1 is null)) and
            ((ent_add.address_2 = entity_address_2) or
             (ent_add.address_2 is null and entity_address_2 is null)) and
            ((ent_add.city = entity_address_city) or (ent_add.city is null and entity_address_city is null)) and
            ((ent_add.state = entity_address_state) or
             (ent_add.state is null and entity_address_state is null)) and
            ((ent_add.zip_code = entity_address_zip_code) or
             (ent_add.zip_code is null and entity_address_zip_code is null)) and
            ((ent_add.zip_ext = entity_address_zip_code_ext) or
             (ent_add.zip_ext is null and entity_address_zip_code_ext is null))