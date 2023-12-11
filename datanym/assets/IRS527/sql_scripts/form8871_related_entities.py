drop_related = 'drop table if exists {form8871_related_entities};'
ddl_related = '''
create table {form8871_related_entities}
    (
        form_id_number      TEXT,
        entity_id           TEXT,
        org_name            TEXT,
        ein                 TEXT,
        entity_name         TEXT,
        entity_relationship TEXT,
        entity_address_id   TEXT
    );
'''

data_related = '''
insert into {form8871_related_entities} (form_id_number, entity_id, org_name, ein, entity_name, entity_relationship, entity_address_id)
select
    form_id_number,
    entity_id,
    org_name,
    ein,
    entity_name,
    entity_relationship,
    ent_add.address_id as entity_address_id
from
    {form8871_related_entities_landing}
        left join {addresses} as ent_add on
            ((ent_add.address_1 = entity_address_1) or
             (ent_add.address_1 is null and entity_address_1 is null)) and
            ((ent_add.address_2 = entity_address_2) or
             (ent_add.address_2 is null and entity_address_2 is null)) and
            ((ent_add.city = entity_address_city) or (ent_add.city is null and entity_address_city is null)) and
            ((ent_add.state = entity_address_st) or
             (ent_add.state is null and entity_address_st is null)) and
            ((ent_add.zip_code = entity_address_zip_code) or
             (ent_add.zip_code is null and entity_address_zip_code is null)) and
            ((ent_add.zip_ext = entity_address_zip_ext) or
             (ent_add.zip_ext is null and entity_address_zip_ext is null));
'''
dagster_run_queries = [drop_related, ddl_related, data_related]