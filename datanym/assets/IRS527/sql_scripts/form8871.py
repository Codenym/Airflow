drop_8871 = 'drop table if exists {form8871};'
ddl_8871 = '''
create table {form8871}
    (
        record_type              text,
        form_type                text,
        form_id_number           text primary key,
        initial_report_indicator boolean,
        amended_report_indicator boolean,
        final_report_indicator   boolean,
        ein                      text,
        organization_name        text,
        e_mail_address           text,
        established_date         date,
        custodian_name           text,
        contact_person_name      text,
        exempt_8872_indicator    boolean,
        exempt_state             text,
        exempt_990_indicator     boolean,
        purpose                  text,
        material_change_date     date,
        insert_datetime          timestamp,
        related_entity_bypass    text,
        eain_bypass              text,
        mailing_address_id       text,
        custodian_address_id     text,
        contact_address_id       text,
        business_address_id      text

    );
'''
data_8871 = '''
insert into
    {form8871} (record_type,
              form_type,
              form_id_number,
              initial_report_indicator,
              amended_report_indicator,
              final_report_indicator,
              ein,
              organization_name,
              e_mail_address,
              established_date,
              custodian_name,
              contact_person_name,
              exempt_8872_indicator,
              exempt_state,
              exempt_990_indicator,
              purpose,
              material_change_date,
              insert_datetime,
              related_entity_bypass,
              eain_bypass,
              mailing_address_id,
              contact_address_id,
              business_address_id,
              custodian_address_id)
select
    record_type,
    form_type,
    form_id_number,
    initial_report_indicator,
    amended_report_indicator,
    final_report_indicator,
    ein,
    organization_name,
    e_mail_address,
    established_date,
    custodian_name,
    contact_person_name,
    exempt_8872_indicator,
    exempt_state,
    exempt_990_indicator,
    purpose,
    material_change_date,
    insert_datetime,
    related_entity_bypass,
    eain_bypass,
    mail_add.address_id as mailing_address_id,
    con_add.address_id  as contact_address_id,
    bus_add.address_id  as business_address_id,
    cus_add.address_id  as custodian_address_id
from
    {form8871_landing}
        left join {addresses} as mail_add on
            ((mail_add.address_1 = mailing_address_1) or (mail_add.address_1 is null and mailing_address_1 is null)) and
            ((mail_add.address_2 = mailing_address_2) or (mail_add.address_2 is null and mailing_address_2 is null)) and
            ((mail_add.city = mailing_address_city) or (mail_add.city is null and mailing_address_city is null)) and
            ((mail_add.state = mailing_address_state) or (mail_add.state is null and mailing_address_state is null)) and
            ((mail_add.zip_code = mailing_address_zip_code) or
             (mail_add.zip_code is null and mailing_address_zip_code is null)) and
            ((mail_add.zip_ext = mailing_address_zip_ext) or
             (mail_add.zip_ext is null and mailing_address_zip_ext is null))
        left join {addresses} as con_add on
            ((con_add.address_1 = contact_address_1) or (con_add.address_1 is null and contact_address_1 is null)) and
            ((con_add.address_2 = contact_address_2) or (con_add.address_2 is null and contact_address_2 is null)) and
            ((con_add.city = contact_address_city) or (con_add.city is null and contact_address_city is null)) and
            ((con_add.state = contact_address_state) or (con_add.state is null and contact_address_state is null)) and
            ((con_add.zip_code = contact_address_zip_code) or
             (con_add.zip_code is null and contact_address_zip_code is null)) and
            ((con_add.zip_ext = contact_address_zip_ext) or
             (con_add.zip_ext is null and contact_address_zip_ext is null))
        left join {addresses} as bus_add on
            ((bus_add.address_1 = business_address_1) or (bus_add.address_1 is null and business_address_1 is null)) and
            ((bus_add.address_2 = business_address_2) or (bus_add.address_2 is null and business_address_2 is null)) and
            ((bus_add.city = business_address_city) or (bus_add.city is null and business_address_city is null)) and
            ((bus_add.state = business_address_state) or (bus_add.state is null and business_address_state is null)) and
            ((bus_add.zip_code = business_address_zip_code) or
             (bus_add.zip_code is null and business_address_zip_code is null)) and
            ((bus_add.zip_ext = business_address_zip_ext) or
             (bus_add.zip_ext is null and business_address_zip_ext is null))
        left join {addresses} as cus_add on
            ((cus_add.address_1 = custodian_address_1) or
             (cus_add.address_1 is null and custodian_address_1 is null)) and
            ((cus_add.address_2 = custodian_address_2) or
             (cus_add.address_2 is null and custodian_address_2 is null)) and
            ((cus_add.city = custodian_address_city) or (cus_add.city is null and custodian_address_city is null)) and
            ((cus_add.state = custodian_address_state) or
             (cus_add.state is null and custodian_address_state is null)) and
            ((cus_add.zip_code = custodian_address_zip_code) or
             (cus_add.zip_code is null and custodian_address_zip_code is null)) and
            ((cus_add.zip_ext = custodian_address_zip_ext) or
             (cus_add.zip_ext is null and custodian_address_zip_ext is null));
'''

dagster_run_queries = [drop_8871, ddl_8871, data_8871]