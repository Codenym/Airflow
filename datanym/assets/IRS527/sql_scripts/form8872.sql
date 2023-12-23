drop table if exists form8872;
CREATE TABLE form8872
    (
        form_id_number              varchar(7) primary key,
        period_begin_date           date,
        period_end_date             date,
        initial_report_indicator    boolean,
        amended_report_indicator    boolean,
        final_report_indicator      boolean,
        change_of_address_indicator boolean,
        organization_name           varchar(72),
        ein                         varchar(9),
        e_mail_address              varchar(72),
        org_formation_date          date,
        custodian_name              varchar(50),
        contact_person_name         varchar(50),
        qtr_indicator               int,
        monthly_rpt_month           int,
        pre_elect_type              varchar(10),
        pre_or_post_elect_date      date,
        pre_or_post_elect_state     varchar(2),
        sched_a_ind                 bool,
        total_sched_a               numeric,
        sched_b_ind                 bool,
        total_sched_b               numeric,
        mailing_address_id          integer,
        contact_address_id          integer,
        business_address_id         integer,
        custodian_address_id        integer,
        insert_datetime             timestamp
    );

insert into
    form8872
(form_id_number,
 period_begin_date,
 period_end_date,
 initial_report_indicator,
 amended_report_indicator,
 final_report_indicator,
 change_of_address_indicator,
 organization_name,
 ein,
 e_mail_address,
 org_formation_date,
 custodian_name,
 contact_person_name,
 qtr_indicator,
 monthly_rpt_month,
 pre_elect_type,
 pre_or_post_elect_date,
 pre_or_post_elect_state,
 mailing_address_id,
 contact_address_id,
 business_address_id,
 custodian_address_id,
 insert_datetime)

select
    form_id_number,
    to_date(period_begin_date,'YYYYMMDD'),
    to_date(period_end_date,'YYYYMMDD'),
    initial_report_indicator=1,
    amended_report_indicator=1,
    final_report_indicator=1,
    change_of_address_indicator=1,
    organization_name,
    ein,
    e_mail_address,
    to_date(org_formation_date,'YYYYMMDD'),
    custodian_name,
    contact_person_name,
    case when qtr_indicator = '' then null else qtr_indicator::int end,
    case when monthly_rpt_month = '' then null else monthly_rpt_month::int end,
    pre_elect_type,
    to_date(pre_or_post_elect_date,'YYYYMMDD'),
    pre_or_post_elect_state,
    mail_add.address_id as mailing_address_id,
    con_add.address_id  as contact_address_id,
    bus_add.address_id  as business_address_id,
    cus_add.address_id  as custodian_address_id,
    insert_datetime
from
    landing.form8872_landing
        left join addresses as mail_add on
            ((mail_add.address_1 = mailing_address_1) or (mail_add.address_1 is null and mailing_address_1 is null)) and
            ((mail_add.address_2 = mailing_address_2) or (mail_add.address_2 is null and mailing_address_2 is null)) and
            ((mail_add.city = mailing_address_city) or (mail_add.city is null and mailing_address_city is null)) and
            ((mail_add.state = mailing_address_state) or (mail_add.state is null and mailing_address_state is null)) and
            ((mail_add.zip_code = mailing_address_zip_code) or
             (mail_add.zip_code is null and mailing_address_zip_code is null)) and
            ((mail_add.zip_ext = mailing_address_zip_ext) or
             (mail_add.zip_ext is null and mailing_address_zip_ext is null))
        left join addresses as con_add on
            ((con_add.address_1 = contact_address_1) or (con_add.address_1 is null and contact_address_1 is null)) and
            ((con_add.address_2 = contact_address_2) or (con_add.address_2 is null and contact_address_2 is null)) and
            ((con_add.city = contact_address_city) or (con_add.city is null and contact_address_city is null)) and
            ((con_add.state = contact_address_state) or (con_add.state is null and contact_address_state is null)) and
            ((con_add.zip_code = contact_address_zip_code) or
             (con_add.zip_code is null and contact_address_zip_code is null)) and
            ((con_add.zip_ext = contact_address_zip_ext) or
             (con_add.zip_ext is null and contact_address_zip_ext is null))
        left join addresses as bus_add on
            ((bus_add.address_1 = business_address_1) or (bus_add.address_1 is null and business_address_1 is null)) and
            ((bus_add.address_2 = business_address_2) or (bus_add.address_2 is null and business_address_2 is null)) and
            ((bus_add.city = business_address_city) or (bus_add.city is null and business_address_city is null)) and
            ((bus_add.state = business_address_state) or (bus_add.state is null and business_address_state is null)) and
            ((bus_add.zip_code = business_address_zip_code) or
             (bus_add.zip_code is null and business_address_zip_code is null)) and
            ((bus_add.zip_ext = business_address_zip_ext) or
             (bus_add.zip_ext is null and business_address_zip_ext is null))
        left join addresses as cus_add on
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
             (cus_add.zip_ext is null and custodian_address_zip_ext is null))
;
