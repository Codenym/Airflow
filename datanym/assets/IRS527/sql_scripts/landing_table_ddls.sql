drop table if exists landing.form8871_directors_landing;
create table landing.form8871_directors_landing
(
record_type varchar(1),
form_id_number varchar(9),
director_id varchar(9),
org_name varchar(200),
ein varchar(9),
entity_name varchar(200),
entity_title varchar(200),
entity_address_1 varchar(200),
entity_address_2 varchar(200),
entity_address_city varchar(200),
entity_address_st varchar(2),
entity_address_zip_code varchar(5),
entity_address_zip_code_ext varchar(4)
);

drop table if exists landing.form8871_ein_landing;
create table landing.form8871_ein_landing
(
record_type varchar(1),
form_id_number varchar(9),
eain_id varchar(9),
election_authority_id_number varchar(50),
state_issued varchar(2)
);
drop table if exists landing.form8871_related_entities_landing;
create table landing.form8871_related_entities_landing
(
record_type varchar(1),
form_id_number varchar(9),
entity_id varchar(9),
org_name varchar(200),
ein varchar(9),
entity_name varchar(200),
entity_relationship varchar(200),
entity_address_1 varchar(200),
entity_address_2 varchar(200),
entity_address_city varchar(200),
entity_address_st varchar(2),
entity_address_zip_code varchar(5),
entity_address_zip_code_ext varchar(4)
);
drop table if exists landing.form8871_landing;
create table landing.form8871_landing
(
record_type varchar(1),
form_type varchar(4),
form_id_number varchar(9),
initial_report_indicator varchar(1),
amended_report_indicator varchar(1),
final_report_indicator varchar(1),
ein varchar(9),
organization_name varchar(200),
mailing_address_1 varchar(200),
mailing_address_2 varchar(200),
mailing_address_city varchar(200),
mailing_address_state varchar(2),
mailing_address_zip_code varchar(5),
mailing_address_zip_ext varchar(4),
e_mail_address varchar(200),
established_date varchar(8),
custodian_name varchar(200),
custodian_address_1 varchar(200),
custodian_address_2 varchar(200),
custodian_address_city varchar(200),
custodian_address_state varchar(2),
custodian_address_zip_code varchar(5),
custodian_address_zip_ext varchar(4),
contact_person_name varchar(200),
contact_address_1 varchar(200),
contact_address_2 varchar(200),
contact_address_city varchar(200),
contact_address_state varchar(2),
contact_address_zip_code varchar(5),
contact_address_zip_ext varchar(4),
business_address_1 varchar(200),
business_address_2 varchar(200),
business_address_city varchar(200),
business_address_state varchar(2),
business_address_zip_code varchar(5),
business_address_zip_ext varchar(4),
exempt_8872_indicator varchar(1),
exempt_state varchar(2),
exempt_990_indicator varchar(1),
purpose varchar(2000),
material_change_date varchar(8),
insert_datetime timestamp,
related_entity_bypass varchar(1),
eain_bypass varchar(1)
);
drop table if exists landing.form8872_landing;
create table landing.form8872_landing
(
record_type varchar(1),
form_type varchar(4),
form_id_number varchar(9),
period_begin_date varchar(8),
period_end_date varchar(8),
initial_report_indicator varchar(1),
amended_report_indicator varchar(1),
final_report_indicator varchar(1),
change_of_address_indicator varchar(1),
organization_name varchar(200),
ein varchar(9),
mailing_address_1 varchar(200),
mailing_address_2 varchar(200),
mailing_address_city varchar(200),
mailing_address_state varchar(2),
mailing_address_zip_code varchar(5),
mailing_address_zip_ext varchar(4),
e_mail_address varchar(200),
org_formation_date varchar(8),
custodian_name varchar(200),
custodian_address_1 varchar(200),
custodian_address_2 varchar(200),
custodian_address_city varchar(200),
custodian_address_state varchar(2),
custodian_address_zip_code varchar(5),
custodian_address_zip_ext varchar(4),
contact_person_name varchar(200),
contact_address_1 varchar(200),
contact_address_2 varchar(200),
contact_address_city varchar(200),
contact_address_state varchar(2),
contact_address_zip_code varchar(5),
contact_address_zip_ext varchar(4),
business_address_1 varchar(200),
business_address_2 varchar(200),
business_address_city varchar(200),
business_address_state varchar(2),
business_address_zip_code varchar(5),
business_address_zip_ext varchar(4),
qtr_indicator varchar(1),
monthly_rpt_month varchar(2),
pre_elect_type varchar(10),
pre_or_post_elect_date varchar(8),
pre_or_post_elect_state varchar(2),
sched_a_ind varchar(1),
total_sched_a varchar(10),
sched_b_ind varchar(1),
total_sched_b varchar(10),
insert_datetime timestamp
);
drop table if exists landing.form8872_schedule_a_landing;
create table landing.form8872_schedule_a_landing
(
record_type varchar(1),
form_id_number varchar(9),
sched_a_id varchar(9),
org_name varchar(200),
ein varchar(9),
contributor_name varchar(200),
contributor_address_1 varchar(200),
contributor_address_2 varchar(200),
contributor_address_city varchar(200),
contributor_address_state varchar(2),
contributor_address_zip_code varchar(5),
contributor_address_zip_ext varchar(4),
contributor_employer varchar(200),
contribution_amount varchar(10),
contributor_occupation varchar(200),
agg_contribution_ytd varchar(10),
contribution_date varchar(8)
);
drop table if exists landing.form8872_schedule_b_landing;
create table landing.form8872_schedule_b_landing
(
record_type varchar(1),
form_id_number varchar(9),
sched_b_id varchar(9),
org_name varchar(200),
ein varchar(9),
reciepient_name varchar(200),
reciepient_address_1 varchar(200),
reciepient_address_2 varchar(200),
reciepient_address_city varchar(200),
reciepient_address_st varchar(2),
reciepient_address_zip_code varchar(5),
reciepient_address_zip_ext varchar(4),
reciepient_employer varchar(200),
expenditure_amount varchar(10),
recipient_occupation varchar(200),
expenditure_date varchar(8),
expenditure_purpose varchar(2000)
);

