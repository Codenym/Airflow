select
    uuid() as address_id,
    *
from
    (select distinct
         upper(address_1) as address_1,
         upper(address_2) as address_2,
         upper(city)      as city,
         upper(state)     as state,
         zip_code         as zip_code,
         zip_ext          as zip_ext
     from
         (select
              mailing_address_1        as address_1,
              mailing_address_2        as address_2,
              mailing_address_city     as city,
              mailing_address_state    as state,
              mailing_address_zip_code as zip_code,
              mailing_address_zip_ext  as zip_ext
          from
              $landing_form8871
          union
          select
              custodian_address_1        as address_1,
              custodian_address_2        as address_2,
              custodian_address_city     as city,
              custodian_address_state    as state,
              custodian_address_zip_code as zip_code,
              custodian_address_zip_ext  as zip_ext
          from
              $landing_form8871
          union
          select
              contact_address_1        as address_1,
              contact_address_2        as address_2,
              contact_address_city     as city,
              contact_address_state    as state,
              contact_address_zip_code as zip_code,
              contact_address_zip_ext  as zip_ext
          from
              $landing_form8871
          union
          select
              business_address_1        as address_1,
              business_address_2        as address_2,
              business_address_city     as city,
              business_address_state    as state,
              business_address_zip_code as zip_code,
              business_address_zip_ext  as zip_ext
          from
              $landing_form8871
          union
          select
              entity_address_1            as address_1,
              entity_address_2            as address_2,
              entity_address_city         as city,
              entity_address_st           as state,
              entity_address_zip_code     as zip_code,
              entity_address_zip_code_ext as zip_ext
          from
              $landing_form8871_directors
          union
          select
              entity_address_1        as address_1,
              entity_address_2        as address_2,
              entity_address_city     as city,
              entity_address_st       as state,
              entity_address_zip_code as zip_code,
              entity_address_zip_ext  as zip_ext
          from
              $landing_form8871_related_entities
          union
          select
              mailing_address_1        as address_1,
              mailing_address_2        as address_2,
              mailing_address_city     as city,
              mailing_address_state    as state,
              mailing_address_zip_code as zip_code,
              mailing_address_zip_ext  as zip_ext
          from
              $landing_form8872
          union
          select
              contact_address_1        as address_1,
              contact_address_2        as address_2,
              contact_address_city     as city,
              contact_address_state    as state,
              contact_address_zip_code as zip_code,
              contact_address_zip_ext  as zip_ext
          from
              $landing_form8872
          union
          select
              business_address_1        as address_1,
              business_address_2        as address_2,
              business_address_city     as city,
              business_address_state    as state,
              business_address_zip_code as zip_code,
              business_address_zip_ext  as zip_ext
          from
              $landing_form8872
          union
          select
              custodian_address_1        as address_1,
              custodian_address_2        as address_2,
              custodian_address_city     as city,
              custodian_address_state    as state,
              custodian_address_zip_code as zip_code,
              custodian_address_zip_ext  as zip_ext
          from
              $landing_form8872
          union
          select
              contributor_address_1        as address_1,
              contributor_address_2        as address_2,
              contributor_address_city     as city,
              contributor_address_state    as state,
              contributor_address_zip_code as zip_code,
              contributor_address_zip_ext  as zip_ext
          from
              $landing_form8872_schedule_a
          union
          select
              reciepient_address_1        as address_1,
              reciepient_address_2        as address_2,
              reciepient_address_city     as city,
              reciepient_address_st       as state,
              reciepient_address_zip_code as zip_code,
              reciepient_address_zip_ext  as zip_ext
          from
              $landing_form8872_schedule_b) t) t2