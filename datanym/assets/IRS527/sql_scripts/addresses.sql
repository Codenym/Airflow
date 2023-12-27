select
    uuid() as address_id,
    *
from
    (select distinct *
     from
         (select
              mailing_address_1        as address_1,
              mailing_address_2        as address_2,
              mailing_address_city     as city,
              mailing_address_state    as state,
              mailing_address_zip_code as zip_code,
              mailing_address_zip_ext  as zip_ext
          from
              $form8871_landing
          union all
          select
              custodian_address_1        as address_1,
              custodian_address_2        as address_2,
              custodian_address_city     as city,
              custodian_address_state    as state,
              custodian_address_zip_code as zip_code,
              custodian_address_zip_ext  as zip_ext
          from
              $form8871_landing
          union all
          select
              contact_address_1        as address_1,
              contact_address_2        as address_2,
              contact_address_city     as city,
              contact_address_state    as state,
              contact_address_zip_code as zip_code,
              contact_address_zip_ext  as zip_ext
          from
              $form8871_landing
          union all
          select
              business_address_1        as address_1,
              business_address_2        as address_2,
              business_address_city     as city,
              business_address_state    as state,
              business_address_zip_code as zip_code,
              business_address_zip_ext  as zip_ext
          from
              $form8871_landing
          union all
          select
              entity_address_1            as address_1,
              entity_address_2            as address_2,
              entity_address_city         as city,
              entity_address_st           as state,
              entity_address_zip_code     as zip_code,
              entity_address_zip_code_ext as zip_ext
          from
              $form8871_directors_landing
          union all
          select
              entity_address_1            as address_1,
              entity_address_2            as address_2,
              entity_address_city         as city,
              entity_address_st           as state,
              entity_address_zip_code     as zip_code,
              entity_address_zip_ext as zip_ext
          from
              $form8871_related_entities_landing
          union all
          select
              mailing_address_1        as address_1,
              mailing_address_2        as address_2,
              mailing_address_city     as city,
              mailing_address_state    as state,
              mailing_address_zip_code as zip_code,
              mailing_address_zip_ext  as zip_ext
          from
              $form8872_landing
          union all
          select
              contact_address_1        as address_1,
              contact_address_2        as address_2,
              contact_address_city     as city,
              contact_address_state    as state,
              contact_address_zip_code as zip_code,
              contact_address_zip_ext  as zip_ext
          from
              $form8872_landing
          union all
          select
              business_address_1        as address_1,
              business_address_2        as address_2,
              business_address_city     as city,
              business_address_state    as state,
              business_address_zip_code as zip_code,
              business_address_zip_ext  as zip_ext
          from
              $form8872_landing
          union all
          select
              custodian_address_1        as address_1,
              custodian_address_2        as address_2,
              custodian_address_city     as city,
              custodian_address_state    as state,
              custodian_address_zip_code as zip_code,
              custodian_address_zip_ext  as zip_ext
          from
              $form8872_landing) t) t2