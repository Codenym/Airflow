with addresses as
         (select distinct mailing_address_1        as address_1,
                          mailing_address_2        as address_2,
                          mailing_address_city     as city,
                          mailing_address_state    as state,
                          mailing_address_zip_code as zip_code,
                          mailing_address_zip_ext  as zip_ext
          from $staging_form8871_a
          union
          select distinct custodian_address_1        as address_1,
                          custodian_address_2        as address_2,
                          custodian_address_city     as city,
                          custodian_address_state    as state,
                          custodian_address_zip_code as zip_code,
                          custodian_address_zip_ext  as zip_ext
          from $staging_form8871_a
          union
          select distinct contact_address_1        as address_1,
                          contact_address_2        as address_2,
                          contact_address_city     as city,
                          contact_address_state    as state,
                          contact_address_zip_code as zip_code,
                          contact_address_zip_ext  as zip_ext
          from $staging_form8871_a
          union
          select distinct business_address_1        as address_1,
                          business_address_2        as address_2,
                          business_address_city     as city,
                          business_address_state    as state,
                          business_address_zip_code as zip_code,
                          business_address_zip_ext  as zip_ext
          from $staging_form8871_a
          union
          select distinct entity_address_1            as address_1,
                          entity_address_2            as address_2,
                          entity_address_city         as city,
                          entity_address_st           as state,
                          entity_address_zip_code     as zip_code,
                          entity_address_zip_code_ext as zip_ext
          from $staging_form8871_directors
          union
          select distinct entity_address_1            as address_1,
                          entity_address_2            as address_2,
                          entity_address_city         as city,
                          entity_address_state        as state,
                          entity_address_zip_code     as zip_code,
                          entity_address_zip_code_ext as zip_ext
          from $staging_form8871_related_entities
          union
          select distinct mailing_address_1        as address_1,
                          mailing_address_2        as address_2,
                          mailing_address_city     as city,
                          mailing_address_state    as state,
                          mailing_address_zip_code as zip_code,
                          mailing_address_zip_ext  as zip_ext
          from $staging_form8872_a
          union
          select distinct contact_address_1        as address_1,
                          contact_address_2        as address_2,
                          contact_address_city     as city,
                          contact_address_state    as state,
                          contact_address_zip_code as zip_code,
                          contact_address_zip_ext  as zip_ext
          from $staging_form8872_a
          union
          select distinct business_address_1        as address_1,
                          business_address_2        as address_2,
                          business_address_city     as city,
                          business_address_state    as state,
                          business_address_zip_code as zip_code,
                          business_address_zip_ext  as zip_ext
          from $staging_form8872_a
          union
          select distinct custodian_address_1        as address_1,
                          custodian_address_2        as address_2,
                          custodian_address_city     as city,
                          custodian_address_state    as state,
                          custodian_address_zip_code as zip_code,
                          custodian_address_zip_ext  as zip_ext
          from $staging_form8872_a
          union
          select distinct contributor_address_1        as address_1,
                          contributor_address_2        as address_2,
                          contributor_address_city     as city,
                          contributor_address_state    as state,
                          contributor_address_zip_code as zip_code,
                          contributor_address_zip_ext  as zip_ext
          from $staging_form8872_schedule_a
          union
          select distinct recipient_address_1        as address_1,
                          recipient_address_2        as address_2,
                          recipient_address_city     as city,
                          recipient_address_state    as state,
                          recipient_address_zip_code as zip_code,
                          recipient_address_zip_ext  as zip_ext
          from $staging_form8872_schedule_b)

select uuid() as id,
       address_1,
       address_2,
       city,
       state,
       zip_code,
       zip_ext,
from addresses
