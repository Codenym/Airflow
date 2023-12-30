select uuid() as form8872_entity_uuid,
       name,
       address_uuid,
       employer,
       occupation
from (select distinct upper(contributor_name)       as name,
                      address_uuid                  as address_uuid,
                      upper(contributor_employer)   as employer,
                      upper(contributor_occupation) as occupation
      from $landing_form8872_schedule_a
               left join $curated_addresses as cus_add
                         on ((cus_add.address_1 = contributor_address_1) or
                             (cus_add.address_1 is null and contributor_address_1 is null)) and
                            ((cus_add.address_2 = contributor_address_2) or
                             (cus_add.address_2 is null and contributor_address_2 is null)) and
                            ((cus_add.city = contributor_address_city) or
                             (cus_add.city is null and contributor_address_city is null)) and
                            ((cus_add.state = contributor_address_state) or
                             (cus_add.state is null and contributor_address_state is null)) and
                            ((cus_add.zip_code = contributor_address_zip_code) or
                             (cus_add.zip_code is null and contributor_address_zip_code is null)) and
                            ((cus_add.zip_ext = contributor_address_zip_ext) or
                             (cus_add.zip_ext is null and contributor_address_zip_ext is null))

      union
      select distinct upper(reciepient_name)      as name,
                      address_uuid                as address_id,
                      upper(reciepient_employer)  as employer,
                      upper(recipient_occupation) as occupation
      from $landing_form8872_schedule_b
               left join $curated_addresses as cus_add
                         on ((cus_add.address_1 = reciepient_address_1) or
                             (cus_add.address_1 is null and reciepient_address_1 is null)) and
                            ((cus_add.address_2 = reciepient_address_2) or
                             (cus_add.address_2 is null and reciepient_address_2 is null)) and
                            ((cus_add.city = reciepient_address_city) or
                             (cus_add.city is null and reciepient_address_city is null)) and
                            ((cus_add.state = reciepient_address_st) or
                             (cus_add.state is null and reciepient_address_st is null)) and
                            ((cus_add.zip_code = reciepient_address_zip_code) or
                             (cus_add.zip_code is null and reciepient_address_zip_code is null)) and
                            ((cus_add.zip_ext = reciepient_address_zip_ext) or
                             (cus_add.zip_ext is null and reciepient_address_zip_ext is null)))