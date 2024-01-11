with tmp as (select distinct upper(reciepient_name)       as name,
                      upper(reciepient_employer)   as employer,
                      upper(recipient_occupation) as occupation,
                      upper(reciepient_address_1)  as reciepient_address_1,
                      upper(reciepient_address_2)  as reciepient_address_2,
                      upper(reciepient_address_city) as reciepient_address_city,
                      upper(reciepient_address_st) as reciepient_address_state,
                      reciepient_address_zip_code as reciepient_address_zip_code,
                      reciepient_address_zip_ext as reciepient_address_zip_ext
      from $landing_form8872_schedule_b)
    select name, address_uuid, employer, occupation
    from tmp
               left join $curated_addresses as cus_add
                         on ((cus_add.address_1 = (reciepient_address_1)) or
                             (cus_add.address_1 is null and (reciepient_address_1) is null)) and
                            ((cus_add.address_2 = (reciepient_address_2)) or
                             (cus_add.address_2 is null and (reciepient_address_2) is null)) and
                            ((cus_add.city = (reciepient_address_city)) or
                             (cus_add.city is null and (reciepient_address_city) is null)) and
                            ((cus_add.state = (reciepient_address_state)) or
                             (cus_add.state is null and (reciepient_address_state) is null)) and
                            ((cus_add.zip_code = reciepient_address_zip_code) or
                             (cus_add.zip_code is null and reciepient_address_zip_code is null)) and
                            ((cus_add.zip_ext = reciepient_address_zip_ext) or
                             (cus_add.zip_ext is null and reciepient_address_zip_ext is null))

