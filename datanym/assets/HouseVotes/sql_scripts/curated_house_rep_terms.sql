with house_rep_terms as(select     
	hrp.record_type,
    hrp.start_date,
    hrp.end_date,
    hrp.state,
    hrp.district,
    hrp.party,
    hrp.bioguide,
    hs.session
    from $staging_house_rep_terms  as  hrp
left join $curated_house_sessions as hs on (hrp.start_date >= hs.start_date) and (hrp.start_date < hs.end_date)
) select * from house_rep_terms
where year(start_date) >= 1900 
