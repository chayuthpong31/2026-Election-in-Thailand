CREATE OR REPLACE VIEW election_db.v_heatmap_province_party AS
SELECT 
    dp.province,
    dpt.name,
    SUM(fvp.party_list_vote) * 100.0 / SUM(SUM(fvp.party_list_vote)) OVER (PARTITION BY dp.province) as vote_percent
FROM election_db.fact_vote_party fvp
JOIN election_db.dim_province dp ON fvp.prov_id = dp.prov_id
JOIN election_db.dim_party dpt ON fvp.party_id = dpt.party_id
GROUP BY dp.province, dpt.name;