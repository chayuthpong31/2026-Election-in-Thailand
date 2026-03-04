CREATE OR REPLACE VIEW election_db.v_scatter_competition AS
SELECT 
    fvc.cons_id,
    dp.province,
    COUNT(fvc.party_id) as candidate_count,
    SUM(fvc.mp_candidate_vote) as total_votes,
    MAX(fvc.mp_candidate_vote) as winner_votes,
    MAX(fvc.mp_candidate_vote) - (
        SELECT mp_candidate_vote FROM election_db.fact_vote_constituency f2 
        WHERE f2.cons_id = fvc.cons_id 
        ORDER BY mp_candidate_vote DESC LIMIT 1 OFFSET 1
    ) as margin_of_victory
FROM election_db.fact_vote_constituency fvc
JOIN election_db.dim_province dp ON fvc.prov_id = dp.prov_id
GROUP BY fvc.cons_id, dp.province;