-- Comparative Analysis: Party List votes versus Constituency votes.
select 
	fvc.prov_id ,
	fvc.cons_id ,
	fvc.party_id ,
	fvc.mp_app_vote_percent ,
	(fvp.party_list_vote * 100.0 / SUM(fvp.party_list_vote) OVER (PARTITION BY fvc.cons_id)) as party_vote_percent,
	ABS(fvc.mp_app_vote_percent - (fvp.party_list_vote * 100.0 / SUM(fvp.party_list_vote) OVER (PARTITION BY fvc.cons_id))) AS vote_gap
from fact_vote_constituency fvc 
join fact_vote_party fvp 
on fvc.prov_id = fvp.prov_id and fvc.cons_id = fvp.cons_id and fvc.party_id = fvp.party_id
order by vote_gap desc;