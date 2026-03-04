create or replace view election_db.v_map_winner_party AS
with agg_province_vote as (
select 
	prov_id,
	party_id,
	SUM(party_list_vote) as party_vote_province
from fact_vote_party
group by prov_id, party_id
),
calculate_rank as (
select 
	*,
	RANK() over (partition by prov_id order by party_vote_province desc) as party_rank
from agg_province_vote
)
select 
	dp.province,
	dpt.name,
	dpt.color,
	cr.party_vote_province
from calculate_rank cr
left join dim_province dp on cr.prov_id = dp.prov_id
left join dim_party dpt on cr.party_id = dpt.party_id
where party_rank = 1;