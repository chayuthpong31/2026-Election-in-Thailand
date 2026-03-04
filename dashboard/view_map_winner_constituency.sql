create view election_db.v_map_winner_constituency AS
with calculate_winner_flag as (
select  
	*,
	case
		when mp_app_rank = 1 then 1
		else 0
	end winner_flag
from fact_vote_constituency 
),
agg as (
select 
	prov_id,
	party_id,
	sum(winner_flag) as total_seat,
	sum(mp_candidate_vote) as total_vote
from calculate_winner_flag
group by prov_id,party_id
),
calculate_cons_prov as (
select
	prov_id,
	count(distinct cons_id) as total_prov_cons,
	sum(mp_candidate_vote) as total_province_vote
from fact_vote_constituency
group by prov_id
),
join_prov as (
select 
	a.*,
	cp.total_prov_cons,
	RANK() over (partition by a.prov_id order by a.total_seat desc, a.total_vote desc) as party_rank
from agg a 
left join calculate_cons_prov cp on a.prov_id = cp.prov_id 
)
select 
	dp.province,
	dpt.name,
	dpt.color,
	jp.total_seat
from join_prov jp
left join dim_province dp on jp.prov_id = dp.prov_id 
left join dim_party dpt on jp.party_id = dpt.party_id 
where party_rank = 1;