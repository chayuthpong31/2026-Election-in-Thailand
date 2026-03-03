-- Which parties received high total popular votes but won a disproportionately low number of seats?
with calculate_cons_winner_flag as (
select 
*,  
case 
	when mp_app_rank = 1 then 1
	else 0
end as winner_flag
from fact_vote_constituency
),
agg_cons_party as (
	select 
	party_id,
	SUM(winner_flag) as total_cons_wins
from calculate_cons_winner_flag
group by party_id
),
calculate_party_total_vote as (
select 
	party_id,
	SUM(party_list_vote) as total_party_vote
from fact_vote_party 
group by party_id
),
join_cte as (
select 
	cp.*,
	pt.total_party_vote 
from agg_cons_party cp 
join calculate_party_total_vote pt on cp.party_id = pt.party_id 
)
select 
	dp.name,
	j.total_party_vote,
	j.total_cons_wins,
	(j.total_party_vote / NULLIF(COALESCE(j.total_cons_wins, 0), 0)) as votes_per_seat
from join_cte j 
left join dim_party dp on j.party_id = dp.party_id 
order by votes_per_seat DESC NULLS LAST;