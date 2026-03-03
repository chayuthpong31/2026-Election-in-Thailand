-- Which party won the highest number of constituencies?

with party_rank as (
select 
	cons_id, 
	party_id, 
	party_list_vote,
	RANK() over (partition by cons_id order by party_list_vote DESC) as rank_vote 
from fact_vote_party fvp
),
winner_flag as (
select 
	* ,
	case 
		when rank_vote = 1 then 1 
		else 0
	end winner
from party_rank
),
aggregate_wins as (
	select 
	party_id, 
	SUM(winner) as total_wins 
from winner_flag
group by party_id
)
select 
	dp.party_no, 
	dp.name, 
	aw.total_wins 
from aggregate_wins aw
left join dim_party dp on aw.party_id = dp.party_id 
order by total_wins DESC;

