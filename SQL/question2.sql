/* Which province is the most competitive? 
(Determined by the narrowest Margin of Victory between the 1st and 2nd ranks) */

with province_vote as (
select 
	prov_id , 
	party_id, 
	SUM(party_list_vote) as total_vote
from fact_vote_party
group by prov_id, party_id
),
province_rank_vote as (
select 
	*,
	RANK() over (partition by prov_id order by total_vote DESC) as rank_vote
from province_vote
),
calculate_second_rank as (
select 
	*,
	LEAD(total_vote) over (partition by prov_id order by rank_vote) as second_rank_vote
from province_rank_vote
)
select 
	dp.province,
	csr.total_vote - csr.second_rank_vote as vote_diff
from calculate_second_rank csr
left join dim_province dp on csr.prov_id = dp.prov_id
where rank_vote = 1
order by vote_diff ;
