create view top_party_constituency as 
select 
	dp.name,
	dp.color,
	sum(party_list_vote) as total_vote
from fact_vote_party fvp 
left join dim_party dp on fvp.party_id = dp.party_id
group by dp.name, dp.color
order by total_vote desc
limit 1;