-- In which constituency did the winner secure a seat with the lowest absolute number of votes?
select 
    fvc.prov_id, 
    dc.cons_no, 
    fvc.mp_candidate_vote, 
    fvc.mp_app_vote_percent 
from fact_vote_constituency fvc
left join dim_constituency dc on fvc.cons_id = dc.cons_id 
where fvc.mp_app_rank = 1
order by mp_candidate_vote;