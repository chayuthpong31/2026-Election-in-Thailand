create view election_db.v_kpi_top_party_seat as 
with calculate_flag as (
select 
*,
case
	when mp_app_rank = 1 then 1
	else 0
end as winner_flag
from fact_vote_constituency),
constituency_seat as (
select 
	party_id, 
	SUM(winner_flag) as constituency_seats
from calculate_flag 
group by party_id
),
total_votes AS (
    SELECT SUM(party_list_vote) AS grand_total FROM fact_vote_party
),
calc_shares AS (
    SELECT 
        party_id, 
        SUM(party_list_vote) AS party_total,
        (SUM(party_list_vote) * 100.0) / (SELECT grand_total FROM total_votes) AS raw_seats
    FROM fact_vote_party
    GROUP BY party_id
),
party_seat_prep AS (
    SELECT 
        party_id,
        party_total,
        FLOOR(raw_seats) AS confirmed_seats, 
        raw_seats - FLOOR(raw_seats) AS remainder,
        ROW_NUMBER() OVER (ORDER BY (raw_seats - FLOOR(raw_seats)) DESC) as remainder_rank
    FROM calc_shares
),
gap_analysis AS (
    SELECT CAST(100 - SUM(confirmed_seats) AS INT) as missing_seats 
    FROM party_seat_prep
), 
party_seat as (
SELECT 
    party_id,
    (confirmed_seats + (CASE WHEN remainder_rank <= (SELECT missing_seats FROM gap_analysis) THEN 1 ELSE 0 END)) as total_party_list_seats
FROM party_seat_prep
)
select 
	dp.name,
	dp.color,
	constituency_seats + total_party_list_seats as total_seats
from constituency_seat cs
join party_seat ps on cs.party_id = ps.party_id
left join dim_party dp on cs.party_id = dp.party_id
order by total_seats desc
limit 1;