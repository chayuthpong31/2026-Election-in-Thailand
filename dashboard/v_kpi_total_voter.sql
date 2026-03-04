create view total_voter as 
SELECT SUM(party_list_vote) from fact_vote_party;