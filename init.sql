-- create schema
CREATE SCHEMA IF NOT EXISTS election_db;

CREATE TABLE IF NOT EXISTS election_db.dim_province(
    province_id INT NOT NULL,
    prov_id VARCHAR(50) NOT NULL UNIQUE,
    province VARCHAR(200) NOT NULL UNIQUE,
    abbre_thai VARCHAR(50) NOT NULL UNIQUE,
    blank_votes INT,
    counted_vote_stations INT,
    invalid_votes INT,
    party_list_blank_votes INT,
    party_list_invalid_votes INT,
    party_list_percent_turn_out FLOAT,
    party_list_turn_out INT,
    party_list_valid_votes INT,
    pause_report BOOLEAN,
    percent_count FLOAT,
    percent_turn_out FLOAT,
    total_registered_vote INT,
    total_vote_stations INT,
    turn_out INT,
    valid_votes INT,
    PRIMARY KEY (province_id),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS election_db.dim_constituency(
    cons_id VARCHAR(50) NOT NULL UNIQUE,
    cons_no INT NOT NULL,
    prov_id VARCHAR(50) NOT NULL,
    registered_vote INT,
    total_vote_stations INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cons_id),
    CONSTRAINT fk_province FOREIGN KEY(prov_id) REFERENCES election_db.dim_province(prov_id)
);

CREATE TABLE IF NOT EXISTS election_db.dim_zone(
    cons_id VARCHAR(50) NOT NULL,
    zone VARCHAR(200) NOT NULL,
    PRIMARY KEY (cons_id, zone),
    CONSTRAINT fk_constituency FOREIGN KEY(cons_id) REFERENCES election_db.dim_constituency(cons_id)
);

CREATE TABLE IF NOT EXISTS election_db.dim_party(
    party_id INT NOT NULL UNIQUE,
    party_no INT NOT NULL UNIQUE,
    abbr VARCHAR(50) ,
    name VARCHAR(200) ,
    color VARCHAR(50) ,
    logo_url TEXT ,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (party_id)
);

CREATE TABLE IF NOT EXISTS election_db.dim_mp_candidate(
    mp_candidate_id VARCHAR(50) NOT NULL UNIQUE,
    mp_candidate_name VARCHAR(200)  NOT NULL,
    mp_candidate_no INT,
    mp_candidate_party_id INT,
    image_url TEXT ,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mp_candidate_id),
    CONSTRAINT fk_party FOREIGN KEY(mp_candidate_party_id) REFERENCES election_db.dim_party(party_id)
);

CREATE TABLE IF NOT EXISTS election_db.dim_party_candidate(
    party_no INT NOT NULL,
    list_no INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    image_url TEXT ,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (party_no, list_no),
    CONSTRAINT fk_party FOREIGN KEY(party_no) REFERENCES election_db.dim_party(party_no)
);

CREATE TABLE IF NOT EXISTS election_db.fact_vote_party(
    prov_id VARCHAR(50) NOT NULL,
    cons_id VARCHAR(50) NOT NULL,
    party_id INT NOT NULL,
    party_list_vote INT,
    party_list_vote_percent FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prov_id, cons_id, party_id),
    CONSTRAINT fk_province FOREIGN KEY(prov_id) REFERENCES election_db.dim_province(prov_id),
    CONSTRAINT fk_constituency FOREIGN KEY(cons_id) REFERENCES election_db.dim_constituency(cons_id),
    CONSTRAINT fk_party FOREIGN KEY(party_id) REFERENCES election_db.dim_party(party_id) 
);

CREATE TABLE IF NOT EXISTS election_db.fact_vote_constituency(
    prov_id VARCHAR(50) NOT NULL,
    cons_id VARCHAR(50) NOT NULL,
    mp_candidate_id VARCHAR(50) NOT NULL,
    party_id INT NOT NULL,
    mp_app_rank INT ,
    mp_candidate_vote INT ,
    mp_app_vote_percent FLOAT ,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prov_id, cons_id, mp_candidate_id, party_id),
    CONSTRAINT fk_province FOREIGN KEY(prov_id) REFERENCES election_db.dim_province(prov_id),
    CONSTRAINT fk_constituency FOREIGN KEY(cons_id) REFERENCES election_db.dim_constituency(cons_id),
    CONSTRAINT fk_candidate FOREIGN KEY(mp_candidate_id) REFERENCES election_db.dim_mp_candidate(mp_candidate_id),
    CONSTRAINT fk_party FOREIGN KEY(party_id) REFERENCES election_db.dim_party(party_id)
);