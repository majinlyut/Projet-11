CREATE TABLE sport_activities (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    date_debut TIMESTAMP,
    type_activite TEXT,
    distance_m FLOAT,
    duree_s INT,
    commentaire TEXT
);
CREATE PUBLICATION debezium_publication FOR TABLE sport_activities;
