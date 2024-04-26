-- create role table
CREATE TABLE IF NOT EXISTS stream (
    created TIMESTAMP,
    "text" TEXT,
    polarity REAL
);
