-- create role table
CREATE TABLE IF NOT EXISTS stream (
    tweet_created TIMESTAMP,
    "text" TEXT,
    polarity REAL
);
