-- create_tables.sql
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.DimTime (
    time_key SERIAL PRIMARY KEY,
    year INTEGER,
    quarter INTEGER
);

CREATE TABLE IF NOT EXISTS dm.DimList (
    list_key SERIAL PRIMARY KEY,
    list_name VARCHAR,
    display_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dm.DimBook (
    book_key SERIAL PRIMARY KEY,
    title VARCHAR,
    author VARCHAR
);

CREATE TABLE IF NOT EXISTS dm.DimPublisher (
    publisher_key SERIAL PRIMARY KEY,
    publisher_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dm.FactBookListRank (
    book_list_rank_key SERIAL PRIMARY KEY,
    list_key INTEGER REFERENCES dm.DimList(list_key),
    book_key INTEGER REFERENCES dm.DimBook(book_key),
    publisher_key INTEGER REFERENCES dm.DimPublisher(publisher_key),
    rank INTEGER,
    published_date DATE,
    time_key INTEGER REFERENCES dm.DimTime(time_key)
);
