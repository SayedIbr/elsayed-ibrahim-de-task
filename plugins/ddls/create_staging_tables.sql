-- create_staging_tables.sql
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.book_list (
    book_list_date DATE,
    list_name VARCHAR,
    display_name VARCHAR,
    list_name_encoded VARCHAR,
    updated VARCHAR
);

CREATE TABLE IF NOT EXISTS staging.book (
    book_date DATE,
    list_name VARCHAR,
    rank INTEGER,
    rank_last_week INTEGER,
    weeks_on_list INTEGER,
    asterisk INTEGER,
    dagger INTEGER,
    primary_isbn10 VARCHAR,
    primary_isbn13 VARCHAR,
    publisher VARCHAR,
    description TEXT,
    title VARCHAR,
    author VARCHAR,
    contributor VARCHAR,
    contributor_note TEXT,
    book_image TEXT,
    book_image_width INTEGER,
    book_image_height INTEGER,
    amazon_product_url TEXT,
    age_group VARCHAR,
    book_review_link TEXT,
    first_chapter_link TEXT,
    sunday_review_link TEXT,
    article_chapter_link TEXT
);
