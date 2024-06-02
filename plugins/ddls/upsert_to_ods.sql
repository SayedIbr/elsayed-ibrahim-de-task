-- upsert_to_ods.sql
DELETE FROM ods.book_list
USING staging.book_list
WHERE ods.book_list.book_list_date = staging.book_list.book_list_date
AND ods.book_list.list_name = staging.book_list.list_name
AND staging.book_list.book_list_date >= (SELECT MAX(book_list_date) - INTERVAL '7 days' FROM ods.book_list);

INSERT INTO ods.book_list (book_list_date, list_name, display_name, list_name_encoded, updated)
SELECT book_list_date, list_name, display_name, list_name_encoded, updated
FROM staging.book_list;

DELETE FROM ods.book
USING staging.book
WHERE ods.book.book_date = staging.book.book_date
AND ods.book.list_name = staging.book.list_name
AND ods.book.rank = staging.book.rank
AND staging.book.book_date >= (SELECT MAX(book_date) - INTERVAL '7 days' FROM ods.book);

INSERT INTO ods.book (book_date, list_name, rank, rank_last_week, weeks_on_list, asterisk, dagger, primary_isbn10, primary_isbn13,
                      publisher, description, title, author, contributor, contributor_note, book_image, book_image_width,
                      book_image_height, amazon_product_url, age_group, book_review_link, first_chapter_link, sunday_review_link,
                      article_chapter_link)
SELECT book_date, list_name, rank, rank_last_week, weeks_on_list, asterisk, dagger, primary_isbn10, primary_isbn13,
       publisher, description, title, author, contributor, contributor_note, book_image, book_image_width,
       book_image_height, amazon_product_url, age_group, book_review_link, first_chapter_link, sunday_review_link,
       article_chapter_link
FROM staging.book;
