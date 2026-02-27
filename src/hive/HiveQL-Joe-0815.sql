/* Assignment 6 - Hive Queries for MovieLens Dataset
   EE 756 Big Data Analytics
   Student: Joe
   Due Date: 2025-08-15
*/

/* ============================
   Part 1: Create External Tables
   ============================ */

DROP TABLE IF EXISTS ratings;
CREATE EXTERNAL TABLE ratings (
    user_id INT,
    movie_id INT,
    rating INT,
    rating_timestamp STRING   /* avoid using timestamp to prevent issues with Hive */
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hhps/ml/ratings';

DROP TABLE IF EXISTS movies;
CREATE EXTERNAL TABLE movies (
    movie_id INT,
    title STRING,
    release_date STRING,
    video_release_date STRING,
    imdb_url STRING,
    unknown INT,
    action INT,
    adventure INT,
    animation INT,
    childrens INT,
    comedy INT,
    crime INT,
    documentary INT,
    drama INT,
    fantasy INT,
    film_noir INT,
    horror INT,
    musical INT,
    mystery INT,
    romance INT,
    sci_fi INT,
    thriller INT,
    war INT,
    western INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/hhps/ml/movies';

DROP TABLE IF EXISTS users;
CREATE EXTERNAL TABLE users (
    user_id INT,
    age INT,
    gender STRING,
    occupation STRING,
    zip_code STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/hhps/ml/users';

/* ============================
   Part 2: Basic Queries and Joins
   ============================ */

-- a) Show the first 10 records from each table
SELECT * FROM ratings LIMIT 10;
SELECT * FROM movies LIMIT 10;
SELECT * FROM users LIMIT 10;

-- b) Retrieve all ratings by users under age 25
SELECT u.user_id, u.age, m.title AS movie_title, r.rating
FROM ratings r
JOIN users u ON r.user_id = u.user_id
JOIN movies m ON r.movie_id = m.movie_id
WHERE u.age < 25
;

-- c) All movies rated 5 by female users
SELECT m.title AS movie_title, r.rating, u.user_id
FROM ratings r
JOIN users u ON r.user_id = u.user_id
JOIN movies m ON r.movie_id = m.movie_id
WHERE u.gender = 'F' AND r.rating = 5
;

-- d) The average rating of each movie
SELECT m.title AS movie_title, AVG(r.rating) AS average_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title
;

-- e) Count of movies by each user
SELECT user_id, COUNT(movie_id) AS count_of_movies
FROM ratings
GROUP BY user_id
;

/* ============================
   Part 3: Aggregation and Filtering
   ============================ */


-- a) Top 10 movies with the highest average rating
SELECT m.title AS movie_title, COUNT(r.rating) AS number_ratings, AVG(r.rating) AS average_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title
HAVING COUNT(r.rating) > 100
ORDER BY AVG(r.rating) DESC
LIMIT 10
;

-- b) Top 5 users who give the most ratings
SELECT user_id, COUNT(rating) AS count_of_ratings
FROM ratings
GROUP BY user_id
ORDER BY COUNT(rating) DESC
LIMIT 5
;


-- c) Average rating by occupation
SELECT u.occupation, AVG(r.rating) AS average_rating
FROM ratings r
JOIN users u ON r.user_id = u.user_id
GROUP BY u.occupation
;

-- d) Count of ratings by rating value
SELECT rating, COUNT(rating) AS count_of_ratings
FROM ratings
GROUP BY rating
ORDER BY COUNT(rating) DESC
;


/* ============================
   Part 4: Hive Features and Analytics
   ============================ */

-- a) Create a view for highly rated movies
CREATE VIEW highly_rated_movies AS
SELECT m.title AS movie_title, AVG(r.rating) AS average_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title
HAVING AVG(r.rating) > 4.5 AND COUNT(r.rating) > 50
;