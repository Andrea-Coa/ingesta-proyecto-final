DROP DATABASE IF EXISTS pizzicato_summary_db;
CREATE DATABASE pizzicato_summary_db CHARSET utf8mb4;

USE pizzicato_summary_db;

CREATE TABLE country_summary (
    country TEXT,
    song_count INT,
    artist_count INT
);

CREATE TABLE artist_summary (
    artist_id TEXT,
    artist_name TEXT,
    favorite_count INT,
    song_count INT
);

CREATE TABLE album_summary (
    album_uuid TEXT,
    album_name TEXT,
    favorite_count INT,
    song_count INT
);

CREATE TABLE genre_summary(
    genre TEXT,
    song_count INT,
    album_count INT
);

CREATE TABLE year_summary(
    release_year INT,
    song_count INT
)