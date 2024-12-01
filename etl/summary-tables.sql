-- BUILD SUMMARY COUNTRY

select country, count(artist_id) as artist_count
from artists
group by country
ORDER BY artist_count DESC;

select sum (song_count) from (
select artists.country, count(*) as song_count
from artists join songs on artists.artist_id = songs.artist_id
group by artists.country) as a

select a.country, song_count, artist_count from (
        select artists.country, count(*) as song_count
        from artists join songs on artists.artist_id = songs.artist_id
        group by artists.country
    ) as a join (
        select country, count(artist_id) as artist_count
        from artists
        group by country
    ) as b on a.country = b.country;


-- BUILD ARTIST SUMMARY


select artists.artist_id, artists.name as artist_name, a.favorite_count, b.song_count
from 
    artists join (
        select songs.artist_id, count(*) as favorite_count
        from songs join favorites on songs.song_uuid = favorites.song_uuid
        group by songs.artist_id 
    ) as a on artists.artist_id = a.artist_id
    join (
        select songs.artist_id, count(*) as song_count
        from songs
        group by songs.artist_id
    ) as b on artists.artist_id = b.artist_id
    order by song_count DESC;

-- BUILD ALBUM SUMMARY

select albums.album_uuid, albums.name as album_name, a.favorite_count, b.song_count
from 
    albums join (
        select songs.album_uuid, count(*) as favorite_count
        from songs join favorites on songs.song_uuid = favorites.song_uuid
        group by songs.album_uuid 
    ) as a on albums.album_uuid = a.album_uuid
    join (
        select songs.album_uuid, count(*) as song_count
        from songs
        group by songs.album_uuid
    ) as b on albums.album_uuid = b.album_uuid
    order by song_count DESC

-- GENRE SUMMARY

select a.genre, a.song_count, b.album_count
from (
    select genre, count(*) as song_count
    from songs
    group by genre
) as a join (
    select genre, count(*) as album_count
    from albums
    group by genre
) as b on a.genre = b.genre;

-- YEAR SUMMARY
select year(parse_datetime(release_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'))  as release_year, count(*) as song_count
from songs
group by year(parse_datetime(release_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
order by release_year
