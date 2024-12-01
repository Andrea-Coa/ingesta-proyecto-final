-- artista con más favoritos por usuario
CREATE OR REPLACE VIEW view_favorite_artist_by_tenant AS

SELECT f.tenant_id, s.artist_id, COUNT(*) AS fav_count
FROM favorites f JOIN songs s ON f.song_uuid = s.song_uuid
GROUP BY f.tenant_id, s.artist_id
HAVING COUNT(*) = (
        SELECT MAX(artist_likes)
        FROM (
            SELECT COUNT(*) AS artist_likes
            FROM favorites f2
            JOIN songs s2 ON f2.song_uuid = s2.song_uuid
            WHERE f2.tenant_id = f.tenant_id
            GROUP BY s2.artist_id
        ) AS subquery
    )
ORDER BY fav_count DESC;

-- álbum con más favoritos por usuario
CREATE OR REPLACE VIEW view_favorite_album_by_tenant AS
SELECT f.tenant_id, s.album_uuid, COUNT(*) AS fav_count
FROM favorites f JOIN songs s ON f.song_uuid = s.song_uuid
WHERE s.album_uuid != 'no album'
GROUP BY f.tenant_id, s.album_uuid
HAVING COUNT(*) = (
        SELECT MAX(album_likes)
        FROM (
            SELECT COUNT(*) AS album_likes
            FROM favorites f2
            JOIN songs s2 ON f2.song_uuid = s2.song_uuid
            WHERE s2.album_uuid != 'no album' 
            AND f2.tenant_id = f.tenant_id
            GROUP BY s2.album_uuid
        ) AS subquery
    )
ORDER BY fav_count DESC;