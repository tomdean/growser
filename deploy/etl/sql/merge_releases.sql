SELECT
    r.release_id,
    rr.repo_id,
    l.login_id,
    r.url,
    r.name,
    r.commitish,
    r.tag,
    r.body,
    r.published_at,
    r.created_at
INTO TEMP releases_ins
FROM release_tmp AS r
JOIN repository AS rr ON rr.name = r.repo
JOIN login AS l ON l.login = r.login
WHERE rr.num_events >= 500;

DELETE FROM releases_ins WHERE EXISTS (
    SELECT 1
    FROM release
    WHERE release_id = releases_ins.release_id
);

INSERT INTO release SELECT * FROM releases_ins;

UPDATE repository AS r
SET last_release_at = u.published_at
FROM releases_ins AS u
WHERE u.repo_id = r.repo_id;

DROP TABLE release_tmp;
DROP TABLE releases_ins;