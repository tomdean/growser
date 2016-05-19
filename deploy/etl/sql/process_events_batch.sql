DROP TABLE IF EXISTS rating_tmp;
DROP TABLE IF EXISTS login_tmp;
DROP TABLE IF EXISTS repository_tmp;
DROP TABLE IF EXISTS redirects_tmp;

CREATE TEMP TABLE rating_tmp (LIKE rating INCLUDING DEFAULTS);
CREATE TABLE login_tmp (LIKE login INCLUDING DEFAULTS);
CREATE TABLE repository_tmp (LIKE repository INCLUDING DEFAULTS);

COPY login_tmp (login_id, login, created_at) FROM '/data/batch/logins.csv' DELIMITER ',' CSV HEADER;
COPY repository_tmp (repo_id, name, created_at) FROM '/data/batch/repos.csv' DELIMITER ',' CSV HEADER;
COPY rating_tmp (login_id, repo_id, rating, created_at) FROM '/data/batch/events.csv' DELIMITER ',' CSV HEADER;

-- Add new logins & repositories
INSERT INTO login
	SELECT *
	FROM login_tmp AS lt
	WHERE NOT EXISTS (
		SELECT 1
		FROM login
		WHERE login.login_id = lt.login_id
	);

INSERT INTO repository
	SELECT *
	FROM repository_tmp AS rt
	WHERE NOT EXISTS (
		SELECT 1
		FROM repository
		WHERE repository.repo_id = rt.repo_id
	);

-- Update any redirected repositories to use their new ID
SELECT
    r1.repo_id AS old_repo_id,
    r2.repo_id AS new_repo_id
INTO TEMP redirects_tmp
FROM repository_redirect AS r
JOIN repository AS r1 ON r1.name = r.name_previous
JOIN repository AS r2 ON r2.name = r.name_updated;

UPDATE rating_tmp AS t
SET repo_id=r.new_repo_id
FROM redirects_tmp AS r
WHERE r.old_repo_id = t.repo_id
    AND NOT EXISTS (
        SELECT 1
        FROM rating_tmp AS r2
        WHERE r2.login_id = t.login_id
            AND r2.repo_id = r.new_repo_id
    );

INSERT INTO rating
	SELECT *
	FROM rating_tmp AS rt
	WHERE NOT EXISTS (
		SELECT 1
		FROM rating AS r
		WHERE r.login_id = rt.login_id
			AND r.repo_id = rt.repo_id
	)
	ORDER BY created_at ASC, repo_id ASC;

SELECT
    repo_id,
    COUNT(1) AS num_events
INTO TEMP repo_events
FROM rating_tmp AS r
GROUP BY repo_id;

UPDATE repository AS r
SET num_events = r.num_events + u.num_events
FROM repo_events AS u
WHERE u.repo_id = r.repo_id;

DROP TABLE rating_tmp;
DROP TABLE login_tmp;
DROP TABLE repository_tmp;
