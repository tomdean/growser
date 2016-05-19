/*
SELECT repo_id
INTO TEMP repo_tmp
FROM rating
GROUP BY repo_id
HAVING COUNT(1) >= 100;

SELECT login_id
INTO TEMP login_tmp
FROM rating
GROUP BY login_id
HAVING COUNT(1) BETWEEN 50 AND 1000;

COPY (
	SELECT
		login_id,
		repo_id,
		rating,
		extract(epoch from created_at) AS created_at
	FROM rating AS r
	WHERE EXISTS (
		SELECT 1
		FROM repo_tmp
		WHERE repo_id = r.repo_id
	) AND EXISTS (
		SELECT 1
		FROM login_tmp
		WHERE login_id = r.login_id
	)
) TO '/data/csv/ratings.all.csv' DELIMITER ',' CSV;

DROP TABLE repo_tmp;
DROP TABLE login_tmp;
*/

SELECT repo_id
INTO TEMP repo_tmp
FROM rating
WHERE created_at >= NOW() - INTERVAL '1 year'
GROUP BY repo_id
HAVING COUNT(1) >= 50;

SELECT login_id
INTO TEMP login_tmp
FROM rating
WHERE created_at >= NOW() - INTERVAL '1 year'
GROUP BY login_id
HAVING COUNT(1) >= 15;

COPY (
	SELECT
		login_id,
		repo_id,
		rating,
		extract(epoch from created_at) AS created_at
	FROM rating AS r
	WHERE r.created_at >= NOW() - INTERVAL '1 year'
	    AND EXISTS (
            SELECT 1
            FROM repo_tmp
            WHERE repo_id = r.repo_id
        ) AND EXISTS (
            SELECT 1
            FROM login_tmp
            WHERE login_id = r.login_id
        )
) TO '/data/csv/ratings.year.csv' DELIMITER ',' CSV;

DROP TABLE repo_tmp;
DROP TABLE login_tmp;

SELECT repo_id
INTO TEMP repo_tmp
FROM rating
WHERE created_at >= NOW() - INTERVAL '120 days'
GROUP BY repo_id
HAVING COUNT(1) >= 25;

SELECT login_id
INTO TEMP login_tmp
FROM rating
WHERE created_at >= NOW() - INTERVAL '120 days'
GROUP BY login_id
HAVING COUNT(1) > 10;

COPY (
	SELECT
		login_id,
		repo_id,
		rating,
		extract(epoch from created_at) AS created_at
	FROM rating AS r
	WHERE r.created_at >= NOW() - INTERVAL '120 days'
	    AND EXISTS (
            SELECT 1
            FROM repo_tmp
            WHERE repo_id = r.repo_id
        ) AND EXISTS (
            SELECT 1
            FROM login_tmp
            WHERE login_id = r.login_id
        )
) TO '/data/csv/ratings.120days.csv' DELIMITER ',' CSV;

DROP TABLE repo_tmp;
DROP TABLE login_tmp;