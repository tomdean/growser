UPDATE repository AS r
SET homepage=u.homepage
FROM (
	SELECT repo_id, substring(description from '(([\w-]+://?|www[.])[^\s()<>]+(?:\([\w\d]+\)|([^[:punct:]\s]|/)))') as homepage
	FROM repository
	WHERE status=1
	    AND homepage=''
		AND (description LIKE '%http://%' OR description LIKE '%https://%')
) AS u
WHERE u.repo_id = r.repo_id and u.homepage <> '';