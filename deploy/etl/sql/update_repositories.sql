UPDATE repository SET
    description   = u.description,
    homepage      = u.homepage,
    language      = u.language,
    num_forks     = u.num_forks,
    num_stars     = u.num_stars,
    num_watchers  = u.num_watchers,
    updated_at    = NOW()
FROM repos_tmp AS u
WHERE u.name = repository.name;

INSERT INTO repository_task (repo_id, name, created_at)
    SELECT r.repo_id, 'github.api.repos', NOW()
    FROM repos_tmp AS t
    JOIN repository AS r ON r.name = t.name;

DROP TABLE repos_tmp;