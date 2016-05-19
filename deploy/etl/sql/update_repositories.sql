UPDATE repository SET
    description=u.description,
    homepage=u.homepage,
    language=u.language,
    num_forks=u.num_forks,
    num_stars=u.num_stars,
    num_watchers=u.num_watchers,
    updated_at=now()
FROM repos_tmp AS u
WHERE u.repo_id = repository.repo_id;

INSERT INTO repository_task (repo_id, name, created_at)
    SELECT repo_id, 'github.api.repos', now()
    FROM repos_tmp;

DROP TABLE repos_tmp;