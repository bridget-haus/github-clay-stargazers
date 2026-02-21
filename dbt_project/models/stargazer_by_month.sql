select
    repo_full_name,
    date_trunc('month', starred_at) as month,
    count(*) as stars
from main.raw_github_stargazers
group by
    repo_full_name,
    date_trunc('month', starred_at)
order by
    repo_full_name,
    month
