select
  user_id,
  login,
  count(distinct repo_full_name) as repos_starred
from main.raw_github_stargazers
group by user_id, login
order by repos_starred desc