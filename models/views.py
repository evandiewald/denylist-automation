

users_view_sql = """CREATE OR REPLACE VIEW users AS (

with entries_expanded as (
select 
	e.address,
	ii.issue_type,
	ii.user,
	ii.state
from entries e
join issues ii on e.issue_number = ii.number
)

select 

i.user as user,
max(i.number) as last_issue,
max(i.created_at) as last_created_at,
min(i.number) as first_issue,
min(i.created_at) as first_created_at,
count(*) as n_issues,
(select count(*) from issues i2 where i2.user = i.user and state = 'closed'::state_type) as n_closed_issues,
(select count(*) from entries_expanded ex where ex.user = i.user and ex.issue_type = 'addition'::issue_type) as n_additions_submitted,
(select count(*) from entries_expanded ex where ex.user = i.user and ex.issue_type = 'addition'::issue_type and ex.state = 'closed'::state_type) as n_additions_closed,
(select count(*) from entries_expanded ex where ex.user = i.user and ex.issue_type = 'removal'::issue_type) as n_removals_submitted,
(select count(*) from entries_expanded ex where ex.user = i.user and ex.issue_type = 'removal'::issue_type and ex.state = 'closed'::state_type) as n_removals_closed


from issues i
group by i.user);"""


