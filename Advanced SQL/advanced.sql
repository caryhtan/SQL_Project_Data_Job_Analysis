create table data_science_jobs(
    job_id INT primary key,
    job_title text,
    company_name text,
    post_date date
)

insert into data_science_jobs
values (1, 'Data Scientist', 'Tech Innovations', '2023-01-01')

insert into data_science_jobs
values
    (2, 'Machine Learning Engineer', 'Data Driven Co', '2023-01-15'),
    (3, 'AI Specialist', 'Future Tech', '2023-02-01');

select * from data_science_jobs

alter table data_science_jobs
add remote boolean;

alter table data_science_jobs
rename column post_date to posted_on

alter table data_science_jobs
alter column remote set default FALSE

insert into data_science_jobs
values (4, 'Data Scientist', 'Google', '2023-02-05');

alter table data_science_jobs
drop column company_name

update data_science_jobs
set  remote = TRUE
where job_id = 2

drop table data_science_jobs

select * from job_postings_fact
limit 5

select 
    job_schedule_type,
    AVG(salary_year_avg) as avg_salary_yearly,
    AVG(salary_hour_avg) as avg_salary_hourly

from
    job_postings_fact

WHERE
    job_posted_date::date > '2023-06-01'

group by
    job_schedule_type

order by
    job_schedule_type

-- data functions 
-- p2
select
    extract(month from job_posted_date at time zone 'UTC' at time zone 'America/New_York') as month,
    count(*) as postings_count

from
    job_postings_fact

group by
    month

order by
    month

-- p3
select * from job_postings_fact
limit 5

select * from company_dim
limit 5

select
    company_dim.name as company_name,
    count(job_postings_fact.job_id) as job_postings_count

from
    job_postings_fact
inner join company_dim on job_postings_fact.company_id = company_dim.company_id

where
    job_postings_fact.job_health_insurance = TRUE
    and extract(quarter from job_postings_fact.job_posted_date ) = 2

group by
    company_name

having
    count(job_postings_fact.job_id) > 0

order by
    job_postings_count desc;

--CASE
--p1
select
    job_id,
    job_title,
    salary_year_avg,
    case
        when salary_year_avg > 100000 then 'high salary'
        when salary_year_avg between 60000 and 99999 then 'standard salary'
        when salary_year_avg < 60000 then 'low salary'
    end as salary_category

from
    job_postings_fact

where
    salary_year_avg is not null
    and job_title_short = 'Data Analyst'

order by
    salary_year_avg desc;

--p2
select
    count(distinct case when job_work_from_home = 'TRUE' then company_id end) as WFH_companies,
    count(distinct case when job_work_from_home = 'FALSE' then company_id end) as non_WFH_companies

from 
    job_postings_fact

--p3
select
    job_id,
    salary_year_avg,
    case
        when job_title ilike '%Senior%' then 'Senior'
        when job_title ilike '%Lead%' or job_title ilike '%Manager%' then 'Lead/Manager'
        when job_title ilike '%Junior%' or job_title ilike '&Entry&' then 'Junior/Entry'
        else 'Not Specified'
    end as experience_level,

    case
        when job_work_from_home = 'TRUE' then 'Yes'
        when job_work_from_home = 'FALSE' then 'No'
    end as remote_option

from 
    job_postings_fact

where
    salary_year_avg is not null

order by
    job_id 

--subqueries
--p1
select
    skills_dim.skills
from
    skills_dim
inner join (
    select skill_id
    from skills_job_dim
    group by skill_id
    order by count(job_id) desc
    limit 5
) as top_skills on skills_dim.skill_id = top_skills.skill_id;

select * from job_postings_fact limit 5

--p2
SELECT 
	company_id,
  name,
	-- Categorize companies
  CASE
	  WHEN job_count < 10 THEN 'Small'
    WHEN job_count BETWEEN 10 AND 50 THEN 'Medium'
    ELSE 'Large'
  END AS company_size

FROM 
-- Subquery to calculate number of job postings per company 
(
  SELECT 
		company_dim.company_id, 
		company_dim.name, 
		COUNT(job_postings_fact.job_id) as job_count
  FROM 
		company_dim
  INNER JOIN job_postings_fact ON company_dim.company_id = job_postings_fact.company_id
  GROUP BY 
		company_dim.company_id, 
		company_dim.name
) AS company_job_count;

--p3
select
    company_dim.name
from 
    company_dim
    inner join(
        select
            company_id,
            avg(salary_year_avg) as avg_yearly
        from job_postings_fact
        group by
            company_id
    ) as company_salaries on company_dim.company_id = company_salaries.company_id
where
    company_salaries.avg_yearly > (
        select
            avg(salary_year_avg)
            from job_postings_fact
    )

--CTE
--p1
with title_diversity as(
    select 
        company_id,
        count(distinct job_title) as unique_titles
    from
        job_postings_fact
    group by
        company_id
)
select 
    company_dim.name,
    title_diversity.unique_titles
from 
    title_diversity
    inner join company_dim on title_diversity.company_id = company_dim.company_id
order by
    unique_titles desc
limit 10

--Union
--p1
(
    select
        job_id,
        job_title,
        'With salary info' as salary_info
    from
        job_postings_fact
    where
        salary_year_avg is not null 
        or salary_hour_avg is not null
)

union all

(
    select
        job_id,
        job_title,
        'Without salary info' as salary_info
    from
        job_postings_fact
    where
        salary_year_avg is null 
        and salary_hour_avg is null
)

order by
    salary_info desc,
    job_id

--p2
