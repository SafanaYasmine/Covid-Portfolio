create database `world_layoffs`;
use `world_layoffs`;
select * from layoffs;
-- copy all data from layoffs to staging table
create table layoffs_staging like layoffs;
insert into layoffs_staging 
select * from layoffs;

select * from layoffs_staging limit 100;
-- remove duplicates 
-- in MySQL we cannot update the table of cte, 
-- error 19:39:34	with cte as ( select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging ) delete  from cte where row_num>1	Error Code: 1288. The target table cte of the DELETE is not updatable	0.000 sec
-- date is a keywod, so use ``
-- create new table with row_num included or


with cte as (
select *,row_number() over(partition by
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging
)
select *  from cte where row_num>1

select * from layoffs_staging where company='Cazoo';
/*
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
*/

with cte as (
select *,row_number() over(partition by
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num -- into layoff_staging3 
from layoffs_staging
)
select *  from cte where row_num>1

insert into layoffs_staging2 
select *,row_number() over(partition by
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num -- into layoff_staging3 
from layoffs_staging;

select * from layoffs_staging2 where row_num>1;
use `world_layoffs`
-- standardizing data
update layoffs_staging2 set company=trim(company);

select distinct(industry) from layoffs_staging2 order by 1;
select distinct(industry)  from layoffs_staging2 where  industry like 'Crypto%';
update layoffs_staging2 set industry='Crypto' where industry like 'Crypto%';

select distinct(trim(trailing '.' from country) )
from layoffs_staging2 where country like 'United States%';
-- '.' at the end of unites states is taken care 
update layoffs_staging2 set country=(trim(trailing '.' from country) ) where country like 'United States%';

-- convert date columns to type date 
-- cast return null
-- select cast(`date` as date) from layoffs_staging2
select date,str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2
-- casting not changed the datatpe to date
-- select cast(`date` as date) as `date` from layoffs_staging2
alter table layoffs_staging2 modify column `date` date;

select * from  layoffs_staging2 where total_laid_off and percentage_laid_off is null;
select * from  layoffs_staging2 where (industry is null or industry='')
-- we will try to populate indsutry based on company
select * from layoffs_staging2 where company in ('Airbnb','Bally\'s Interactive','Carnava','Juul');

select t1.industry,t2.industry from layoffs_staging2 t1 , layoffs_staging2 t2
where  t1.company=t2.company and (t1.industry is null or t1.industry ='') and t2.industry is not null;

-- no update happened becuase of two type of empty values in industry, so update empty to null
update layoffs_staging2 set  industry=null where industry ='';
update layoffs_staging2 t1 join  layoffs_staging2 t2
on t1.company=t2.company
 set t1.industry =t2.industry
where  t1.company=t2.company and (t1.industry is null or t1.industry ='') and t2.industry is not null;

-- remove null values
select * from  layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

--  remove unwanted colums or rows

alter table layoffs_staging2 drop column row_num;

select * from layoffs_staging2;









