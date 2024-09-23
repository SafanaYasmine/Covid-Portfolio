select * from  layoffs_staging2;
select min(date),max(date) from layoffs_staging2;
select country,sum(total_laid_off) from layoffs_staging2 group by country order by 2 desc; 
-- since 2020-03-11 (covid's imapct) till 2023-03-06 
-- united states had laid off more employess - 258159
-- India 35993
-- Netherlands 17220

select industry,sum(total_laid_off) from layoffs_staging2 group by industry order by 2 desc; 
-- consumer , retail. other, transportation had more laid-offs

select stage,sum(total_laid_off) from layoffs_staging2 group by stage order by 2 desc; 
-- post-ipo , unknown , acquired,series A to J  had more laid offs
select date,sum(total_laid_off) from layoffs_staging2 group by date order by 2 desc; 
-- 2023-01-04 had the maximum laid off

-- month wise laid offs
-- select year(date)||month(date) as YYYYMM ,sum(total_laid_off) from layoffs_staging2 group by YYYYMM order by 2 desc; 
select year(date) year ,sum(total_laid_off) from layoffs_staging2 group by year order by 2 desc; 
-- 2022 had more laid offs than 2023
-- select format(`date`,'%Y/%m') yearmonth ,sum(total_laid_off) from layoffs_staging2 group by yearmonth order by 2 desc; 
WITH rolling_total_cte as (
SELECT substring(`date`,1,7) yearmonth ,sum(total_laid_off) total_laid_off 
FROM layoffs_staging2 where `date` IS NOT NULL
GROUP BY yearmonth ORDER BY 2 DESC
) 
-- view progression
SELECT yearmonth,total_laid_off,sum(total_laid_off) OVER(ORDER BY yearmonth) AS rolling_total
FROM rolling_total_cte

-- company rolling_total
-- list down top 10 company had a series of more lay offs 
WITH company_total as (
SELECT company,year(`date`) years ,sum(total_laid_off) total_laid_off 
FROM layoffs_staging2 where `date` IS NOT NULL
GROUP BY company,years ORDER BY 3  DESC
), company_rolling_total as
(
SELECT company,years,total_laid_off, sum(total_laid_off) OVER(PARTITION BY company ORDER BY years,total_laid_off ) AS rolling_total,
ROW_NUMBER() OVER(PARTITION BY company ORDER BY years,total_laid_off ) AS ranking
FROM company_total
)
SELECT company,rolling_total FROM company_rolling_total WHERE ranking>1 order by rolling_total desc limit 10;

-- another approach of top 10 series of more layoffs happened in company
select company,sum(total_laid_off) total_laid_off 
from layoffs_staging2
group by company having count(distinct year(`date`))>1
order by total_laid_off desc
limit 10

-- list top 5 companys who had more laid offs in each year
WITH company_laid_off (company,years,total_laid_off) as
(
SELECT company, YEAR(`date`) ,sum(total_laid_off) 
FROM layoffs_staging2 WHERE `date` IS NOT NULL 
GROUP BY company, YEAR(`date`) ORDER BY 3 DESC
), company_laid_off_ranking as
(SELECT *,RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking FROM company_laid_off) 

SELECT * FROM company_laid_off_ranking WHERE ranking<=5;









