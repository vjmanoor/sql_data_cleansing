-- Data Cleaning

use world_layoffs;

SELECT 
    *
FROM
    layoffs;

/*1.Remove dups
2.Standardize the data
3.Handing NULL and blank values
4.Remove redundant / residue columns */

CREATE TABLE layoffs_staging LIKE layoffs;

SELECT 
    *
FROM
    layoffs_staging;
    
insert layoffs_staging
select * from layoffs;

-- 1. Removing duplicates    
with duplicate_cte as 
(
  SELECT 
    *,
    row_number() over(partition by company, location, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging )
select * from duplicate_cte
where row_num > 1;

select * from layoffs_staging 
where company = 'casper';

with duplicate_cte as 
(
  SELECT 
    *,
    row_number() over(partition by company, location, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging )
delete from duplicate_cte
where row_num > 1;

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

insert into layoffs_staging2
  SELECT 
    *,
    row_number() over(partition by company, location, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging;
    
DELETE FROM layoffs_staging2 
WHERE
    row_num > 1;
    
SELECT 
    *
FROM
    layoffs_staging2;
    
-- Standardizing Data

SELECT company,
    trim(company)
FROM
    layoffs_staging2;
    
UPDATE layoffs_staging2 
SET 
    company = TRIM(company);

SELECT DISTINCT
    *
FROM
    layoffs_staging2
WHERE
    industry LIKE 'crypto%';

UPDATE layoffs_staging2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

SELECT DISTINCT
    industry
FROM
    layoffs_staging2;
    
SELECT DISTINCT
    country
FROM
    layoffs_staging2
order by 1;

UPDATE layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';


SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;
    
    
update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT 
    `date`
FROM
    layoffs_staging2;
    
alter table layoffs_staging2
modify column `date` DATE;

-- 3. Handing NULL and blank values

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT DISTINCT
    *
FROM
    layoffs_staging2
WHERE
    industry IS NULL OR industry = '';
    
UPDATE layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = '';
    
SELECT DISTINCT
    *
FROM
    layoffs_staging2
where company like 'Bally%';

SELECT 
t1.industry , t2.industry
FROM
    layoffs_staging2 t1
        JOIN
    layoffs_staging2 t2 ON t1.company = t2.company
        AND t1.location = t2.location
WHERE
    t1.industry IS NULL 
        AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
        JOIN
    layoffs_staging2 t2 ON t1.company = t2.company
        AND t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;


SELECT 
    *
FROM
    layoffs_staging2;

-- 4. Removing redundant column

alter table layoffs_staging2
DROP column row_num;








