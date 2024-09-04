-- Data Cleaning

SELECT *
FROM layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors such as correcting spelling errors
-- 3. Look at null values or blank values 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. check for duplicates and remove any

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- REMOVING DUPLICATE ROWS

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
/*
Inserted 2 times so all rows were duplicated. deleted all table and re-inserted
SELECT * FROM layoffs_staging;
DELETE FROM layoffs_staging;
*/

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


# got following statements from left pane, right clicking layoffs_staging >>> copy to clipboard >>> create statement


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

SELECT *
FROM layoffs_staging2
WHERE row_num > 1; 

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1; 

SELECT *
FROM layoffs_staging2
WHERE row_num > 1; 

SELECT *
FROM layoffs_staging2;

-- 2. standardize data and fix errors such as correcting spelling errors

SELECT DISTINCT(company) 
FROM layoffs_staging2;

SELECT DISTINCT(TRIM(company)) 
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# ------------------------------------------------------------
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry 
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

# ------------------------------------------------------------
SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;

# ------------------------------------------------------------

SELECT * 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) # TRIM(TRAILING is a bit advanced to get rid of '.' at end of United States
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country = 'United States%';

# ------------------------------------------------------------

# currently date column is in text format and we want to change format to date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') # % to start fomat of date followed by month, "lower case", (upper case M is totally different thing)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `DATE` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

# Now if we refresh schema on the left pane and look at the type of date, it is still text but correct format
# now after having correct date format we change the text type to date type

ALTER TABLE layoffs_staging2
MODIFY COLUMN `DATE` DATE;

# now after alter table and modify column, refresh schema on the left pane and look at the type of date, it is changed from text to date type

# ------------------------------------------------------------

SELECT * 
FROM layoffs_staging2;

# changed issues with company, Industry, country, and date, extra column row_num,  to delete duplicates

-- 3. Look at null values or blank values

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT DISTINCT industry 
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT * 
FROM layoffs_staging2 t1   	# could say AS t1
JOIN layoffs_staging2 t2		# join to itself. nameing it t1 and t2 because they are exact same table
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2 t1   	
JOIN layoffs_staging2 t2		
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT * 
FROM layoffs_staging2;

-- 4. remove any columns and rows that are not necessary - few ways

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;








