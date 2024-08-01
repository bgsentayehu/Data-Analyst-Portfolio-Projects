-- Data Cleaning 

-- Selecting all data from the layoffs table
SELECT * 
FROM layoffs; 

-- 1) Remove duplicates 
-- 2) Standardize the data
-- 3) Handle null values or blank values
-- 4) Remove unnecessary columns

-- REMOVING DUPLICATES

-- Creating a staging table with the same structure as the original table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Inserting data from the original table into the staging table
INSERT INTO layoffs_staging 
SELECT *
FROM layoffs;

-- Adding a row number for duplicate detection
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Creating a CTE to identify duplicate rows
WITH duplicate_CTE AS 
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
-- Selecting duplicate rows (row_num >= 2 indicates duplicates)
SELECT * 
FROM duplicate_CTE 
WHERE row_num >= 2;

-- Checking a specific company for duplicates
SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- Creating another staging table to hold unique records
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

-- Verifying the structure of the new staging table
SELECT *
FROM layoffs_staging2;

-- Inserting data into the new staging table with row numbers for duplicate detection
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting duplicate rows from the new staging table
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- STANDARDIZING DATA

-- Trimming whitespace from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Updating the company names to remove leading and trailing whitespace
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Selecting rows where industry starts with 'Crypto'
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Standardizing the industry names to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Removing trailing periods from country names and verifying the unique values
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Updating country names to remove trailing periods
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Converting date strings to date format
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Updating the date column to store dates in date format
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Modifying the date column to be of DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULL AND BLANK VALUES

-- Selecting rows with null or blank industry values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- Checking rows for a specific company
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Setting blank industry values to NULL
UPDATE layoffs_staging2 
SET industry = NULL 
WHERE industry = '';

-- Finding industry values to use for updating null values
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

-- Updating null industry values using values from other rows with the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Removing unnecessary columns

-- Selecting rows where both total_laid_off and percentage_laid_off are null
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

-- Deleting rows where both total_laid_off and percentage_laid_off are null
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

-- Verifying the data after cleaning
SELECT *
FROM layoffs_staging2;

-- Dropping the row_num column as it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Exploratory Data Analysis

-- Finding the maximum values for total_laid_off and percentage_laid_off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Selecting rows where percentage_laid_off is 1, ordered by total_laid_off in descending order
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Summing total_laid_off by company, ordered by the sum in descending order
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Finding the date range of the layoffs data
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Summing total_laid_off by industry, ordered by the sum in descending order
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Summing total_laid_off by country, ordered by the sum in descending order
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Summing total_laid_off by year, ordered by year in descending order
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Summing total_laid_off by stage, ordered by the sum in descending order
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Calculating a rolling sum of total_laid_off by month
WITH Rolling_Sum AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS Total_off 
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` 
)
SELECT `MONTH`, Total_off, SUM(Total_Off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Sum;

-- Summing total_laid_off by company and year, ordered by the sum in descending order
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- Ranking companies by total_laid_off for each year
WITH rank_laid_off AS
(
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, years
ORDER BY 3 DESC
), Company_year_ranking AS
(
SELECT company, years, total_off,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_off DESC) AS ranking
FROM rank_laid_off
WHERE years IS NOT NULL
)
-- Selecting the top 5 companies with the most layoffs for each year
SELECT *
FROM Company_year_ranking
WHERE ranking <= 5;
