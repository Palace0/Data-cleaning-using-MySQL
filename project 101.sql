
-- Data cleaning

SELECT*
FROM layoffs;

-- Remove Duplicate
-- Standardize the Data
-- Null values
-- Remove any columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH Duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM Duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoffs_staging
WHERE company='Casper';

WITH Duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

DELETE
FROM Duplicate_cte
WHERE row_num > 1;


CREATE TABLE layoffs_staging2 AS
SELECT *
FROM layoffs_staging;


