
/*
===========================================================================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
===========================================================================================================

Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load) Process to populate the 'Silver'
	Schema tables from the 'bronze' schema.

Action Performed:
	- Truncate Silver tables.
	- Inserts transformed and cleased data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC Silver.load_silver;
*/

CREATE OR ALTER PROC Silver.load_silver
AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	SET @batch_start_time = GETDATE()

	BEGIN TRY
		PRINT '=====================================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=====================================================================================';


		PRINT '-------------------------------------------------------------------------------------';
		PRINT 'loading CRM Tables'
		PRINT '-------------------------------------------------------------------------------------';

		-- Loading Silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_cust_info'
		TRUNCATE TABLE Silver.crm_cust_info;

		PRINT '>> Inserting Data Into: Silver.crm_cust_info'
		INSERT INTO Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS t
		WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>-----------------'

		-- Loading Silver.crm_prod_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_prod_info'
		TRUNCATE TABLE Silver.crm_prod_info;

		PRINT '>> Inserting Data Into: Silver.crm_prod_info'
		INSERT INTO Silver.crm_prod_info (
			prd_id, 
			cat_id,
			prd_key, 
			prd_nm, 
			prd_cost, 
			prd_line, 
			prd_start_dt, 
			prd_end_dt 
		)
		SELECT 
			prd_id,
			-- sperate prd_key columns to cat_id and prd_key columns
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN  'M' THEN 'Mountain'
				WHEN  'R' THEN 'Road'
				WHEN  'S' THEN 'Other Sales'
				WHEN  'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prod_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>-----------------'

		-- Loading Silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_sales_details'
		TRUNCATE TABLE Silver.crm_sales_details;

		PRINT '>> Inserting Data Into: Silver.crm_sales_details'
		INSERT INTO Silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt, 
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,	-- recalculate sales if original value is missing or invalid.
			sls_quantity,
			CASE	
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price  -- derive price if original value is invalid.
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>-----------------'

		-- Loading Silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_cust_az12'
		TRUNCATE TABLE Silver.erp_cust_az12;

		PRINT '>> Inserting Data Into: Silver.erp_cust_az12'
		INSERT INTO Silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN  'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN  'Female'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12

		PRINT '>> Truncating Table: Silver.erp_loc_a101'
		TRUNCATE TABLE Silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: Silver.erp_loc_a101'
		INSERT INTO Silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and handle missing or blank country values.
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>-----------------'

		-- Loading Silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2'
		INSERT INTO Silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>-----------------'

		SET @batch_end_time = GETDATE();
		PRINT '==================================================';
		PRINT 'Loading Silver Layer Completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) + ' seconds'
		PRINT '==================================================';


	END TRY

	BEGIN CATCH
		PRINT '=========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================================================';
	END CATCH
END 

