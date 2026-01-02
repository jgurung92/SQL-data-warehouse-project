/*
	--=================================================================================
	-- DDL Script: Create Gold Views
	--=================================================================================
	
	Script Purpose:
		This script creates views for the gold layer in the data warehouse.
		The Gold layer represents the final dimension and fact tables (Star Schema)

		Each view performs transformations and combines data from the silver layer
		to produce a clean, enriched, and business-ready dataset.

	Usage:
		- These views can be queried directly for analytics and reporting.

	--=================================================================================
*/

--=================================================================================
	-- Create dim Table: gold.dim_customers
	--=================================================================================
	IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
	GO
	
	CREATE VIEW gold.dim_customers 
	AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		ci.cst_marital_status AS marital_status,
		Lo.cntry AS country,
		CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE (Ca.gen, 'n/a')
		END AS gender,
		Ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
	FROM Silver.crm_cust_info AS ci
	LEFT JOIN Silver.erp_cust_az12 Ca ON ci.cst_key = Ca.cid
	LEFT JOIN Silver.erp_loc_a101 Lo ON ci.cst_key = Lo.cid
	GO

	--=================================================================================
	-- Create dim Table: gold.dim_products
	--================================================================================		
	IF OBJECT_ID ('gold.dim_products', 'v') IS NOT NULL
	DROP VIEW gold.dim_products
	GO
	
	CREATE VIEW gold.dim_products
	AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY prd_start_dt) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date
	FROM Silver.crm_prod_info pn
	LEFT JOIN Silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL -- Filter out all historical data, only keep current.
	GO
   
  --=================================================================================
	-- Create Fact Table: gold.fact_sales
	--=================================================================================
	IF OBJECT_ID ('gold.fact_sales', 'v') IS NOT NULL
	DROP VIEW gold.fact_sales
	GO

	CREATE VIEW gold.fact_sales 
	AS
	SELECT 
		s.sls_ord_num AS order_number,
		p.product_key,
		c.customer_key,
		s.sls_order_dt AS order_date,
		s.sls_ship_dt AS shipping_date,
		s.sls_due_dt AS due_date,
		s.sls_sales AS sales_amount,
		s.sls_quantity AS quantity,
		s.sls_price AS price
	FROM silver.crm_sales_details AS s
	LEFT JOIN gold.dim_products AS p  ON s.sls_prd_key = p.product_number
	LEFT JOIN gold.dim_customers AS c ON s.sls_cust_id = customer_id
	GO
