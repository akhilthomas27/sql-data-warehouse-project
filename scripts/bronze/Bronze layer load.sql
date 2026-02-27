/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- Loading the CRM and ERP csv data to the datawarehouse using BULK INSERT Method
        PRINT ''
        PRINT '================================================================'
        PRINT 'LOADING THE BRONZE LAYER'
        PRINT '================================================================'
        PRINT ''

        -- Loading the CRM data to the datawarehouse
        PRINT '----------------------------------------------------------------'
        PRINT 'LOADING CRM Tables'
        PRINT '----------------------------------------------------------------'
        PRINT ''
        
        -- Loading the cust_info.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/import/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

        SELECT COUNT(*) FROM bronze.crm_cust_info;
        PRINT ''

        -- Loading the prd_info.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/import/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        
        SELECT COUNT(*) FROM bronze.crm_prd_info;
        PRINT ''

        -- Loading the sales_details.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/import/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

        SELECT COUNT(*) FROM bronze.crm_sales_details;
        PRINT ''

        -- Loading the ERP data to the datawarehouse using BULK INSERT Method
        PRINT '----------------------------------------------------------------'
        PRINT 'LOADING ERP Tables'
        PRINT '----------------------------------------------------------------'
        PRINT ''

        -- Loading the CUST_AZ12.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/import/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

        SELECT COUNT(*) FROM bronze.erp_cust_az12;
        PRINT ''

        -- Loading the LOC_A101.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/import/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

        SELECT COUNT(*) FROM bronze.erp_loc_a101;
        PRINT ''

        -- Loading the PX_CAT_G1V2.csv into the datawarehouse
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.px_cat_g1v2';
        TRUNCATE TABLE bronze.px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.px_cat_g1v2';
        BULK INSERT bronze.px_cat_g1v2
        FROM '/var/opt/mssql/import/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

        SELECT COUNT(*) FROM bronze.px_cat_g1v2;
        PRINT ''
        
        SET @batch_end_time = GETDATE();
        PRINT '======================================================='
        PRINT 'Loading Bronze Layer is Completed';
        PRINT ' - Total Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'
        PRINT '======================================================='
    END TRY
    BEGIN CATCH
        PRINT '======================================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================='
    END CATCH
END
