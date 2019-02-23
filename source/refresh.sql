-- Step 0: Clean up determinitic utilities
ALTER PACKAGE hazardutilities COMPILE PACKAGE
	PLSQL_OPTIMIZE_LEVEL = 3
	PLSQL_CODE_TYPE = NATIVE
	PLSQL_DEBUG = FALSE;
	
-- Step 1: Clean up automation utilities
ALTER PACKAGE maintenanceutilities COMPILE PACKAGE
	PLSQL_OPTIMIZE_LEVEL = 3
	PLSQL_CODE_TYPE = NATIVE
	PLSQL_DEBUG = FALSE;

-- Step 2: Refresh person demographic sources
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dispatchjobs('demographics');
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;

-- Step 3: Refresh person surveillance sources
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dispatchjobs('surveillance');
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;

-- Step 4: Refresh person surveillance extremums
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dispatchjobs('extremums');
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;

-- Step 5: Refresh person fiscal utilization
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dispatchjobs('utilization');
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;

-- Step 6: Refresh person fiscal census
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dispatchjobs('census');
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;

-- Step 7: Clean up
SET SERVEROUTPUT ON;
BEGIN
	maintenanceutilities.dropjobs;
EXCEPTION
	WHEN OTHERS THEN
		sys.dbms_output.put_line(SQLERRM);
END;