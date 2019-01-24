CREATE OR REPLACE PACKAGE BODY maintenanceutilities AS

	/*
	 *  Current schema name.
	 */
	CURSOR getschema IS
	SELECT 
		UPPER(a0.username) schemaname
	FROM 
		sys.user_users a0
	GROUP BY 
		a0.username;

	/*
	 *  Check to make sure the table exists.
	 */
	CURSOR tableexists(localtable IN VARCHAR2) IS
	SELECT 
		COUNT(a0.table_name) foundcount 
  FROM 
		dual
		LEFT JOIN 
		sys.user_tables a0
		ON
			a0.table_name = localtable;

	/*
	 *  Refresh state of a single materialized view.
	 */
	CURSOR getstate(localtable VARCHAR2) IS
	SELECT 
		a0.staleness 
	FROM 
		sys.user_mviews a0
	WHERE 
		a0.mview_name = localtable;

	/*
	 *  Build and dispatch the chain of jobs to refresh the data.
	 */
	PROCEDURE dispatchchain IS
	BEGIN
		NULL;
	END dispatchchain;
	
	/*
	 *  Drop chain and all jobs
	 */
	PROCEDURE dropchain IS
	BEGIN
		NULL;
	END dropchain;

	/*
	 *  Refresh and optimize tables.
	 *
	 *  Validate the table existence to prevent injection attacks. Clicks the shutter on a
	 *  single snapshot by refreshing the materialized view, and then calling the compact and
	 *  analyze procedure. Intented to be used for asynchronous job submission.
	 */
	PROCEDURE refreshtable(tablename IN VARCHAR2) IS
	PRAGMA AUTONOMOUS_TRANSACTION;
		localschema VARCHAR2(30);
		localtable VARCHAR2(30) := UPPER(tablename);
		localcount INTEGER;
		localquery VARCHAR2(4096);
		localstate VARCHAR2(4096);
	BEGIN

		-- Get the current schema name
		OPEN getschema;
			FETCH getschema INTO localschema;
		CLOSE getschema;

		-- Test for valid table name, to prevent injection attacks
		OPEN tableexists(localtable);
			FETCH tableexists INTO localcount;
		CLOSE tableexists;

		-- Exit if table is not found
		IF localcount <> 1 THEN
			RETURN;
		END IF;

		-- Run parallel
		localquery := 'ALTER SESSION FORCE PARALLEL DDL PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL DML PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8';
		EXECUTE IMMEDIATE localquery;

		-- Preemptively recompile
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' COMPILE';
		EXECUTE IMMEDIATE localquery;

		-- On demand table refresh, all rows
		sys.dbms_snapshot.refresh
		(
			localtable,
			purge_option => 2,
			parallelism => 8,
			method => 'C',
			atomic_refresh => FALSE
		);

		-- Compact the table
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' ENABLE ROW MOVEMENT';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' SHRINK SPACE COMPACT';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' DISABLE ROW MOVEMENT';
		EXECUTE IMMEDIATE localquery;

		-- Recompile after row movement
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' COMPILE';
		EXECUTE IMMEDIATE localquery;

		-- Analyze the table
		sys.dbms_stats.gather_table_stats
		(
			ownname => localschema,
			tabname => localtable,
			estimate_percent => 100,
			degree => 16,
			granularity => 'ALL'
		);

		-- Get the refresh state
		OPEN getstate(localtable);
			FETCH getstate INTO localstate;
		CLOSE getstate;

		-- Force fresh if marked stale
		IF localstate = 'STALE' THEN
			localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' CONSIDER FRESH';
			EXECUTE IMMEDIATE localquery;
		END IF;

		-- Finalize
		COMMIT;
	END refreshtable;
END maintenanceutilities;