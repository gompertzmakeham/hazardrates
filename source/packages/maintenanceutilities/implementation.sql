CREATE OR REPLACE PACKAGE BODY maintenanceutilities AS
/*
 *  Refresh job chain initiation, launching, and cleanup.
 */

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
	CURSOR getstate(localschema VARCHAR2, localtable VARCHAR2) IS
	SELECT 
		UPPER(a0.staleness) staleness 
	FROM 
		sys.user_mviews a0
	WHERE
		a0.owner = localschema
		AND
		a0.mview_name = localtable;

	/*
	 *  List all the jobs
	 */
	CURSOR getjobs IS
	SELECT 
		UPPER(a0.job_name) jobname
	FROM 
		sys.user_scheduler_jobs a0;

	CURSOR gettables(localsection VARCHAR2) IS
	WITH
		tablelist AS
		(
			SELECT 'surveillance' sectionname, 'surveyambulatorycare' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveyannualregistry' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveycontinuingcare' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveyinpatientcare' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveylaboratorycollection' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveypharmacydispense' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveyprimarycare' tablename FROM dual UNION ALL
			SELECT 'surveillance' sectionname, 'surveyvitalstatistics' tablename FROM dual UNION ALL
			SELECT 'demographics' sectionname, 'persondemographic' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censusambulatorycare' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censusinpatientcare' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censuslaboratorycollection' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censuslongtermcare' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censuspharmacydispense' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censusprimarycare' tablename FROM dual UNION ALL
			SELECT 'census' sectionname, 'censussupportiveliving' tablename FROM dual UNION ALL
			SELECT 'utilization' sectionname, 'personutilization' tablename FROM dual
		)
	SELECT
		UPPER(a0.tablename) tablename
	FROM
		tablelist a0
	WHERE
		a0.sectionname = localsection
		OR
		a0.tablename = localsection;

	/*
	 *  Build and dispatch asynchronous jobs to refresh the data.
	 */
	PROCEDURE dispatchjobs(sectionname VARCHAR2) IS
	BEGIN
		FOR localrow IN gettables(sectionname) LOOP
			dispatchjob(localrow.tablename);
		END LOOP;
	END dispatchjobs;

	/*
	 *  Build and dispatch a single job to refresh the data.
	 */
	PROCEDURE dispatchjob(tablename VARCHAR2) IS
		localjob VARCHAR2(30) := rpad(substr(tablename, 1, 22), 22, '0') || TRIM(to_char(MOD(to_number(sys.dbms_scheduler.generate_job_name('')), 4294967296), '0XXXXXXX'));
	BEGIN

		-- Initialize the job
		sys.dbms_scheduler.create_job
		(
			job_name => localjob,
			job_type => 'STORED_PROCEDURE',
			job_action => 'MAINTENANCEUTILITIES.REFRESHTABLE',
			number_of_arguments => 1,
			enabled => FALSE,
			auto_drop => FALSE,
			comments => 'MAINTENANCEUTILITIES.REFRESHTABLE(''' || tablename || ''')'
		);

		-- Set the table name
		sys.dbms_scheduler.set_job_argument_value
		( 
			job_name => localjob, 
			argument_position => 1,
			argument_value => tablename
		);

		-- Log all activity
		sys.dbms_scheduler.set_attribute
		( 
			name => localjob, 
			attribute => 'logging_level', 
			value => sys.dbms_scheduler.logging_full
		);

		-- Run the job
		sys.dbms_scheduler.enable(name => localjob);
	END dispatchjob;

	/*
	 *  Drop jobs
	 */
	PROCEDURE dropjobs IS
	BEGIN
		FOR localrow IN getjobs LOOP
			sys.dbms_scheduler.drop_job
			(
				job_name => localrow.jobname,
				force => TRUE
			);
		END LOOP;
	END dropjobs;

	/*
	 *  Refresh and optimize tables. Validate the table existence to prevent injection
	 *  attacks. Clicks the shutter on a single snapshot by refreshing the materialized view,
	 *  and then calling the compact and analyze procedure. Intented to be used for
	 *  asynchronous job submission.
	 */
	PROCEDURE refreshtable(tablename IN VARCHAR2) IS
		PRAGMA AUTONOMOUS_TRANSACTION;
		localschema VARCHAR2(30);
		localtable VARCHAR2(61);
		localcount INTEGER;
		localquery VARCHAR2(4096);
		localstate VARCHAR2(4096);
	BEGIN

		-- Get the current schema name
		OPEN getschema;
			FETCH getschema INTO localschema;
		CLOSE getschema;

		-- Test for valid table name, to prevent injection attacks
		OPEN tableexists(tablename);
			FETCH tableexists INTO localcount;
		CLOSE tableexists;

		-- Exit if table is not found
		CASE localcount
			WHEN 1 THEN
				localtable := localschema || '.' || tablename;
			ELSE
				RETURN;
		END CASE;

		-- Preemptively recompile
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' COMPILE';
		EXECUTE IMMEDIATE localquery;
		COMMIT;

		-- Run parallel
		localquery := 'ALTER SESSION FORCE PARALLEL DDL PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL DML PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8';
		EXECUTE IMMEDIATE localquery;

		-- On demand table refresh, all rows
		sys.dbms_snapshot.refresh
		(
			localtable,
			purge_option => 2,
			refresh_after_errors => TRUE,
			parallelism => 8,
			method => 'C',
			atomic_refresh => FALSE
		);
		COMMIT;

		-- Run parallel
		localquery := 'ALTER SESSION FORCE PARALLEL DDL PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL DML PARALLEL 8';
		EXECUTE IMMEDIATE localquery;
		localquery := 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8';
		EXECUTE IMMEDIATE localquery;

		-- Analyze the table
		sys.dbms_stats.gather_table_stats
		(
			ownname => localschema,
			tabname => tablename,
			estimate_percent => 100,
			degree => 8,
			granularity => 'ALL'
		);
		COMMIT;

		-- Get the refresh state
		OPEN getstate(localschema, tablename);
			FETCH getstate INTO localstate;
		CLOSE getstate;

		-- Force fresh if marked stale
		IF localstate = 'STALE' THEN
			localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' CONSIDER FRESH';
			EXECUTE IMMEDIATE localquery;
		END IF;
		COMMIT;

		-- Final compile recompile
		localquery := 'ALTER MATERIALIZED VIEW ' || localtable || ' COMPILE';
		EXECUTE IMMEDIATE localquery;
		COMMIT;
	END refreshtable;
END maintenanceutilities;