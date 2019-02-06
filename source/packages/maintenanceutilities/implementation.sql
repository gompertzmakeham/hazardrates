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
	CURSOR getstate(localtable VARCHAR2) IS
	SELECT 
		a0.staleness 
	FROM 
		sys.user_mviews a0
	WHERE 
		a0.mview_name = localtable;

	/*
	 *  List all the jobs
	 */
	CURSOR getjobs IS
	SELECT 
		a0.job_name jobname
	FROM 
		sys.user_scheduler_jobs a0;

	/*
	 *  List all chains
	 */
	CURSOR getchains IS
	SELECT 
		a0.chain_name chainname
	FROM 
		sys.user_scheduler_chains a0;

	/*
	 *  List all the programs
	 */
	CURSOR getprograms IS
	SELECT 
		a0.job_name programname
	FROM 
		sys.user_scheduler_programs a0;

	CURSOR gettables IS
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveyambulatorycare' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveyannualregistry' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveycontinuingcare' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveyinpatientcare' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveylaboratorycollection' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveypharmacydispense' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveyprimarycare' tablename FROM dual UNION ALL
	SELECT 1 ruleset, 0 dependency, 0 endrule, 'surveyvitalstatistcs' tablename FROM dual UNION ALL
	SELECT 2 ruleset, 1 dependency, 6 endrule, 'personsurveillance' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censusambulatorycare' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censusinpatientcare' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censuslaboratorycollection' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censuslongtermcare' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censuspharmacydispense' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censusprimarycare' tablename FROM dual UNION ALL
	SELECT 3 ruleset, 2 dependency, 0 endrule, 'censussupportiveliving' tablename FROM dual UNION ALL
	SELECT 4 ruleset, 3 dependency, 6 endrule, 'personutilization' tablename FROM dual UNION ALL
	SELECT 5 ruleset, 2 dependency, 6 endrule, 'personcensus' tablename FROM dual;

	/*
	 *  Build and dispatch the chain of jobs to refresh the data.
	 */
	PROCEDURE dispatchchain IS
		TYPE rulesarray IS VARRAY(6) OF VARCHAR2(30);
		localrules rulesarray := rulesarray();
		localchain VARCHAR2(30) := rpad('REFRESH', 22, '0') || TRIM(to_char(MOD(to_number(sys.dbms_scheduler.generate_job_name('')), 4294967296), '0XXXXXXX'));
		localprogram VARCHAR2(30);
	BEGIN

		-- Initialize the chain
		sys.dbms_scheduler.create_chain
		(
			chain_name => localchain,
			comments => 'Refresh hazardrates schema'
		);

		-- Build the chain
		FOR localrow IN gettables LOOP

			--Set the identifier of the job and the step
			localprogram := rpad(substr(localrow.tablename, 1, 22), 22, '0') || TRIM(to_char(MOD(to_number(sys.dbms_scheduler.generate_job_name('')), 4294967296), '0XXXXXXX'));

			-- Initialize the program
			sys.dbms_scheduler.create_program 
			(
				program_name => localprogram,
				program_type => 'STORED_PROCEDURE',
				program_action => 'MAINTENANCEUTILITIES.REFRESHTABLE',
				number_of_arguments => 1,
				enabled => FALSE,
				comments => 'maintenanceutilities.refreshtable(''' || UPPER(localrow.tablename) || ''')'
			);

			-- Set the table name
			sys.dbms_scheduler.define_program_argument
			( 
				program_name => localprogram, 
				argument_position => 1,
				argument_name => 'tablename',
				default_value => localrow.tablename
			);

			-- Enable the program
			sys.dbms_scheduler.enable(localprogram);

			-- Wrap the program in a step
			sys.dbms_scheduler.define_chain_step
			(
				chain_name => localchain,
				step_name => localprogram,
				program_name => localprogram
			);

			-- Extend the depdency rules
			localrules(localrow.ruleset) := localrules(localrow.ruleset) || 'SUCCEEDED AND ';
			
			-- End conditions
			CASE localrow.endrule
				WHEN 0 THEN
					NULL;
				ELSE
					localrules(localrow.endrule) := localrules(localrow.endrule) || 'SUCCEEDED AND ';
			END CASE;
		END LOOP;

		-- Loop again building the rules
		FOR localrow IN gettables LOOP
			
			-- Start conditions
			CASE localrow.dependency
				WHEN 0 THEN
					sys.dbms_scheduler.define_chain_rule
					(
						chain_name => localchain,
						condition => 'TRUE'
					);
				ELSE
					NULL;
			END CASE;
		END LOOP;

		-- Enable the chain
		sys.dbms_scheduler.enable(localchain);

		-- Initialize the job
		sys.dbms_scheduler.create_job 
		(
			job_name => localchain,
			job_type => 'CHAIN',
			job_action => localchain,
			enabled => FALSE,
			auto_drop => FALSE,
			comments => 'Refresh hazardrates schema'
		);

		-- Log all activity
		sys.dbms_scheduler.set_attribute
		( 
			name => localchain, 
			attribute => 'logging_level', 
			value => sys.dbms_scheduler.logging_full
		);
		
		-- Run the job
		sys.dbms_scheduler.enable(name => localchain);
	END dispatchchain;
	
	/*
	 *  Drop chain in reverse dependency order
	 */
	PROCEDURE dropchain IS
	BEGIN
		FOR localrow IN getjobs LOOP
			sys.dbms_scheduler.drop_job
			(
				job_name => localrow.jobname,
				force => TRUE
			);
		END LOOP;
		FOR localrow IN getchains LOOP
			sys.dbms_scheduler.drop_chain
			(
				chain_name => localrow.chainname,
				force => TRUE
			);
		END LOOP;
		FOR localrow IN getprograms LOOP
			sys.dbms_scheduler.drop_program
			(
				program_name => localrow.programname,
				force => TRUE
			);
		END LOOP;
	END dropchain;

	/*
	 *  Refresh and optimize tables. Validate the table existence to prevent injection
	 *  attacks. Clicks the shutter on a single snapshot by refreshing the materialized view,
	 *  and then calling the compact and analyze procedure. Intented to be used for
	 *  asynchronous job submission.
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
			degree => 8,
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