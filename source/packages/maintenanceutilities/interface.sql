CREATE OR REPLACE PACKAGE maintenanceutilities AUTHID CURRENT_USER AS
/*
 *  Refresh job chain initiation, launching, and cleanup.
 */

	/*
	 *  Build and dispatch the chain of jobs to refresh the data.
	 */
	PROCEDURE dispatchjobs(sectionname VARCHAR2);

	/*
	 *  Build and dispatch a single job to refresh the data.
	 */
	PROCEDURE dispatchjob(tablename VARCHAR2);
	
	/*
	 *  Drop chain and all jobs
	 */
	PROCEDURE dropjobs;

	/*
	 *  Refresh and optimize tables. Validate the table existence to prevent injection
	 *  attacks. Clicks the shutter on a single snapshot by refreshing the materialized view,
	 *  and then calling the compact and analyze procedure. Intented to be used for
	 *  asynchronous job submission.
	 */
	PROCEDURE refreshtable(tablename IN VARCHAR2);
END maintenanceutilities;