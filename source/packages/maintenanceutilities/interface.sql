CREATE OR REPLACE PACKAGE maintenanceutilities AUTHID CURRENT_USER AS

	/*
	 *  Build and dispatch the chain of jobs to refresh the data.
	 */
	PROCEDURE dispatchchain;
	
	/*
	 *  Drop chain and all jobs
	 */
	PROCEDURE dropchain;

	/*
	 *  Refresh and optimize tables. Validate the table existence to prevent injection
	 *  attacks. Clicks the shutter on a single snapshot by refreshing the materialized view,
	 *  and then calling the compact and analyze procedure. Intented to be used for
	 *  asynchronous job submission.
	 */
	PROCEDURE refreshtable(tablename IN VARCHAR2);
END maintenanceutilities;