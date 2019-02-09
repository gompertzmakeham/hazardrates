CREATE OR REPLACE PACKAGE surveillanceutilities AS
/*
 *  Generate extremum surveillance intervals.
 */

	/*
	 *  A pair of rows representing the surveillance extremums of birth and deceased dates.
	 */
	TYPE outputinterval IS RECORD
	(
		uliabphn INTEGER,
		sex VARCHAR2(1),
		firstnations INTEGER,
		cornercase VARCHAR2(1),
		birthdateequipoise INTEGER,
		deceaseddateequipoise INTEGER,
		birthequipoise INTEGER,
		deceasedequipoise INTEGER,
		immigrateequipoise INTEGER,
		emigrateequipoise INTEGER,
		startequipoise INTEGER,
		endequipoise INTEGER,
		ageequipoise INTEGER,
		birthdate DATE,
		deceaseddate DATE,
		surveillancestart DATE,
		surveillanceend DATE,
		extremumstart DATE,
		extremumend DATE,
		surveillancebirth INTEGER,
		surveillancedeceased INTEGER,
		surveillanceimmigrate INTEGER,
		surveillanceemigrate INTEGER,
		censoreddate DATE
	);

	/*
	 *  Collector object for pairs of surveillance extremums.
	 */
	TYPE outputintervals IS TABLE OF outputinterval;

	/*
	 *  Generate a pair of surveillance extremum records for each person.
	 */
	FUNCTION generateoutput RETURN outputintervals PIPELINED;	
END surveillanceutilities;