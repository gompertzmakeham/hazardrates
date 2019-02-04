CREATE OR REPLACE PACKAGE surveillanceutilities AS

	/*
	 *  A representation of a single person reduced from events.
	 */
	TYPE inputinterval IS RECORD
	(
		uliabphn INTEGER,
		sex VARCHAR2(1),
		firstnations INTEGER,
		leastbirth DATE,
		greatestbirth DATE,
		leastdeceased DATE,
		greatestdeceased DATE,
		servicestart DATE,
		serviceend DATE,
		surveillancestart DATE,
		surveillanceend DATE,
		surveillancebirth INTEGER,
		surveillancedeceased INTEGER,
		surveillanceimmigrate INTEGER,
		surveillanceemigrate INTEGER,
		censoreddate DATE
	);

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
	 *  Collector object for single person surveillance intervals.
	 */
	TYPE inputintervals IS TABLE OF inputinterval;

	/*
	 *  Collector object for pairs of surveillance extremums.
	 */
	TYPE outputintervals IS TABLE OF outputinterval;

	/*
	 *  An individual person reduced from events.
	 */
	CURSOR generateinput RETURN inputinterval;

	/*
	 *  Generate a pair of surveillance extremum records for each person.
	 */
	FUNCTION generateoutput RETURN outputintervals PIPELINED;	
END surveillanceutilities;