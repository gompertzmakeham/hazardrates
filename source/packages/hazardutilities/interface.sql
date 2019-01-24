CREATE OR REPLACE PACKAGE hazardutilities AS

	/*
	 *  A single census interval division of an event.
	 */
	TYPE censusinterval IS RECORD
	(
		censusstart DATE,
		censusend DATE,
		agestart DATE,
		ageend DATE,
		intervalstart DATE,
		intervalend DATE,
		durationstart DATE,
		durationend DATE,
		durationdays INTEGER,
		intervalage INTEGER,
		ageinterval INTEGER,
		agecensus INTEGER,
		evententry INTEGER,
		eventexit INTEGER
	);

	/*
	 *  A collection of census intervals partitioning an event by fiscal year and birthday
	 */
	TYPE censusintervals IS TABLE OF censusinterval;

	/*
	 *  Partition an event into fiscal years, subpartitioned by the birthday.
	 */
	FUNCTION generatecensus(eventstart IN DATE, eventend IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED;

	/*
	 *  Truncate an event into fiscal years, subpartitioned by the birthday.
	 */
	FUNCTION generatecensus(eventdate IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED;

	/*
	 *  Lower truncated years between start date and end date.
	 */
	FUNCTION ageyears(startdate IN DATE, enddate IN DATE) RETURN INTEGER DETERMINISTIC;

	/*
	 *  Last day of the year starting on the date.
	 */
	FUNCTION yearend(inputdate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  The anniversary of the start date in the year following the end date.
	 */
	FUNCTION yearanniversary(startdate IN DATE, enddate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the fiscal year, the preceding April 1.
	 */
	FUNCTION fiscalstart(inputdate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the fiscal year, the following March 31.
	 */
	FUNCTION fiscalend(inputdate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the fiscal year given the date as string.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the fiscal year given the date as string.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the calendar year, the preceding January 1.
	 */
	FUNCTION calendarstart(inputdate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the calendar year, the following December 31.
	 */
	FUNCTION calendarend(inputdate IN DATE) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the calendar year given the date as string.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the calendar year given the date as string.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Try to convert a string to a date according to the format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Try to convert a string to a date using a default format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC;

	/*
	 *  Check for a minimally plausible Alberta provincial healthcare number, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanphn(inputphn IN INTEGER) RETURN INTEGER DETERMINISTIC;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial healthcare number, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanphn(inputphn IN VARCHAR2) RETURN INTEGER DETERMINISTIC;

	/*
	 *  For fields intended to indicate biological sex, not self identified gender, clean all
	 *  characters not indicating either female or male.
	 */
	FUNCTION cleansex(inputsex IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
	
	/*
	 *  Ensure the inpatient care facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient(inputfacility IN INTEGER) RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Convert to number and ensure the inpatient facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient(inputfacility IN VARCHAR2) RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Ensure the ambulatory care facility number is between 88000 and 88999.
	 */
	FUNCTION cleanambulatory(inputfacility IN INTEGER) RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Convert to number ensure the ambulatory care facility number is between 88000 and 
	 *  88999.
	 */
	FUNCTION cleanambulatory(inputfacility IN VARCHAR2) RETURN INTEGER DETERMINISTIC;
END hazardutilities;