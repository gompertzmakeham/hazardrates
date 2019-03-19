CREATE OR REPLACE PACKAGE hazardutilities AS
/*
 *  Deterministic data processing functions.
 */

	/*
	 *  A single demographic interval of a person.
	 */
	TYPE demographicinterval IS RECORD
	(
		leastbirth DATE,
		greatestbirth DATE,
		leastdeceased DATE,
		greatestdeceased DATE,
		leastimmigrate DATE,
		greatestimmigrate DATE,
		leastemigrate DATE,
		greatestemigrate DATE,
		surveillancestart DATE,
		surveillanceend DATE,
		birthdateequipoise INTEGER,
		deceaseddateequipoise INTEGER,
		birthobservationequipoise INTEGER,
		deceasedobservationequipoise INTEGER,
		immigratedateequipoise INTEGER,
		emigratedateequipoise INTEGER,
		immigrateobservationequipoise INTEGER,
		emigrateobservationequipoise INTEGER,
		surveillancestartequipoise INTEGER,
		surveillanceendequipoise INTEGER,
		ageequipoise INTEGER
	);

	/*
	 *  A collection of demographic intervals of persons.
	 */
	TYPE demographicintervals IS TABLE OF demographicinterval;

	/*
	 *  A single extremum surveillance interval of a person.
	 */
	TYPE surveillancecinterval IS RECORD
	(
		cornercase VARCHAR2(1),
		birthdate DATE,
		deceaseddate DATE,
		immigratedate DATE,
		emigratedate DATE,
		extremumstart DATE,
		extremumend DATE
	);

	/*
	 *  A collection of extremum surveillance intervals of persons.
	 */
	TYPE surveillancecintervals IS TABLE OF surveillancecinterval;

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
		agecoincideinterval INTEGER,
		agecoincidecensus INTEGER,
		intervalage INTEGER,
		intervalfirst INTEGER,
		intervallast INTEGER,
		intervalcount INTEGER,
		intervalorder INTEGER
	);

	/*
	 *  A collection of census intervals partitioning an event by fiscal year and birthday.
	 */
	TYPE censusintervals IS TABLE OF censusinterval;

	/*
	 *  A single measure in a single census interval.
	 */
	TYPE censusmeasure IS RECORD
	(
		measurevalue INTEGER,
		measureidentifier VARCHAR2(32),
		measuredescription VARCHAR2(1024)
	);

	/*
	 * A collection of census measures for a census interval.
	 */
	TYPE censusmeasures IS TABLE OF censusmeasure;

	/*
	 *  Map event data extremums of each person to a demographic interval.
	 */
	FUNCTION generatedemographic
	(
		leastbirth IN DATE,
		greatestbirth IN DATE,
		leastdeceased IN DATE,
		greatestdeceased IN DATE,
		leastservice IN DATE,
		greatestservice IN DATE,
		leastsurveillancestart IN DATE,
		leastsurveillanceend IN DATE,
		greatestsurveillancestart IN DATE,
		greatestsurveillanceend IN DATE,
		surveillancebirth IN INTEGER,
		surveillancedeceased IN INTEGER,
		surveillanceimmigrate IN INTEGER,
		surveillanceemigrate IN INTEGER
	)
	RETURN demographicintervals PIPELINED DETERMINISTIC;

	/*
	 *  Map the demographic interval of each person to a pair of surveillance extremums.
	 */
	FUNCTION generatesurveillance
	(
		leastbirth IN DATE,
		greatestbirth IN DATE,
		leastdeceased IN DATE,
		greatestdeceased IN DATE,
		leastimmigrate IN DATE,
		greatestimmigrate IN DATE,
		leastemigrate IN DATE,
		greatestemigrate IN DATE,
		surveillancestart IN DATE,
		surveillanceend IN DATE
	)
	RETURN surveillancecintervals PIPELINED DETERMINISTIC;

	/*
	 *  Chidi Anagonye's Time Knife. Partition an event into fiscal years, subpartitioned by
	 *  the birthday.
	 */
	FUNCTION generatecensus
	(
		eventstart IN DATE, 
		eventend IN DATE,
		birthdate IN DATE
	)
	RETURN censusintervals PIPELINED DETERMINISTIC;

	/*
	 *  Chidi Anagonye's Time Knife. Truncate an event into fiscal years, subpartitioned by
	 *  the birthday.
	 */
	FUNCTION generatecensus
	(
		eventdate IN DATE,
		birthdate IN DATE
	)
	RETURN censusintervals PIPELINED DETERMINISTIC;

	/*
	 *  Pivot a census utilization record to a columnar list of measures.
	 */
	FUNCTION generatemeasures
	(
		ambulatoryminutes IN INTEGER,
		ambulatoryvisits IN INTEGER,
		ambulatorysitedays IN INTEGER,
		ambulatorydays IN INTEGER,
		inpatientdays IN INTEGER,
		inpatientadmissions IN INTEGER,
		inpatientdischarges IN INTEGER,
		inpatientstays IN INTEGER,
		caremanagerdays IN INTEGER,
		caremanagerallocations IN INTEGER,
		caremanagerreleases IN INTEGER,
		caremanagers IN INTEGER,
		homecareactivities IN INTEGER,
		homecarevisits IN INTEGER,
		homecaredays IN INTEGER,
		laboratoryassays IN INTEGER,
		laboratorysitedays IN INTEGER,
		laboratorydays IN INTEGER,
		longtermcaredays IN INTEGER,
		longtermcareadmissions IN INTEGER,
		longtermcaredischarges IN INTEGER,
		longtermcarestays IN INTEGER,
		pharmacystandarddailydoses IN INTEGER,
		pharmacycontrolleddailydoses IN INTEGER,
		pharmacydailydoses IN INTEGER,
		pharmacystandardtherapeutics IN INTEGER,
		pharmacycontrolledtherapeutics IN INTEGER,
		pharmacytherapeutics IN INTEGER,
		pharmacystandardsitedays IN INTEGER,
		pharmacycontrolledsitedays IN INTEGER,
		pharmacysitedays IN INTEGER,
		pharmacystandarddays IN INTEGER,
		pharmacycontrolleddays IN INTEGER,
		pharmacydays IN INTEGER,
		anesthesiologyprocedures IN INTEGER,
		consultprocedures IN INTEGER,
		generalpracticeprocedures IN INTEGER,
		obstetricprocedures IN INTEGER,
		pathologyprocedures IN INTEGER,
		radiologyprocedures IN INTEGER,
		specialtyprocedures IN INTEGER,
		surgicalprocedures IN INTEGER,
		primarycareprocedures IN INTEGER,
		anesthesiologistsdays IN INTEGER,
		consultprovidersdays IN INTEGER,
		generalpractitionersdays IN INTEGER,
		obstetriciansdays IN INTEGER,
		pathologistsdays IN INTEGER,
		radiologistsdays IN INTEGER,
		specialistsdays IN INTEGER,
		surgeonsdays IN INTEGER,
		primarycareproviderdays IN INTEGER,
		anesthesiologydays IN INTEGER,
		consultdays IN INTEGER,
		generalpracticedays IN INTEGER,
		obstetricdays IN INTEGER,
		pathologydays IN INTEGER,
		radiologydays IN INTEGER,
		specialtydays IN INTEGER,
		surgerydays IN INTEGER,
		primarycaredays IN INTEGER,
		supportivelivingdays IN INTEGER,
		supportivelivingadmissions IN INTEGER,
		supportivelivingdischarges IN INTEGER,
		supportivelivingstays IN INTEGER
	)
	RETURN censusmeasures PIPELINED DETERMINISTIC;

	/*
	 *  Lower truncated years between start date and end date.
	 */
	FUNCTION ageyears
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  The start of the anniversary year of the start date that the end date falls in.
	 */
	FUNCTION anniversarystart
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  The end of the anniversary year of the start date that the end date falls in.
	 */
	FUNCTION anniversaryend
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the fiscal year, the preceding April 1.
	 */
	FUNCTION fiscalstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the fiscal year, the following March 31.
	 */
	FUNCTION fiscalend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the fiscal year given the date as string.
	 */
	FUNCTION fiscalstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the fiscal year given the date as string.
	 */
	FUNCTION fiscalend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the calendar year, the preceding January 1.
	 */
	FUNCTION calendarstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the calendar year, the following December 31.
	 */
	FUNCTION calendarend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the calendar year given the date as string.
	 */
	FUNCTION calendarstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the calendar year given the date as string.
	 */
	FUNCTION calendarend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the quarter.
	 */
	FUNCTION quarterstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the quarter.
	 */
	FUNCTION quarterend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the quarter given the date as string.
	 */
	FUNCTION quarterstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the quarter given the date as string.
	 */
	FUNCTION quarterend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the quarter given the date as string, and default format.
	 */
	FUNCTION quarterstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the quarter given the date as string, and default format.
	 */
	FUNCTION quarterend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the month.
	 */
	FUNCTION monthstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the month.
	 */
	FUNCTION monthend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the month given the date as string.
	 */
	FUNCTION monthstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the month given the date as string.
	 */
	FUNCTION monthend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the month given the date as string, and default format.
	 */
	FUNCTION monthstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the month given the date as string, and default format.
	 */
	FUNCTION monthend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the start of the week.
	 */
	FUNCTION weekstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Truncate a date to the end of the week.
	 */
	FUNCTION weekend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the week given the date as string.
	 */
	FUNCTION weekstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the week given the date as string.
	 */
	FUNCTION weekend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the start of the week given the date as string, and default format.
	 */
	FUNCTION weekstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Return the end of the week given the date as string, and default format.
	 */
	FUNCTION weekend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Try to convert a string to a date according to the format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Try to convert a string to a date using a default format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC;

	/*
	 *  Check for a minimally plausible Alberta provincial healthcare number, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanphn
	(
		inputphn IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial healthcare number, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanphn
	(
		inputphn IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  For fields intended to indicate biological sex, not self identified gender, clean all
	 *  characters not indicating either female or male.
	 */
	FUNCTION cleansex
	(
		inputsex IN VARCHAR2
	)
	RETURN VARCHAR2 DETERMINISTIC;
	
	/*
	 *  Ensure the inpatient care facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient
	(
		inputfacility IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Convert to number and ensure the inpatient facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient
	(
		inputfacility IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Ensure the ambulatory care facility number is between 88000 and 88999.
	 */
	FUNCTION cleanambulatory
	(
		inputfacility IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC;
	
	/*
	 *  Convert to number ensure the ambulatory care facility number is between 88000 and 
	 *  88999.
	 */
	FUNCTION cleanambulatory
	(
		inputfacility IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  Check for a minimally plausible Alberta provincial provider identifier, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanprid
	(
		inputprid IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial provider identifier, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanprid
	(
		inputprid IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC;

	/*
	 *  Clean a string of all non-numeric characters.
	 */
	FUNCTION cleaninteger
	(
		inputstring IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC;
END hazardutilities;