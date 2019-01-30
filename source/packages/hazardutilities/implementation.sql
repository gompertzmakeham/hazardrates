CREATE OR REPLACE PACKAGE BODY hazardutilities AS

	/*
	 *  Partition an event into fiscal years, subpartitioned by the birthday.
	 */
	FUNCTION generatecensus(eventstart IN DATE, eventend IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED AS
		returnfiscal censusinterval;
		returnbirth censusinterval;
		lastfiscal DATE := fiscalstart(eventend);
	BEGIN

		-- Assign representation
		returnfiscal.ageinterval := 0;
		returnbirth.ageinterval := 1;

		-- Determine if the birthday interval coincides with the fiscal interval
		CASE birthdate 
			WHEN fiscalstart(birthdate) THEN
				returnfiscal.agecensus := 1;
				returnbirth.agecensus := 1;
			ELSE
				returnfiscal.agecensus := 0;
				returnbirth.agecensus := 0;
		END CASE;

		-- Fiscal interval
		returnfiscal.censusstart := fiscalstart(eventstart);
		returnfiscal.censusend := yearend(returnfiscal.censusstart);
		returnbirth.censusstart := fiscalstart(eventstart);
		returnbirth.censusend := yearend(returnbirth.censusstart);

		-- Partition the event
		WHILE returnfiscal.censusstart <= lastfiscal LOOP

			-- Birthday interval
			returnfiscal.agestart := yearanniversary(birthdate, returnfiscal.censusstart);
			returnfiscal.ageend := yearend(returnfiscal.agestart);
			returnbirth.agestart := yearanniversary(birthdate, returnbirth.censusstart);
			returnbirth.ageend := yearend(returnbirth.agestart);

			-- Interval starting on the fiscal year start
			returnfiscal.intervalstart := returnfiscal.censusstart;
			returnfiscal.intervalend := returnfiscal.agestart - 1;

			-- Interval starting on the birthday
			returnbirth.intervalstart := returnbirth.agestart;
			returnbirth.intervalend := returnbirth.censusend;

			-- Duration within the interval
			returnfiscal.durationstart := greatest(eventstart, returnfiscal.intervalstart);
			returnfiscal.durationend := least(eventend, returnfiscal.intervalend);
			returnbirth.durationstart := greatest(eventstart, returnbirth.intervalstart);
			returnbirth.durationend := least(eventend, returnbirth.intervalend);

			-- Age at interval start
			returnfiscal.intervalage := ageyears(birthdate, returnfiscal.intervalstart);
			returnbirth.intervalage := ageyears(birthdate, returnbirth.intervalstart);

			-- Fiscal start and event start flag
			CASE returnfiscal.durationstart
				WHEN eventstart THEN
					returnfiscal.evententry := 1;
				ELSE
					returnfiscal.evententry := 0;
			END CASE;
			
			-- Birth start and event start flag
			CASE returnbirth.durationstart
				WHEN eventstart THEN
					returnbirth.evententry := 1;
				ELSE
					returnbirth.evententry := 0;
			END CASE;

			-- Fiscal end and event end flag
			CASE returnfiscal.durationend
				WHEN eventend THEN
					returnfiscal.eventexit := 1;
				ELSE
					returnfiscal.eventexit := 0;
			END CASE;
			
			-- Birth end and event end flag
			CASE returnbirth.durationend
				WHEN eventend THEN
					returnbirth.eventexit := 1;
				ELSE
					returnbirth.eventexit := 0;
			END CASE;

			-- Assign duration and send
			CASE
				WHEN returnfiscal.durationstart <= returnfiscal.durationend THEN
					returnfiscal.durationdays := 1 + returnfiscal.durationend - returnfiscal.durationstart;
					PIPE ROW (returnfiscal);
				ELSE
					returnfiscal.durationdays := 0;
			END CASE;
			CASE
				WHEN returnbirth.durationstart <= returnbirth.durationend THEN
					returnbirth.durationdays := 1 + returnbirth.durationend - returnbirth.durationstart;
					PIPE ROW (returnbirth);
				ELSE
					returnbirth.durationdays := 0;
			END CASE;

			-- Increment fiscal interval
			returnfiscal.censusstart := add_months(returnfiscal.censusstart, 12);
			returnfiscal.censusend := add_months(returnfiscal.censusend, 12);
			returnbirth.censusstart := add_months(returnbirth.censusstart, 12);
			returnbirth.censusend := add_months(returnbirth.censusend, 12);
		END LOOP;
		RETURN;
	END generatecensus;

	/*
	 *  Truncate an event into fiscal years, subpartitioned by the birthday.
	 */
	FUNCTION generatecensus(eventdate IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED AS
		returncensus censusinterval;
	BEGIN

		-- Fiscal interval
		returncensus.censusstart := fiscalstart(eventdate);
		returncensus.censusend := yearend(returncensus.censusstart);

		-- Birthday interval
		returncensus.agestart := yearanniversary(birthdate, returncensus.censusstart);
		returncensus.ageend := yearend(returncensus.agestart);

		-- Assign the event to an interval
		CASE

			-- Birthday interval coincides with the fiscal interval
			WHEN returncensus.censusstart = returncensus.agestart THEN
				returncensus.agecensus := 1;
				returncensus.ageinterval := 1;
				returncensus.intervalstart := returncensus.censusstart;
				returncensus.intervalend := returncensus.censusend;

			-- Interval starting on the fiscal year start
			WHEN eventdate < returncensus.agestart THEN
				returncensus.agecensus := 0;
				returncensus.ageinterval := 0;
				returncensus.intervalstart := returncensus.censusstart;
				returncensus.intervalend := returncensus.agestart - 1;

			-- Interval starting on the birthday
			ELSE
				returncensus.agecensus := 0;
				returncensus.ageinterval := 1;
				returncensus.intervalstart := returncensus.agestart;
				returncensus.intervalend := returncensus.censusend;
		END CASE;

		-- Age and days
		returncensus.durationstart := eventdate;
		returncensus.durationend := eventdate;
		returncensus.intervalage := ageyears(birthdate, returncensus.intervalstart);
		returncensus.durationdays := 1;
		returncensus.evententry := 1;
		returncensus.eventexit := 1;

		-- Send
		PIPE ROW (returncensus);
		RETURN;
	END generatecensus;

	/*
	 *  Lower truncated years between start date and end date.
	 */
	FUNCTION ageyears(startdate IN DATE, enddate IN DATE) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN floor(months_between(enddate, startdate) / 12);
	END ageyears;

	/*
	 *  Last day of the year starting on the date.
	 */
	FUNCTION yearend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(inputdate, 12) - 1;
	END yearend;

	/*
	 *  The anniversary of the start date in the year following the end date.
	 */
	FUNCTION yearanniversary(startdate IN DATE, enddate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(startdate, 12 * ceil(months_between(enddate, startdate) / 12));
	END yearanniversary;

	/*
	 *  Truncate a date to the start of the fiscal year, the preceding April 1.
	 */
	FUNCTION fiscalstart(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(TRUNC(add_months(inputdate, -3), 'yyyy'), 3);
	END fiscalstart;

	/*
	 *  Truncate a date to the end of the fiscal year, the following March 31.
	 */
	FUNCTION fiscalend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN yearend(fiscalstart(inputdate));
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring, formatmodel));
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring, formatmodel));
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring));
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring));
	END fiscalend;

	/*
	 *  Truncate a date to the start of the calendar year, the preceding January 1.
	 */
	FUNCTION calendarstart(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(inputdate, 'yyyy');
	END calendarstart;

	/*
	 *  Truncate a date to the end of the calendar year, the following December 31.
	 */
	FUNCTION calendarend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN yearend(calendarstart(inputdate));
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring, formatmodel));
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring, formatmodel));
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring));
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring));
	END calendarend;

	/*
	 *  Try to convert a string to a date according to the format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
		returndate DATE;
	BEGIN
		returndate := TO_DATE(datestring, formatmodel);
		RETURN returndate;
	EXCEPTION
		WHEN OTHERS THEN
			returndate := NULL;
			RETURN returndate;
	END cleandate;

	/*
	 *  Try to convert a string to a date using a default format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN cleandate(datestring, 'YYYYMMDD');
	END cleandate;

	/*
	 *  Check for a minimally plausible Alberta provincial healthcare number, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanphn(inputphn IN INTEGER) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		CASE
			WHEN inputphn BETWEEN 100000000 AND 999999999 THEN
				RETURN inputphn;
			ELSE
				RETURN NULL;
		END CASE;
	END cleanphn;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial healthcare number, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanphn(inputphn IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanphn(to_number(regexp_replace(inputphn, '[^0-9]', '')));
	END cleanphn;

	/*
	 *  For fields intended to indicate biological sex, not self identified gender, clean all
	 *  characters not indicating either female or male.
	 */
	FUNCTION cleansex(inputsex IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC AS
	BEGIN
		RETURN regexp_substr(UPPER(inputsex), '[FM]', 1, 1, 'i');
	END cleansex;
	
	/*
	 *  Ensure the inpatient care facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient(inputfacility IN INTEGER) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		CASE
			WHEN inputfacility BETWEEN 80000 AND 80999 THEN
				RETURN inputfacility;
			ELSE
				RETURN NULL;
		END CASE;
	END cleaninpatient;
	
	/*
	 *  Convert to number and ensure the inpatient facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient(inputfacility IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleaninpatient(to_number(COALESCE(regexp_replace(inputfacility, '[^0-9]', ''), '0')));
	END cleaninpatient;
	
	/*
	 *  Ensure the ambulatory care facility number is between 88000 and 88999.
	 */
	FUNCTION cleanambulatory(inputfacility IN INTEGER) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		CASE
			WHEN inputfacility BETWEEN 88000 AND 88999 THEN
				RETURN inputfacility;
			ELSE
				RETURN NULL;
		END CASE;
	END cleanambulatory;
	
	/*
	 *  Convert to number ensure the ambulatory care facility number is between 88000 and 
	 *  88999.
	 */
	FUNCTION cleanambulatory(inputfacility IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanambulatory(to_number(COALESCE(regexp_replace(inputfacility, '[^0-9]', ''), '0')));
	END cleanambulatory;

	/*
	 *  Check for a minimally plausible Alberta provincial provider identifier, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanprid(inputprid IN INTEGER) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		CASE
			WHEN inputprid BETWEEN 100000000 AND 999999999 THEN
				RETURN inputprid;
			ELSE
				RETURN NULL;
		END CASE;
	END cleanprid;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial provider identifier, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanprid(inputprid IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanprid(to_number(regexp_replace(inputprid, '[^0-9]', '')));
	END cleanprid;
END hazardutilities;