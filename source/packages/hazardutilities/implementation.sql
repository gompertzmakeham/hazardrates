CREATE OR REPLACE PACKAGE BODY hazardutilities AS

	/*
	 *  Partition an event into fiscal years, subpartitioned by the birthday.
	 */
	FUNCTION generatecensus(eventstart IN DATE, eventend IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED DETERMINISTIC AS
		returnfiscal censusinterval;
		returnbirth censusinterval;
		lastfiscal DATE := fiscalstart(eventend);
		localcount INTEGER := 0;
	BEGIN

		-- Assign representation
		returnfiscal.agecoincideinterval := 0;
		returnbirth.agecoincideinterval := 1;

		-- Fiscal interval
		returnfiscal.censusstart := fiscalstart(eventstart);
		returnfiscal.censusend := yearend(returnfiscal.censusstart);
		returnbirth.censusstart := fiscalstart(eventstart);
		returnbirth.censusend := yearend(returnbirth.censusstart);
		
		-- Determine if the birthday interval coincides with the fiscal interval
		CASE birthdate 
			WHEN fiscalstart(birthdate) THEN
				returnfiscal.agecoincidecensus := 1;
				returnbirth.agecoincidecensus := 1;
				returnfiscal.intervalcount := 1 + floor(months_between(lastfiscal, returnfiscal.censusstart) / 12);
				returnbirth.intervalcount := 1 + floor(months_between(lastfiscal, returnbirth.censusstart) / 12);
			ELSE
				returnfiscal.agecoincidecensus := 0;
				returnbirth.agecoincidecensus := 0;
				returnfiscal.intervalcount := 2 * (1 + floor(months_between(lastfiscal, returnfiscal.censusstart) / 12));
				returnbirth.intervalcount := 2 * (1 + floor(months_between(lastfiscal, returnbirth.censusstart) / 12));
		END CASE;

		-- Check for no initial fiscal interval
		CASE
			WHEN yearanniversary(birthdate, returnfiscal.censusstart) <= eventstart THEN
				returnfiscal.intervalcount := returnfiscal.intervalcount - 1;
				returnbirth.intervalcount := returnbirth.intervalcount - 1;
			ELSE
				NULL;
		END CASE;

		-- Check for no final age interval
		CASE
			WHEN eventend < yearanniversary(birthdate, lastfiscal) THEN
				returnfiscal.intervalcount := returnfiscal.intervalcount - 1;
				returnbirth.intervalcount := returnbirth.intervalcount - 1;
			ELSE
				NULL;
		END CASE;

		-- Partition the event
		WHILE returnfiscal.censusstart <= lastfiscal LOOP

			-- Birthday interval
			returnfiscal.agestart := add_months(yearanniversary(birthdate, returnfiscal.censusstart), -12);
			returnfiscal.ageend := yearend(returnfiscal.agestart);
			returnbirth.agestart := yearanniversary(birthdate, returnbirth.censusstart);
			returnbirth.ageend := yearend(returnbirth.agestart);

			-- Intersection of fiscal and age intervals
			returnfiscal.intervalstart := greatest(returnfiscal.censusstart, returnfiscal.agestart);
			returnfiscal.intervalend := least(returnfiscal.censusend, returnfiscal.ageend);
			returnbirth.intervalstart := greatest(returnbirth.censusstart, returnbirth.agestart);
			returnbirth.intervalend := least(returnbirth.censusend, returnbirth.ageend);

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

			-- Assign duration, increment, and send
			CASE
				WHEN returnfiscal.durationstart <= returnfiscal.durationend THEN
					localcount := 1 + localcount;
					returnfiscal.durationdays := 1 + returnfiscal.durationend - returnfiscal.durationstart;
					returnfiscal.intervalorder := localcount;
					PIPE ROW (returnfiscal);
				ELSE
					returnfiscal.durationdays := 0;
					returnfiscal.intervalorder := localcount;
			END CASE;
			CASE
				WHEN returnbirth.durationstart <= returnbirth.durationend THEN
					localcount := 1 + localcount;
					returnbirth.durationdays := 1 + returnbirth.durationend - returnbirth.durationstart;
					returnbirth.intervalorder := localcount;
					PIPE ROW (returnbirth);
				ELSE
					returnbirth.durationdays := 0;
					returnbirth.intervalorder := localcount;
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
	FUNCTION generatecensus(eventdate IN DATE, birthdate IN DATE) RETURN censusintervals PIPELINED DETERMINISTIC AS
		returncensus censusinterval;
		localbirth DATE;
	BEGIN

		-- Fiscal interval
		returncensus.censusstart := fiscalstart(eventdate);
		returncensus.censusend := yearend(returncensus.censusstart);

		-- Order and count
		returncensus.intervalcount := 1;
		returncensus.intervalorder := 1;

		-- Assign the event to an interval
		localbirth := yearanniversary(birthdate, returncensus.censusstart);
		CASE

			-- Birthday interval coincides with the fiscal interval
			WHEN returncensus.censusstart = localbirth THEN
				returncensus.agecoincidecensus := 1;
				returncensus.agecoincideinterval := 1;
				returncensus.agestart := localbirth;

			-- Interval starting on the fiscal year start
			WHEN eventdate < localbirth THEN
				returncensus.agecoincidecensus := 0;
				returncensus.agecoincideinterval := 0;
				returncensus.agestart := add_months(localbirth, -12);

			-- Interval starting on the birthday
			ELSE
				returncensus.agecoincidecensus := 0;
				returncensus.agecoincideinterval := 1;
				returncensus.agestart := localbirth;
		END CASE;

		-- Final interval boundaries
		returncensus.ageend := yearend(returncensus.agestart);
		returncensus.intervalstart := greatest(returncensus.censusstart, returncensus.agestart);
		returncensus.intervalend := least(returncensus.censusend, returncensus.ageend);
		returncensus.durationstart := eventdate;
		returncensus.durationend := eventdate;

		-- Age and days
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
		RETURN cleanphn(cleaninteger(inputphn));
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
		RETURN cleaninpatient(cleaninteger(inputfacility));
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
		RETURN cleanambulatory(cleaninteger(inputfacility));
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
		RETURN cleanprid(cleaninteger(inputprid));
	END cleanprid;

	/*
	 *  Clean a string of all non-numeric characters.
	 */
	FUNCTION cleaninteger(inputstring IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN to_number(regexp_replace(inputstring, '[^0-9]', ''));
	END cleaninteger;
END hazardutilities;