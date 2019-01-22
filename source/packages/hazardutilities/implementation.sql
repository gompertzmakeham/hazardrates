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

			-- Assign duration
			CASE
				WHEN returnfiscal.durationstart <= returnfiscal.durationend THEN
					returnfiscal.durationdays := 1 + returnfiscal.durationend - returnfiscal.durationstart;
				ELSE
					returnfiscal.durationdays := 0;
			END CASE;
			CASE
				WHEN returnbirth.intervalstart <= returnbirth.durationend THEN
					returnbirth.durationdays := 1 + returnbirth.durationend - returnbirth.durationstart;
				ELSE
					returnbirth.durationdays := 0;
			END CASE;

			-- Send
			PIPE ROW (returnfiscal);
			PIPE ROW (returnbirth);

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
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END ageyears;

	/*
	 *  Last day of the year starting on the date.
	 */
	FUNCTION yearend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(inputdate, 12) - 1;
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END yearend;

	/*
	 *  The anniversary of the start date in the year following the end date.
	 */
	FUNCTION yearanniversary(startdate IN DATE, enddate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(startdate, 12 * ceil(months_between(enddate, startdate) / 12));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END yearanniversary;

	/*
	 *  Truncate a date to the start of the fiscal year, the preceding April 1.
	 */
	FUNCTION fiscalstart(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(TRUNC(add_months(cleandate(inputdate), -3), 'yyyy'), 3);
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalstart;

	/*
	 *  Truncate a date to the end of the fiscal year, the following March 31.
	 */
	FUNCTION fiscalend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN yearend(fiscalstart(cleandate(inputdate)));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring, formatmodel));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring, formatmodel));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END fiscalend;

	/*
	 *  Truncate a date to the start of the calendar year, the preceding January 1.
	 */
	FUNCTION calendarstart(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(cleandate(inputdate), 'yyyy');
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarstart;

	/*
	 *  Truncate a date to the end of the calendar year, the following December 31.
	 */
	FUNCTION calendarend(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN yearend(calendarstart(cleandate(inputdate)));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring, formatmodel));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring, formatmodel));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarstart(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarend(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END calendarend;

	/*
	 *  Check for minimally plausible date range
	 */
	FUNCTION cleandate(inputdate IN DATE) RETURN DATE DETERMINISTIC AS
	BEGIN
		CASE
			WHEN inputdate BETWEEN TO_DATE('18000101', 'YYYYMMDD') AND TRUNC(SYSDATE, 'MM') THEN
				RETURN inputdate;
			ELSE
				RETURN NULL;
		END CASE;
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END cleandate;

	/*
	 *  Try to convert a string to a date according to the format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2, formatmodel IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN cleandate(TO_DATE(datestring, formatmodel));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END cleandate;

	/*
	 *  Try to convert a string to a date using a default format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate(datestring IN VARCHAR2) RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN cleandate(datestring, 'YYYYMMDD');
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
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
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END cleanphn;

	/*
	 *  Clean a string of all non-numeric characters, then check for a minimally plausible 
	 *  Alberta provincial healthcare number, containing exactly nine digits with no leading
	 *  zeroes, return null otherwise.
	 */
	FUNCTION cleanphn(inputphn IN VARCHAR2) RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanphn(to_number(regexp_replace(inputphn, '[^0-9]', '')));
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END cleanphn;

	/*
	 *  For fields intended to indicate biological sex, not self identified gender, clean all
	 *  characters not indicating either female or male.
	 */
	FUNCTION cleansex(inputsex IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC AS
	BEGIN
		RETURN regexp_substr(UPPER(inputsex), '[FM]', 1, 1, 'i');
	EXCEPTION
		WHEN OTHERS THEN
			RETURN NULL;
	END cleansex;
END hazardutilities;