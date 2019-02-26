CREATE OR REPLACE PACKAGE BODY hazardutilities AS
/*
 *  Deterministic data processing functions.
 */

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
	RETURN demographicintervals PIPELINED DETERMINISTIC AS
		returnrow demographicinterval;
		localgreatest DATE;
		localleast DATE;
	BEGIN

		-- Maximum observation bounds
		returnrow.surveillancestart := leastsurveillancestart;
		returnrow.surveillanceend := greatestsurveillanceend;
		
		-- Estimate unobserved least birth date
		CASE surveillancebirth
			WHEN 1 THEN
				returnrow.leastbirth := COALESCE(leastbirth, leastsurveillancestart);
			ELSE
				returnrow.leastbirth := leastbirth;
		END CASE;

		-- Estimate unobserved greatest birth date
		CASE surveillancebirth
			WHEN 1 THEN
				returnrow.greatestbirth := COALESCE
				(
					greatestbirth,
					least
					(
						leastsurveillanceend,
						COALESCE(leastservice, leastsurveillanceend)
					)
				);
			ELSE
				returnrow.greatestbirth := greatestbirth;
		END CASE;

		-- Estimate unobserved least deceased date
		CASE surveillancedeceased
			WHEN 1 THEN
				returnrow.leastdeceased := COALESCE
				(
					leastdeceased,
					greatest
					(
						greatestsurveillancestart,
						COALESCE(greatestservice, greatestsurveillancestart)
					)
				);
			ELSE
				returnrow.leastdeceased := leastdeceased;
		END CASE;

		-- Estimate unobserved greatest deceased date
		CASE surveillancedeceased
			WHEN 1 THEN
				returnrow.greatestdeceased := COALESCE(greatestdeceased, greatestsurveillanceend);
			ELSE
				returnrow.greatestdeceased := greatestdeceased;
		END CASE;

		-- Least immigration date
		CASE
			WHEN returnrow.surveillancestart <= returnrow.leastbirth THEN
				returnrow.leastimmigrate := NULL;
			WHEN surveillanceimmigrate = 0 THEN
				returnrow.leastimmigrate := NULL;
			ELSE
				returnrow.leastimmigrate := leastsurveillancestart;
		END CASE;

		-- Greatest immigration date
		CASE
			WHEN returnrow.surveillancestart <= returnrow.greatestbirth THEN
				returnrow.greatestimmigrate := NULL;
			WHEN surveillanceimmigrate = 0 THEN
				returnrow.greatestimmigrate := NULL;
			ELSE
				returnrow.greatestimmigrate := least
				(
					leastsurveillanceend,
					COALESCE(leastservice, leastsurveillanceend)
				);
		END CASE;

		-- Least emigration date
		CASE
			WHEN returnrow.leastdeceased <= returnrow.surveillanceend THEN
				returnrow.leastemigrate := NULL;
			WHEN surveillanceemigrate = 0 THEN
				returnrow.leastemigrate := NULL;
			ELSE
				returnrow.leastemigrate := greatest
				(
					greatestsurveillancestart,
					COALESCE(greatestservice, greatestsurveillancestart)
				);
		END CASE;

		-- Greatest emigration date
		CASE
			WHEN returnrow.greatestdeceased <= returnrow.surveillanceend THEN
				returnrow.greatestemigrate := NULL;
			WHEN surveillanceemigrate = 0 THEN
				returnrow.greatestemigrate := NULL;
			ELSE
				returnrow.greatestemigrate := greatestsurveillanceend;
		END CASE;

		-- Birth date equipoise flags
		CASE
			WHEN returnrow.leastbirth < returnrow.greatestbirth THEN
				returnrow.birthdateequipoise := 0;
			ELSE
				returnrow.birthdateequipoise := 1;
		END CASE;

		-- Deceased date equipoise flag
		CASE
			WHEN returnrow.leastdeceased < returnrow.greatestdeceased THEN
				returnrow.deceaseddateequipoise := 0;
			ELSE
				returnrow.deceaseddateequipoise := 1;
		END CASE;

		-- Immigrate date equipoise flag
		CASE
			WHEN returnrow.leastimmigrate IS NULL AND returnrow.greatestimmigrate IS NULL THEN
				returnrow.immigratedateequipoise := 1;
			WHEN returnrow.leastimmigrate = returnrow.greatestimmigrate THEN
				returnrow.immigratedateequipoise := 1;
			ELSE
				returnrow.immigratedateequipoise := 0;
		END CASE;

		-- Emigrate date equipoise flag
		CASE
			WHEN returnrow.leastemigrate IS NULL AND returnrow.greatestemigrate IS NULL THEN
				returnrow.emigratedateequipoise := 1;
			WHEN returnrow.leastemigrate = returnrow.greatestemigrate THEN
				returnrow.emigratedateequipoise := 1;
			ELSE
				returnrow.emigratedateequipoise := 0;
		END CASE;

		-- Birth observation equipoise
		CASE
			WHEN returnrow.surveillancestart <= returnrow.leastbirth THEN
				returnrow.birthobservationequipoise := 1;
			WHEN returnrow.greatestbirth < returnrow.surveillancestart THEN
				returnrow.birthobservationequipoise := 1;
			ELSE
				returnrow.birthobservationequipoise := returnrow.birthdateequipoise;
		END CASE;

		-- Deceased observation equipoise
		CASE
			WHEN returnrow.greatestdeceased <= returnrow.surveillanceend THEN
				returnrow.deceasedobservationequipoise := 1;
			WHEN returnrow.surveillanceend < returnrow.leastdeceased THEN
				returnrow.deceasedobservationequipoise := 1;
			ELSE
				returnrow.deceasedobservationequipoise := returnrow.deceaseddateequipoise;
		END CASE;

		-- Immigration observation equipoise
		CASE
			WHEN returnrow.surveillancestart <= returnrow.leastimmigrate THEN
				returnrow.immigrateobservationequipoise := 1;
			WHEN returnrow.greatestimmigrate < returnrow.surveillancestart THEN
				returnrow.immigrateobservationequipoise := 1;
			ELSE
				returnrow.immigrateobservationequipoise := returnrow.immigratedateequipoise;
		END CASE;

		-- Emigration observation equipoise
		CASE
			WHEN returnrow.greatestemigrate <= returnrow.surveillanceend THEN
				returnrow.emigrateobservationequipoise := 1;
			WHEN returnrow.surveillanceend < returnrow.leastemigrate THEN
				returnrow.emigrateobservationequipoise := 1;
			ELSE
				returnrow.emigrateobservationequipoise := returnrow.emigratedateequipoise;
		END CASE;

		-- Surveillance start equipoise flag
		localleast := greatest
		(
			returnrow.surveillancestart,
			returnrow.leastbirth,
			COALESCE(returnrow.leastimmigrate, returnrow.surveillancestart)
		);
		localgreatest := greatest
		(
			returnrow.surveillancestart,
			returnrow.greatestbirth,
			COALESCE(returnrow.greatestimmigrate, returnrow.surveillancestart)
		);
		CASE
			WHEN localleast IS NULL AND localgreatest IS NULL THEN
				returnrow.surveillancestartequipoise := 1;
			WHEN localleast = localgreatest THEN
				returnrow.surveillancestartequipoise := 1;
			ELSE
				returnrow.surveillancestartequipoise := 0; 
		END CASE;

		-- Surveillance end equipoise flag
		localleast := least
		(
			returnrow.surveillanceend,
			COALESCE(returnrow.leastdeceased, returnrow.surveillanceend),
			COALESCE(returnrow.leastemigrate, returnrow.surveillanceend)
		);
		localgreatest := least
		(
			returnrow.surveillanceend,
			COALESCE(returnrow.greatestdeceased, returnrow.surveillanceend),
			COALESCE(returnrow.greatestemigrate, returnrow.surveillanceend)
		);
		CASE
			WHEN localleast IS NULL AND localgreatest IS NULL THEN
				returnrow.surveillanceendequipoise := 1;
			WHEN localleast = localgreatest THEN
				returnrow.surveillanceendequipoise := 1;
			ELSE
				returnrow.surveillanceendequipoise := 0;
		END CASE;

		-- Age equipoise flag
		CASE
			WHEN returnrow.leastbirth IS NULL AND returnrow.greatestbirth IS NULL THEN
				returnrow.ageequipoise := 1;
			WHEN fiscalstart(returnrow.leastbirth) = fiscalstart(returnrow.greatestbirth) THEN
				returnrow.ageequipoise := 1;
			ELSE
				returnrow.ageequipoise := 0;
		END CASE;		

		-- Send
		PIPE ROW (returnrow);
		RETURN;
	END generatedemographic;

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
	RETURN surveillancecintervals PIPELINED DETERMINISTIC AS
		returnlower surveillancecinterval;
		returnupper surveillancecinterval;
	BEGIN

		-- Lower lifespan extremum
		returnlower.cornercase := 'L';
		returnlower.birthdate := greatestbirth;
		returnlower.deceaseddate := leastdeceased;
		returnlower.immigratedate := greatestimmigrate;
		returnlower.emigratedate := leastemigrate;
		returnlower.extremumstart := greatest
		(
			surveillancestart,
			returnlower.birthdate,
			COALESCE(returnlower.immigratedate, surveillancestart)
		);
		returnlower.extremumend := least
		(
			surveillanceend,
			COALESCE(returnlower.deceaseddate, surveillanceend),
			COALESCE(returnlower.emigratedate, surveillanceend)
		);

		-- Upper lifespan extremum
		returnupper.cornercase := 'U';
		returnupper.birthdate := leastbirth;
		returnupper.deceaseddate := greatestdeceased;
		returnupper.immigratedate := leastimmigrate;
		returnupper.emigratedate := greatestemigrate;
		returnupper.extremumstart := greatest
		(
			surveillancestart,
			returnupper.birthdate,
			COALESCE(returnupper.immigratedate, surveillancestart)
		);
		returnupper.extremumend := least
		(
			surveillanceend,
			COALESCE(returnupper.deceaseddate, surveillanceend),
			COALESCE(returnupper.emigratedate, surveillanceend)
		);

		-- Send
		PIPE ROW (returnlower);
		PIPE ROW (returnupper);
		RETURN;
	END;

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
	RETURN censusintervals PIPELINED DETERMINISTIC AS
		returninterval censusinterval;
	BEGIN

		-- Common initialization
		returninterval.censusstart := fiscalstart(eventstart);
		returninterval.censusend := fiscalend(eventstart);
		returninterval.agestart := anniversarystart(birthdate, eventstart);
		returninterval.ageend := anniversaryend(birthdate, eventstart);
		returninterval.intervalstart := greatest(returninterval.censusstart, returninterval.agestart);
		returninterval.intervalend := least(returninterval.censusend, returninterval.ageend);
		returninterval.durationstart := eventstart;
		returninterval.intervalorder := 1;
		returninterval.intervalfirst := 1;
		returninterval.intervalage := ageyears(birthdate, returninterval.agestart);

		-- Determine the type of the first interval
		CASE returninterval.censusstart
			WHEN returninterval.agestart THEN
				returninterval.agecoincidecensus := 1;
				returninterval.agecoincideinterval := 1;
			WHEN returninterval.intervalstart THEN
				returninterval.agecoincidecensus := 0;
				returninterval.agecoincideinterval := 0;
			ELSE
				returninterval.agecoincidecensus := 0;
				returninterval.agecoincideinterval := 1;
		END CASE;

		-- Determine the number of intervals
		CASE

			-- Fiscal and age interval are the same
			WHEN returninterval.agecoincidecensus = 1 THEN
				returninterval.intervalcount := 1 + ageyears(returninterval.censusstart, fiscalstart(eventend));

			-- Start on the age interval and end on the fiscal interval
			WHEN returninterval.agecoincideinterval = 1 AND anniversarystart(birthdate, eventend) < fiscalstart(eventend) THEN
				returninterval.intervalcount := 2 * ageyears(returninterval.censusstart, fiscalstart(eventend));

			-- Start on the age interval and end on the age interval
			WHEN returninterval.agecoincideinterval = 1 THEN
				returninterval.intervalcount := 1 + 2 * ageyears(returninterval.censusstart, fiscalstart(eventend));

			-- Start on the fiscal interval and end on the fiscal interval
			WHEN anniversarystart(birthdate, eventend) < fiscalstart(eventend) THEN
				returninterval.intervalcount := 1 + 2 * ageyears(returninterval.censusstart, fiscalstart(eventend));
				
			-- Start on the fiscal interval and end on the age interval
			ELSE
				returninterval.intervalcount := 2 * (1 + ageyears(returninterval.censusstart, fiscalstart(eventend)));
		END CASE;

		-- Test for one record
		CASE returninterval.intervalcount
			WHEN 1 THEN
				NULL;
			ELSE
				returninterval.intervallast := 0;
				returninterval.durationend := returninterval.intervalend;
				returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
				PIPE ROW (returninterval);
				returninterval.intervalfirst := 0;
		END CASE;

		-- Determine the loop cases
		CASE
		
			-- No op for only one record
			WHEN returninterval.intervallast = 1 THEN
				NULL;

			-- Fiscal and census coincide
			WHEN returninterval.agecoincidecensus = 1 THEN
				LOOP

					-- Next interval
					returninterval.intervalorder := 1 + returninterval.intervalorder;
					returninterval.censusstart := add_months(returninterval.censusstart, 12);
					returninterval.censusend := add_months(returninterval.censusend, 12);
					returninterval.agestart := returninterval.censusstart;
					returninterval.ageend := returninterval.censusend;
					returninterval.intervalstart := returninterval.censusstart;
					returninterval.intervalend := returninterval.censusend;
					returninterval.durationstart := returninterval.intervalstart;
					returninterval.intervalage := 1 + returninterval.intervalage;

					-- End test
					EXIT WHEN eventend <= returninterval.intervalend;
					returninterval.durationend := returninterval.intervalend;
					returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
					PIPE ROW (returninterval);
				END LOOP;

			-- Start on age interval
			WHEN returninterval.agecoincideinterval = 1 THEN
				LOOP

					-- Fiscal interval
					returninterval.intervalorder := 1 + returninterval.intervalorder;
					returninterval.agecoincideinterval := 0;
					returninterval.censusstart := add_months(returninterval.censusstart, 12);
					returninterval.censusend := add_months(returninterval.censusend, 12);
					returninterval.intervalstart := returninterval.censusstart;
					returninterval.intervalend := returninterval.ageend;
					returninterval.durationstart := returninterval.intervalstart;

					-- End test
					EXIT WHEN eventend <= returninterval.intervalend;
					returninterval.durationend := returninterval.intervalend;
					returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
					PIPE ROW (returninterval);

					-- Next age interval
					returninterval.intervalorder := 1 + returninterval.intervalorder;
					returninterval.agecoincideinterval := 1;
					returninterval.agestart := anniversarystart(birthdate, returninterval.censusend);
					returninterval.ageend := anniversaryend(birthdate, returninterval.censusend);
					returninterval.intervalstart := returninterval.agestart;
					returninterval.intervalend := returninterval.censusend;
					returninterval.durationstart := returninterval.intervalstart;
					returninterval.intervalage := 1 + returninterval.intervalage;

					-- End test
					EXIT WHEN eventend <= returninterval.intervalend;
					returninterval.durationend := returninterval.intervalend;
					returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
					PIPE ROW (returninterval);
				END LOOP;

			-- Start on fiscal interval
			ELSE
				LOOP

					-- Age interval
					returninterval.intervalorder := 1 + returninterval.intervalorder;
					returninterval.agecoincideinterval := 1;
					returninterval.agestart := anniversarystart(birthdate, returninterval.censusend);
					returninterval.ageend := anniversaryend(birthdate, returninterval.censusend);
					returninterval.intervalstart := returninterval.agestart;
					returninterval.intervalend := returninterval.censusend;
					returninterval.durationstart := returninterval.intervalstart;
					returninterval.intervalage := 1 + returninterval.intervalage;

					-- End test
					EXIT WHEN eventend <= returninterval.intervalend;
					returninterval.durationend := returninterval.intervalend;
					returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
					PIPE ROW (returninterval);

					-- Fiscal interval
					returninterval.intervalorder := 1 + returninterval.intervalorder;
					returninterval.agecoincideinterval := 0;
					returninterval.censusstart := add_months(returninterval.censusstart, 12);
					returninterval.censusend := add_months(returninterval.censusend, 12);
					returninterval.intervalstart := returninterval.censusstart;
					returninterval.intervalend := returninterval.ageend;
					returninterval.durationstart := returninterval.intervalstart;

					-- End test
					EXIT WHEN eventend <= returninterval.intervalend;
					returninterval.durationend := returninterval.intervalend;
					returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
					PIPE ROW (returninterval);
				END LOOP;
		END CASE;

		-- Common finalization
		returninterval.intervallast := 1;
		returninterval.durationend := eventend;
		returninterval.durationdays := 1 + returninterval.durationend - returninterval.durationstart;
		PIPE ROW (returninterval);
		RETURN;
	END generatecensus;

	/*
	 *  Chidi Anagonye's Time Knife. Truncate an event into fiscal years, subpartitioned by
	 *  the birthday.
	 */
	FUNCTION generatecensus
	(
		eventdate IN DATE,
		birthdate IN DATE
	)
	RETURN censusintervals PIPELINED DETERMINISTIC AS
		returninterval censusinterval;
	BEGIN

		-- Fiscal, age, and duration boundaries
		returninterval.censusstart := fiscalstart(eventdate);
		returninterval.censusend := fiscalend(eventdate);
		returninterval.agestart := anniversarystart(birthdate, eventdate);
		returninterval.ageend := anniversaryend(birthdate, eventdate);
		returninterval.intervalstart := greatest(returninterval.censusstart, returninterval.agestart);
		returninterval.intervalend := least(returninterval.censusend, returninterval.ageend);
		returninterval.durationstart := eventdate;
		returninterval.durationend := eventdate;

		-- Order, count, duration, and age
		returninterval.intervalfirst := 1;
		returninterval.intervallast := 1;
		returninterval.intervalcount := 1;
		returninterval.intervalorder := 1;
		returninterval.durationdays := 1;
		returninterval.intervalage := ageyears(birthdate, returninterval.agestart);

		-- Determine the type of the interval
		CASE returninterval.censusstart
			WHEN returninterval.agestart THEN
				returninterval.agecoincidecensus := 1;
				returninterval.agecoincideinterval := 1;
			WHEN returninterval.intervalstart THEN
				returninterval.agecoincidecensus := 0;
				returninterval.agecoincideinterval := 0;
			ELSE
				returninterval.agecoincidecensus := 0;
				returninterval.agecoincideinterval := 1;
		END CASE;

		-- Send
		PIPE ROW (returninterval);
		RETURN;
	END generatecensus;

	/*
	 *  Lower truncated years between start date and end date.
	 */
	FUNCTION ageyears
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN floor(months_between(enddate, startdate) / 12);
	END ageyears;

	/*
	 *  The start of the anniversary year of the start date that the end date falls in.
	 */
	FUNCTION anniversarystart
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
		localmonths INTEGER := 12 * ageyears(startdate, enddate);
	BEGIN
		RETURN least
		(
			1 + add_months(startdate - 1, localmonths),
			add_months(startdate, localmonths)
		);
	END anniversarystart;

	/*
	 *  The end of the anniversary year of the start date that the end date falls in.
	 */
	FUNCTION anniversaryend
	(
		startdate IN DATE,
		enddate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
		localmonths INTEGER := 12 * (1 + ageyears(startdate, enddate));
	BEGIN
		RETURN least
		(
			1 + add_months(startdate - 1, localmonths),
			add_months(startdate, localmonths)
		) - 1;
	END anniversaryend;

	/*
	 *  Truncate a date to the start of the fiscal year, the preceding April 1.
	 */
	FUNCTION fiscalstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(TRUNC(add_months(inputdate, -3), 'yyyy'), 3);
	END fiscalstart;

	/*
	 *  Truncate a date to the end of the fiscal year, the following March 31.
	 */
	FUNCTION fiscalend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(fiscalstart(inputdate), 12) - 1;
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string.
	 */
	FUNCTION fiscalstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring, formatmodel));
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string.
	 */
	FUNCTION fiscalend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring, formatmodel));
	END fiscalend;

	/*
	 *  Return the start of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalstart(cleandate(datestring));
	END fiscalstart;

	/*
	 *  Return the end of the fiscal year given the date as string, and default format.
	 */
	FUNCTION fiscalend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN fiscalend(cleandate(datestring));
	END fiscalend;

	/*
	 *  Truncate a date to the start of the calendar year, the preceding January 1.
	 */
	FUNCTION calendarstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(inputdate, 'yyyy');
	END calendarstart;

	/*
	 *  Truncate a date to the end of the calendar year, the following December 31.
	 */
	FUNCTION calendarend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(calendarstart(inputdate), 12) - 1;
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string.
	 */
	FUNCTION calendarstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring, formatmodel));
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string.
	 */
	FUNCTION calendarend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring, formatmodel));
	END calendarend;

	/*
	 *  Return the start of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarstart(cleandate(datestring));
	END calendarstart;

	/*
	 *  Return the end of the calendar year given the date as string, and default format.
	 */
	FUNCTION calendarend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN calendarend(cleandate(datestring));
	END calendarend;

	/*
	 *  Truncate a date to the start of the quarter.
	 */
	FUNCTION quarterstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(inputdate, 'q');
	END quarterstart;

	/*
	 *  Truncate a date to the end of the quarter.
	 */
	FUNCTION quarterend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(quarterstart(inputdate), 3) - 1;
	END quarterend;

	/*
	 *  Return the start of the quarter given the date as string.
	 */
	FUNCTION quarterstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN quarterstart(cleandate(datestring, formatmodel));
	END quarterstart;

	/*
	 *  Return the end of the quarter given the date as string.
	 */
	FUNCTION quarterend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN quarterend(cleandate(datestring, formatmodel));
	END quarterend;

	/*
	 *  Return the start of the quarter given the date as string, and default format.
	 */
	FUNCTION quarterstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN quarterstart(cleandate(datestring));
	END quarterstart;

	/*
	 *  Return the end of the quarter given the date as string, and default format.
	 */
	FUNCTION quarterend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN quarterend(cleandate(datestring));
	END quarterend;

	/*
	 *  Truncate a date to the start of the month.
	 */
	FUNCTION monthstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(inputdate, 'mm');
	END monthstart;

	/*
	 *  Truncate a date to the end of the month.
	 */
	FUNCTION monthend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN add_months(monthstart(inputdate), 1) - 1;
	END monthend;

	/*
	 *  Return the start of the month given the date as string.
	 */
	FUNCTION monthstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN monthstart(cleandate(datestring, formatmodel));
	END monthstart;

	/*
	 *  Return the end of the month given the date as string.
	 */
	FUNCTION monthend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN monthend(cleandate(datestring, formatmodel));
	END monthend;

	/*
	 *  Return the start of the month given the date as string, and default format.
	 */
	FUNCTION monthstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN monthstart(cleandate(datestring));
	END monthstart;

	/*
	 *  Return the end of the month given the date as string, and default format.
	 */
	FUNCTION monthend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN monthend(cleandate(datestring));
	END monthend;

	/*
	 *  Truncate a date to the start of the week.
	 */
	FUNCTION weekstart
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN TRUNC(inputdate, 'dy');
	END weekstart;

	/*
	 *  Truncate a date to the end of the week.
	 */
	FUNCTION weekend
	(
		inputdate IN DATE
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN weekstart(inputdate) + 6;
	END weekend;

	/*
	 *  Return the start of the week given the date as string.
	 */
	FUNCTION weekstart
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN weekstart(cleandate(datestring, formatmodel));
	END weekstart;

	/*
	 *  Return the end of the week given the date as string.
	 */
	FUNCTION weekend
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN weekend(cleandate(datestring, formatmodel));
	END weekend;

	/*
	 *  Return the start of the week given the date as string, and default format.
	 */
	FUNCTION weekstart
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN weekstart(cleandate(datestring));
	END weekstart;

	/*
	 *  Return the end of the week given the date as string, and default format.
	 */
	FUNCTION weekend
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN weekend(cleandate(datestring));
	END weekend;

	/*
	 *  Try to convert a string to a date according to the format model. Return null when the
	 *  string cannot be converted to a date.
	 */
	FUNCTION cleandate
	(
		datestring IN VARCHAR2,
		formatmodel IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
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
	FUNCTION cleandate
	(
		datestring IN VARCHAR2
	)
	RETURN DATE DETERMINISTIC AS
	BEGIN
		RETURN cleandate(datestring, 'YYYYMMDD');
	END cleandate;

	/*
	 *  Check for a minimally plausible Alberta provincial healthcare number, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanphn
	(
		inputphn IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC AS
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
	FUNCTION cleanphn
	(
		inputphn IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanphn(cleaninteger(inputphn));
	END cleanphn;

	/*
	 *  For fields intended to indicate biological sex, not self identified gender, clean all
	 *  characters not indicating either female or male.
	 */
	FUNCTION cleansex
	(
		inputsex IN VARCHAR2
	)
	RETURN VARCHAR2 DETERMINISTIC AS
	BEGIN
		RETURN regexp_substr(UPPER(inputsex), '[FM]', 1, 1, 'i');
	END cleansex;
	
	/*
	 *  Ensure the inpatient care facility number is between 80000 and 80999.
	 */
	FUNCTION cleaninpatient
	(
		inputfacility IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC AS
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
	FUNCTION cleaninpatient
	(
		inputfacility IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleaninpatient(cleaninteger(inputfacility));
	END cleaninpatient;
	
	/*
	 *  Ensure the ambulatory care facility number is between 88000 and 88999.
	 */
	FUNCTION cleanambulatory
	(
		inputfacility IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC AS
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
	FUNCTION cleanambulatory
	(
		inputfacility IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanambulatory(cleaninteger(inputfacility));
	END cleanambulatory;

	/*
	 *  Check for a minimally plausible Alberta provincial provider identifier, containing
	 *  exactly nine digits with no leading zeroes, return null otherwise.
	 */
	FUNCTION cleanprid
	(
		inputprid IN INTEGER
	)
	RETURN INTEGER DETERMINISTIC AS
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
	FUNCTION cleanprid
	(
		inputprid IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN cleanprid(cleaninteger(inputprid));
	END cleanprid;

	/*
	 *  Clean a string of all non-numeric characters.
	 */
	FUNCTION cleaninteger
	(
		inputstring IN VARCHAR2
	)
	RETURN INTEGER DETERMINISTIC AS
	BEGIN
		RETURN to_number(regexp_replace(inputstring, '[^0-9]', ''));
	END cleaninteger;
END hazardutilities;