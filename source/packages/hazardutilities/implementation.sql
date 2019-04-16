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
			WHEN returninterval.intervalfirst = 1 THEN
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
	 *  Pivot a census utilization record to a columnar list of measures.
	 */
	FUNCTION generatemeasures
	(
		livenewborns IN INTEGER,
		ambulatoryminutes IN INTEGER,
		ambulatoryvisits IN INTEGER,
		ambulatorysitedays IN INTEGER,
		ambulatorydays IN INTEGER,
		ambulatoryprivateminutes IN INTEGER,
		ambulatoryprivatevisits IN INTEGER,
		ambulatoryprivatesitedays IN INTEGER,
		ambulatoryprivatedays IN INTEGER,
		ambulatoryworkminutes IN INTEGER,
		ambulatoryworkvisits IN INTEGER,
		ambulatoryworksitedays IN INTEGER,
		ambulatoryworkdays IN INTEGER,
		inpatientdays IN INTEGER,
		inpatientadmissions IN INTEGER,
		inpatientdischarges IN INTEGER,
		inpatientstays IN INTEGER,
		inpatientprivatedays IN INTEGER,
		inpatientprivateadmissions IN INTEGER,
		inpatientprivatedischarges IN INTEGER,
		inpatientprivatestays IN INTEGER,
		inpatientworkdays IN INTEGER,
		inpatientworkadmissions IN INTEGER,
		inpatientworkdischarges IN INTEGER,
		inpatientworkstays IN INTEGER,
		caremanagerdays IN INTEGER,
		caremanagerallocations IN INTEGER,
		caremanagerreleases IN INTEGER,
		caremanagers IN INTEGER,
		homecareprofessionalservices IN INTEGER,
		homecaretransitionservices IN INTEGER,
		homecareservices IN INTEGER,
		homecareprofessionalvisits IN INTEGER,
		homecaretransitionvisits IN INTEGER,
		homecarevisits IN INTEGER,
		homecareprofessionaldays IN INTEGER,
		homecaretransitiondays IN INTEGER,
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
		psychiatryprocedures IN INTEGER,
		radiologyprocedures IN INTEGER,
		specialtyprocedures IN INTEGER,
		surgicalprocedures IN INTEGER,
		primarycareprocedures IN INTEGER,
		anesthesiologistsdays IN INTEGER,
		consultprovidersdays IN INTEGER,
		generalpractitionersdays IN INTEGER,
		obstetriciansdays IN INTEGER,
		pathologistsdays IN INTEGER,
		psychiatristsdays IN INTEGER,
		radiologistsdays IN INTEGER,
		specialistsdays IN INTEGER,
		surgeonsdays IN INTEGER,
		primarycareproviderdays IN INTEGER,
		anesthesiologydays IN INTEGER,
		consultdays IN INTEGER,
		generalpracticedays IN INTEGER,
		obstetricdays IN INTEGER,
		pathologydays IN INTEGER,
		psychiatrydays IN INTEGER,
		radiologydays IN INTEGER,
		specialtydays IN INTEGER,
		surgerydays IN INTEGER,
		primarycaredays IN INTEGER,
		supportivelivingdays IN INTEGER,
		supportivelivingadmissions IN INTEGER,
		supportivelivingdischarges IN INTEGER,
		supportivelivingstays IN INTEGER
	)
	RETURN censusmeasures PIPELINED DETERMINISTIC AS
		localmeasure censusmeasure;
	BEGIN

		-- Elide empty newborns records
		IF livenewborns > 0 THEN
			localmeasure.measurevalue := livenewborns;
			localmeasure.measureidentifier := 'livenewborns';
			localmeasure.measuredescription := 'Naive count of live newborns delivered by the mother in the census interval, minimal plausibility checks.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty ambulatory records
		IF ambulatoryminutes > 0 THEN
			localmeasure.measurevalue := ambulatoryminutes;
			localmeasure.measureidentifier := 'ambulatoryminutes';
			localmeasure.measuredescription := 'Naive sum of emergency ambulatory care minutes that intersected with the census interval, including overlapping visits.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryvisits;
			localmeasure.measureidentifier := 'ambulatoryvisits';
			localmeasure.measuredescription := 'Emergency ambulatory care visits in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatorysitedays;
			localmeasure.measureidentifier := 'ambulatorysitedays';
			localmeasure.measuredescription := 'Unique combinations of days and ambulatory care sites visited for an emergency in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatorydays;
			localmeasure.measureidentifier := 'ambulatorydays';
			localmeasure.measuredescription := 'Unique days of ambulatory care visits for an emergency in the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty ambulatory private casualty records
		IF ambulatoryprivateminutes > 0 THEN
			localmeasure.measurevalue := ambulatoryprivateminutes;
			localmeasure.measureidentifier := 'ambulatoryprivateminutes';
			localmeasure.measuredescription := 'Naive sum of emergency ambulatory care minutes, for private casualties, that intersected with the census interval, including overlapping visits.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryprivatevisits;
			localmeasure.measureidentifier := 'ambulatoryprivatevisits';
			localmeasure.measuredescription := 'Emergency ambulatory care visits, for private casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryprivatesitedays;
			localmeasure.measureidentifier := 'ambulatoryprivatesitedays';
			localmeasure.measuredescription := 'Unique combinations of days and ambulatory care sites visited for a private casualty emergency in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryprivatedays;
			localmeasure.measureidentifier := 'ambulatoryprivatedays';
			localmeasure.measuredescription := 'Unique days of ambulatory care visits for a private casualty emergency in the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty ambulatory workplace casualty records
		IF ambulatoryworkminutes > 0 THEN
			localmeasure.measurevalue := ambulatoryworkminutes;
			localmeasure.measureidentifier := 'ambulatoryworkminutes';
			localmeasure.measuredescription := 'Naive sum of emergency ambulatory care minutes, for workplace casualties, that intersected with the census interval, including overlapping visits.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryworkvisits;
			localmeasure.measureidentifier := 'ambulatoryworkvisits';
			localmeasure.measuredescription := 'Emergency ambulatory care visits, for workplace casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryworksitedays;
			localmeasure.measureidentifier := 'ambulatoryworksitedays';
			localmeasure.measuredescription := 'Unique combinations of days and ambulatory care sites visited for a workplace casualty emergency in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := ambulatoryworkdays;
			localmeasure.measureidentifier := 'ambulatoryworkdays';
			localmeasure.measuredescription := 'Unique days of ambulatory care visits for a workplace casualty emergency in the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty inpatient records
		IF inpatientdays > 0 THEN
			localmeasure.measurevalue := inpatientdays;
			localmeasure.measureidentifier := 'inpatientdays';
			localmeasure.measuredescription := 'Naive sum of emergency inpatient care days that intersected with the census interval, including overlapping stays.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientadmissions;
			localmeasure.measureidentifier := 'inpatientadmissions';
			localmeasure.measuredescription := 'Emergency inpatient care admissions in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientdischarges;
			localmeasure.measureidentifier := 'inpatientdischarges';
			localmeasure.measuredescription := 'Emergency inpatient care discharges in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientstays;
			localmeasure.measureidentifier := 'inpatientstays';
			localmeasure.measuredescription := 'Emergency inpatient care stays intersecting with the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty inpatient private casualty records
		IF inpatientprivatedays > 0 THEN
			localmeasure.measurevalue := inpatientprivatedays;
			localmeasure.measureidentifier := 'inpatientprivatedays';
			localmeasure.measuredescription := 'Naive sum of emergency inpatient care days, for private casualties, that intersected with the census interval, including overlapping stays.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientprivateadmissions;
			localmeasure.measureidentifier := 'inpatientprivateadmissions';
			localmeasure.measuredescription := 'Emergency inpatient care admissions, for private casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientprivatedischarges;
			localmeasure.measureidentifier := 'inpatientprivatedischarges';
			localmeasure.measuredescription := 'Emergency inpatient care discharges, for private casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientprivatestays;
			localmeasure.measureidentifier := 'inpatientprivatestays';
			localmeasure.measuredescription := 'Emergency inpatient care stays, for private casualties, intersecting with the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty inpatient workplace casualty records
		IF inpatientworkdays > 0 THEN
			localmeasure.measurevalue := inpatientworkdays;
			localmeasure.measureidentifier := 'inpatientworkdays';
			localmeasure.measuredescription := 'Naive sum of emergency inpatient care days, for workplace casualties, that intersected with the census interval, including overlapping stays.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientworkadmissions;
			localmeasure.measureidentifier := 'inpatientworkadmissions';
			localmeasure.measuredescription := 'Emergency inpatient care admissions, for workplace casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientworkdischarges;
			localmeasure.measureidentifier := 'inpatientworkdischarges';
			localmeasure.measuredescription := 'Emergency inpatient care discharges, for workplace casualties, in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := inpatientworkstays;
			localmeasure.measureidentifier := 'inpatientworkstays';
			localmeasure.measuredescription := 'Emergency inpatient care stays, for workplace casualties, intersecting with the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty care management records
		IF caremanagerdays > 0 THEN
			localmeasure.measurevalue := caremanagerdays;
			localmeasure.measureidentifier := 'caremanagerdays';
			localmeasure.measuredescription := 'Naive sum of days of professionals allocated to provide care, case, transition, or placement managment or coordination, that intersected with the census interval, including overlapping allocations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := caremanagerallocations;
			localmeasure.measureidentifier := 'caremanagerallocations';
			localmeasure.measuredescription := 'Allocations of professionals to provide care, case, transition, or placement managment or coordination.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := caremanagerreleases;
			localmeasure.measureidentifier := 'caremanagerreleases';
			localmeasure.measuredescription := 'Release of professionals from providing care, case, transition, or placement managment or coordination.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := caremanagers;
			localmeasure.measureidentifier := 'caremanagers';
			localmeasure.measuredescription := 'Allocations of professionals providing care, case, transition, or placement managment or coordination that intersected with the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty home care professional services
		IF homecareprofessionalservices > 0 THEN
			localmeasure.measurevalue := homecareprofessionalservices;
			localmeasure.measureidentifier := 'homecareprofessionalservices';
			localmeasure.measuredescription := 'Number of of home care activities provided by a registered, regulated, or licensed professional in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecareprofessionalvisits;
			localmeasure.measureidentifier := 'homecareprofessionalvisits';
			localmeasure.measuredescription := 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one home care service to the person in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecareprofessionaldays;
			localmeasure.measureidentifier := 'homecareprofessionaldays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was provided home care services by a registered or regulated professional.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty home care transition services
		IF homecaretransitionservices > 0 THEN
			localmeasure.measurevalue := homecaretransitionservices;
			localmeasure.measureidentifier := 'homecaretransitionservices';
			localmeasure.measuredescription := 'Number of of transition, or placement activities provided by a registered, regulated, or licensed professional in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecaretransitionvisits;
			localmeasure.measureidentifier := 'homecaretransitionvisits';
			localmeasure.measuredescription := 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one transition, or placement service to the person in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecaretransitiondays;
			localmeasure.measureidentifier := 'homecaretransitiondays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was provided transition, or placement services by a registered or regulated professional.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty home care services
		IF homecareservices > 0 THEN
			localmeasure.measurevalue := homecareservices;
			localmeasure.measureidentifier := 'homecareservices';
			localmeasure.measuredescription := 'Number of of home care, transition, or placement activities provided by a registered, regulated, or licensed professional in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecarevisits;
			localmeasure.measureidentifier := 'homecarevisits';
			localmeasure.measuredescription := 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one home care, transition, or placement service to the person in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := homecaredays;
			localmeasure.measureidentifier := 'homecaredays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was provided home care, transition, or placement services by a registered or regulated professional.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty laboratory collection records
		IF laboratoryassays > 0 THEN
			localmeasure.measurevalue := laboratoryassays;
			localmeasure.measureidentifier := 'laboratoryassays';
			localmeasure.measuredescription := 'Number assays done of community laboratory samples collected in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := laboratorysitedays;
			localmeasure.measureidentifier := 'laboratorysitedays';
			localmeasure.measuredescription := 'Number unique combinations of community laboratory collection sites and days in the census interval where the person had a collection taken.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := laboratorydays;
			localmeasure.measureidentifier := 'laboratorydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person had a community laboratory collection taken.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty long term care records
		IF longtermcaredays > 0 THEN
			localmeasure.measurevalue := longtermcaredays;
			localmeasure.measureidentifier := 'longtermcaredays';
			localmeasure.measuredescription := 'Naive sum of long term care days that intersected with the census interval, including overlapping stays.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := longtermcareadmissions;
			localmeasure.measureidentifier := 'longtermcareadmissions';
			localmeasure.measuredescription := 'Long term care admissions in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := longtermcaredischarges;
			localmeasure.measureidentifier := 'longtermcaredischarges';
			localmeasure.measuredescription := 'Long term care discharges in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := longtermcarestays;
			localmeasure.measureidentifier := 'longtermcarestays';
			localmeasure.measuredescription := 'Long term care stays intersecting with the census interval.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty standard therapeutics dispensing
		IF pharmacystandarddailydoses > 0 THEN
			localmeasure.measurevalue := pharmacystandarddailydoses;
			localmeasure.measureidentifier := 'pharmacystandarddailydoses';
			localmeasure.measuredescription := 'Naive sum of days supply dispensed from a community pharmacy of standard prescription therapeutics not subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacystandardtherapeutics;
			localmeasure.measureidentifier := 'pharmacystandardtherapeutics';
			localmeasure.measuredescription := 'Number of distinct standard prescription therapeutics dispensed from a community pharmacy not subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacystandardsitedays;
			localmeasure.measureidentifier := 'pharmacystandardsitedays';
			localmeasure.measuredescription := 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacystandarddays;
			localmeasure.measureidentifier := 'pharmacystandarddays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was dispensed from a community pharmacy a standard prescription of a therapeutic not subject to controlled substances regulations.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty controlled therapeutics dispensing
		IF pharmacycontrolleddailydoses > 0 THEN
			localmeasure.measurevalue := pharmacycontrolleddailydoses;
			localmeasure.measureidentifier := 'pharmacycontrolleddailydoses';
			localmeasure.measuredescription := 'Naive sum of days supply dispensed from a community pharmacy of triple pad prescription therapeutics subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacycontrolledtherapeutics;
			localmeasure.measureidentifier := 'pharmacycontrolledtherapeutics';
			localmeasure.measuredescription := 'Number of distinct triple pad prescription therapeutics dispensed from a community pharmacy subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacycontrolledsitedays;
			localmeasure.measureidentifier := 'pharmacycontrolledsitedays';
			localmeasure.measuredescription := 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacycontrolleddays;
			localmeasure.measureidentifier := 'pharmacycontrolleddays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was dispensed from a community pharmacy a triple pad prescription of a therapeutic subject to controlled substances regulations.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty therapeutics dispensing
		IF pharmacydailydoses > 0 THEN
			localmeasure.measurevalue := pharmacydailydoses;
			localmeasure.measureidentifier := 'pharmacydailydoses';
			localmeasure.measuredescription := 'Naive sum of days supply dispensed from a community pharmacy of all prescription therapeutics.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacytherapeutics;
			localmeasure.measureidentifier := 'pharmacytherapeutics';
			localmeasure.measuredescription := 'Number of distinct prescription therapeutics dispensed from a community pharmacy.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacysitedays;
			localmeasure.measureidentifier := 'pharmacysitedays';
			localmeasure.measuredescription := 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed any prescription therapeutic.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pharmacydays;
			localmeasure.measureidentifier := 'pharmacydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person was dispensed from a community pharmacy any prescription therapeutic.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty anesthesiology records
		IF anesthesiologyprocedures > 0 THEN
			localmeasure.measurevalue := anesthesiologyprocedures;
			localmeasure.measureidentifier := 'anesthesiologyprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := anesthesiologydays;
			localmeasure.measureidentifier := 'anesthesiologydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := anesthesiologistsdays;
			localmeasure.measureidentifier := 'anesthesiologistsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care anesthesiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty consult records
		IF consultprocedures > 0 THEN
			localmeasure.measurevalue := consultprocedures;
			localmeasure.measureidentifier := 'consultprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a provider when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := consultprovidersdays;
			localmeasure.measureidentifier := 'consultprovidersdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care providers and days in the census interval when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := consultdays;
			localmeasure.measureidentifier := 'consultdays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when either the primary care provider role was consult, assistant, or second, or the procedure was outside of their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty general practice records
		IF generalpracticeprocedures > 0 THEN
			localmeasure.measurevalue := generalpracticeprocedures;
			localmeasure.measureidentifier := 'generalpracticeprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := generalpractitionersdays;
			localmeasure.measureidentifier := 'generalpractitionersdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := generalpracticedays;
			localmeasure.measureidentifier := 'generalpracticedays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty obstetrics and gynecology records
		IF obstetriciansdays > 0 THEN
			localmeasure.measurevalue := obstetricprocedures;
			localmeasure.measureidentifier := 'obstetricprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a obstetrician-gynecologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := obstetriciansdays;
			localmeasure.measureidentifier := 'obstetriciansdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care obstetrician-gynecologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := obstetricdays;
			localmeasure.measureidentifier := 'obstetricdays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care obstetrician-gynecologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty pathology records
		IF pathologyprocedures > 0 THEN
			localmeasure.measurevalue := pathologyprocedures;
			localmeasure.measureidentifier := 'pathologyprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pathologistsdays;
			localmeasure.measureidentifier := 'pathologistsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care pathologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := pathologydays;
			localmeasure.measureidentifier := 'pathologydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty psychiatry records
		IF psychiatryprocedures > 0 THEN
			localmeasure.measurevalue := psychiatryprocedures;
			localmeasure.measureidentifier := 'psychiatryprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a psychiatrist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := psychiatristsdays;
			localmeasure.measureidentifier := 'psychiatristsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care psychiatrists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := psychiatrydays;
			localmeasure.measureidentifier := 'psychiatrydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care psychiatrist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty radiology records
		IF radiologyprocedures > 0 THEN
			localmeasure.measurevalue := radiologyprocedures;
			localmeasure.measureidentifier := 'radiologyprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := radiologistsdays;
			localmeasure.measureidentifier := 'radiologistsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := radiologydays;
			localmeasure.measureidentifier := 'radiologydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty specialty records
		IF specialtyprocedures > 0 THEN
			localmeasure.measurevalue := specialtyprocedures;
			localmeasure.measureidentifier := 'specialtyprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := specialistsdays;
			localmeasure.measureidentifier := 'specialistsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care specialists other than an anesthesiologists, general practitioners, pathologists, radiologists, or surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := specialtydays;
			localmeasure.measureidentifier := 'specialtydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty surgery records
		IF surgicalprocedures > 0 THEN
			localmeasure.measurevalue := surgicalprocedures;
			localmeasure.measureidentifier := 'surgicalprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval delivered by a surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := surgeonsdays;
			localmeasure.measureidentifier := 'surgeonsdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := surgerydays;
			localmeasure.measureidentifier := 'surgerydays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when a primary care surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty primary care records
		IF primarycareprocedures > 0 THEN
			localmeasure.measurevalue := primarycareprocedures;
			localmeasure.measureidentifier := 'primarycareprocedures';
			localmeasure.measuredescription := 'Number of primary care procedures in the census interval.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := primarycareproviderdays;
			localmeasure.measureidentifier := 'primarycareproviderdays';
			localmeasure.measuredescription := 'Number of unique combinations of primary care providers and unique days in the census interval when the person utilized primary care.';
			PIPE ROW (localmeasure);

			localmeasure.measurevalue := primarycaredays;
			localmeasure.measureidentifier := 'primarycaredays';
			localmeasure.measuredescription := 'Number of unique days in the census interval when the person visited primary care in the community.';
			PIPE ROW (localmeasure);
		END IF;

		-- Elide empty designated supportive living records
		IF supportivelivingdays > 0 THEN
			localmeasure.measurevalue := supportivelivingdays;
			localmeasure.measureidentifier := 'supportivelivingdays';
			localmeasure.measuredescription := 'Naive sum of designated supportive living days that intersected with the census interval, including overlapping stays.';
			PIPE ROW (localmeasure);
	
			localmeasure.measurevalue := supportivelivingadmissions;
			localmeasure.measureidentifier := 'supportivelivingadmissions';
			localmeasure.measuredescription := 'Designated supportive living admissions in the census interval.';
			PIPE ROW (localmeasure);
	
			localmeasure.measurevalue := supportivelivingdischarges;
			localmeasure.measureidentifier := 'supportivelivingdischarges';
			localmeasure.measuredescription := 'Designated supportive living discharges in the census interval.';
			PIPE ROW (localmeasure);
	
			localmeasure.measurevalue := supportivelivingstays;
			localmeasure.measureidentifier := 'supportivelivingstays';
			localmeasure.measuredescription := 'Designated supportive living stays intersecting with the census interval.';
			PIPE ROW (localmeasure);
		END IF;
		RETURN;
	END generatemeasures;

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