ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
SELECT

	-- Demographics
	a0.uliabphn,
	a0.sex,
	a0.firstnations,

	/****************************************************************************************
	 *                                                                                      *
	 * CAUTION! CORNER CASE MUST BE INCLUDED AS A DIMENSION IN ANY AGGREGATE, GROUP BY,     *
	 * WINDOW, OR PARTITION CLAUSE! THIS IDENTIFIERS THE EXTREMUM OF THE LIFE TABLE         *
	 * OBSERVATION, ENCAPSULATING EQUIVOCATION IN THE OBSERVATION OF BIRTH AND DEATH DATES! *
	 *                                                                                      *
	 ****************************************************************************************/
	a0.cornercase,
	
	-- Start and end of life (obviously), deceased date is null when not observed
	a0.birthdate,
	a0.deceaseddate,
	
	-- Start and end of residency, null when not observed
	a0.immigratedate,
	a0.emigratedate,

	-- Extent of surveillance observation
	a0.surveillancestart,
	a0.surveillanceend,

	-- Surveillance interval rectified by birth, deceased, and censored dates
	a0.extremumstart,
	a0.extremumend,

	/*
	 *  Comparisons of the two surveillance extremums within the same person.
	 */

	-- Least and greatest birth dates are in the same fiscal year
	a0.ageequipoise,

	-- Least and greatest birth dates are equal
	a0.birthdateequipoise,

	-- Least and greatest deceased dates are equal
	a0.deceaseddateequipoise,

	-- Birth observed flag is equal in both surveillance extremums
	a0.birthobservationequipoise,

	-- Death observed flag is equal in both surveillance extremums
	a0.deceasedobservationequipoise,

	-- Least and greatest immigration dates are equal
	a0.immigratedateequipoise,

	-- Least and greatest emigration dates are equal
	a0.emigratedateequipoise,

	-- In migration observed flag is equal in both surveillance extremums
	a0.immigrateobservationequipoise,

	-- Out migration observed flag is equal in both surveillance extremums
	a0.emigrateobservationequipoise,

	-- Surveillance extremum start dates are equal
	a0.surveillancestartequipoise,

	-- Surveillance extremum end dates are equal
	a0.surveillanceendequipoise,

	/*
	 *  Census interval properties, the duration is used as the denominator.
	 */

	-- Does the unrectified intersection start on the birthday
	a0.agecoincideinterval,

	-- Does the birthday fall on the start of the fiscal year
	a0.agecoincidecensus,

	-- The start and end of the fiscal year
	a0.censusstart,
	a0.censusend,

	-- The start and end of the person's age year, with the age specified in interval age
	a0.agestart,
	a0.ageend,

	-- The start and end of the intersection of the fiscal year and the person's age year
	a0.intervalstart,
	a0.intervalend,

	-- The intersection rectified by the start and end of the surveillance interval
	a0.durationstart,
	a0.durationend,

	/****************************************************************************************
	 *                                                                                      *
	 * LOOK NO FURTHER! THIS IS YOUR HAZARD RATE DENOMINATOR! SUM THIS WITHIN EACH CORNER   *
	 * CASE! TYPICALLY DIVIDE THE SUM BY 365.25 TO REPORT IN UNITS OF AMOUNT PER PERSON     *
	 * YEAR!                                                                                *
	 *                                                                                      *
	 ****************************************************************************************/
	a0.durationdays,

	-- The age of the person's age year that intersects with the interval
	a0.intervalage,

	-- Birth was observed
	a0.intervalbirth,

	-- Death was observed
	a0.intervaldeceased,

	-- In migration was observed
	a0.intervalimmigrate,

	-- Out migration was observed
	a0.intervalemigrate,

	-- Is this the first census interval
	a0.intervalfirst,

	-- Is this the last census interval
	a0.intervallast,

	-- Total number of census intervals in the partition of the surveillance interval
	a0.intervalcount,

	-- Order of the census interval in the partition of the surveillance interval
	a0.intervalorder,

	/*
	 *  Utilization in the census intervals, used as the numerators.
	 */
	a1.measurevalue utilizationmeasure,
	a1.measureidentifier utilizationidentifier,
	a1.measuredescription utilizationdescription,
	
	-- Last refresh
	a0.censoreddate
FROM

	-- Each surveillance interval is partitioned into census intervals, a pair for each fiscal
	-- year, the interval before the birthday, and the interval after. This is the denominator
	-- data in the hazard rates.
	ab_hzrd_rts_anlys.personcensus a0
	LEFT JOIN
	
	-- Most, but not all, census intervals will have some form of utilization. This is the
	-- numerator in the hazard rates.
	ab_hzrd_rts_anlys.personmeasure a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend;