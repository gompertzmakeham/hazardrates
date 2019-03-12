ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
CREATE TABLE hazardrates NOLOGGING COMPRESS NOCACHE PARALLEL 8 AS
SELECT

	-- Demographics
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.sex AS VARCHAR2(1)) sex,
	CAST(a0.firstnations AS INTEGER) firstnations,

	/****************************************************************************************
	 *                                                                                      *
	 * CAUTION! CORNER CASE MUST BE INCLUDED AS A DIMENSION IN ANY AGGREGATE, GROUP BY,     *
	 * WINDOW, OR PARTITION CLAUSE! THIS IDENTIFIERS THE EXTREMUM OF THE LIFE TABLE         *
	 * OBSERVATION, ENCAPSULATING EQUIVOCATION IN THE OBSERVATION OF BIRTH AND DEATH DATES! *
	 *                                                                                      *
	 ****************************************************************************************/
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	
	-- Start and end of life (obviously), deceased date is null when not observed
	CAST(a0.birthdate AS DATE) birthdate,
	CAST(a0.deceaseddate AS DATE) deceaseddate,
	
	-- Start and end of residency, null when not observed
	CAST(a0.immigratedate AS DATE) immigratedate,
	CAST(a0.emigratedate AS DATE) emigratedate,

	-- Extent of surveillance observation
	CAST(a0.surveillancestart AS DATE) surveillancestart,
	CAST(a0.surveillanceend AS DATE) surveillanceend,

	-- Surveillance interval rectified by birth, deceased, and censored dates
	CAST(a0.extremumstart AS DATE) extremumstart,
	CAST(a0.extremumend AS DATE) extremumend,

	/*
	 *  Comparisons of the two surveillance extremums within the same person.
	 */

	-- Least and greatest birth dates are in the same fiscal year
	CAST(a0.ageequipoise AS INTEGER) ageequipoise,

	-- Least and greatest birth dates are equal
	CAST(a0.birthdateequipoise AS INTEGER) birthdateequipoise,

	-- Least and greatest deceased dates are equal
	CAST(a0.deceaseddateequipoise AS INTEGER) deceaseddateequipoise,

	-- Birth observed flag is equal in both surveillance extremums
	CAST(a0.birthobservationequipoise AS INTEGER) birthobservationequipoise,

	-- Death observed flag is equal in both surveillance extremums
	CAST(a0.deceasedobservationequipoise AS INTEGER) deceasedobservationequipoise,

	-- Least and greatest immigration dates are equal
	CAST(a0.immigratedateequipoise AS INTEGER) immigratedateequipoise,

	-- Least and greatest emigration dates are equal
	CAST(a0.emigratedateequipoise AS INTEGER) emigratedateequipoise,

	-- In migration observed flag is equal in both surveillance extremums
	CAST(a0.immigrateobservationequipoise AS INTEGER),

	-- Out migration observed flag is equal in both surveillance extremums
	CAST(a0.emigrateobservationequipoise AS INTEGER) immigrateobservationequipoise,

	-- Surveillance extremum start dates are equal
	CAST(a0.surveillancestartequipoise AS INTEGER) surveillancestartequipoise,

	-- Surveillance extremum end dates are equal
	CAST(a0.surveillanceendequipoise AS INTEGER) surveillanceendequipoise,

	/*
	 *  Census interval properties, the duration is used as the denominator.
	 */

	-- Does the unrectified intersection start on the birthday
	CAST(a0.agecoincideinterval AS INTEGER) agecoincideinterval,

	-- Does the birthday fall on the start of the fiscal year
	CAST(a0.agecoincidecensus AS INTEGER) agecoincidecensus,

	-- The start and end of the fiscal year
	CAST(a0.censusstart AS DATE) censusstart,
	CAST(a0.censusend AS DATE) censusend,

	-- The start and end of the person's age year, with the age specified in interval age
	CAST(a0.agestart AS DATE) agestart,
	CAST(a0.ageend AS DATE) ageend,

	-- The start and end of the intersection of the fiscal year and the person's age year
	CAST(a0.intervalstart AS DATE) intervalstart,
	CAST(a0.intervalend AS DATE) intervalend,

	-- The intersection rectified by the start and end of the surveillance interval
	CAST(a0.durationstart AS DATE) durationstart,
	CAST(a0.durationend AS DATE) durationend,

	/****************************************************************************************
	 *                                                                                      *
	 * LOOK NO FURTHER! THIS IS YOUR HAZARD RATE DENOMINATOR! SUM THIS WITHIN EACH CORNER   *
	 * CASE! TYPICALLY DIVIDE THE SUM BY 365.25 TO REPORT IN UNITS OF AMOUNT PER PERSON     *
	 * YEAR!                                                                                *
	 *                                                                                      *
	 ****************************************************************************************/
	CAST(a0.durationdays AS INTEGER) durationdays,

	-- The age of the person's age year that intersects with the interval
	CAST(a0.intervalage AS INTEGER) intervalage,

	-- Birth was observed
	CAST(a0.intervalbirth AS INTEGER) intervalbirth,

	-- Death was observed
	CAST(a0.intervaldeceased AS INTEGER) intervaldeceased,

	-- In migration was observed
	CAST(a0.intervalimmigrate AS INTEGER) intervalimmigrate,

	-- Out migration was observed
	CAST(a0.intervalemigrate AS INTEGER) intervalemigrate,

	-- Is this the first census interval
	CAST(a0.intervalfirst AS INTEGER) intervalfirst,

	-- Is this the last census interval
	CAST(a0.intervallast AS INTEGER) intervallast,

	-- Total number of census intervals in the partition of the surveillance interval
	CAST(a0.intervalcount AS INTEGER) intervalcount,

	-- Order of the census interval in the partition of the surveillance interval
	CAST(a0.intervalorder AS INTEGER) intervalorder,

	/*
	 *  Utilization in the census intervals, used as the numerators.
	 */
	CAST(COALESCE(a1.measurevalue, 0) AS INTEGER) utilizationmeasure,
	CAST(COALESCE(a1.measureidentifier, 'nomeasures') AS VARCHAR2(32)) utilizationidentifier,
	CAST(COALESCE(a1.measuredescription, 'No utilization measured in the census interval.') AS VARCHAR2(1024)) utilizationdescription,
	
	-- Last refresh
	CAST(a0.censoreddate AS DATE) censoreddate
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

ALTER TABLE hazardrates ADD CONSTRAINT primaryrates PRIMARY KEY (uliabphn, cornercase, intervalstart, intervalend, utilizationidentifier);