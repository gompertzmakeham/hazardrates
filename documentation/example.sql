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
	a1.cornercase,
	
	-- Start and end of life (obviously), deceased date is null when not observed
	a1.birthdate,
	a1.deceaseddate,

	-- Extent of surveillance observation
	a0.surveillancestart,
	a0.surveillanceend,

	-- Surveillance interval rectified by birth, deceased, and censored dates
	a1.extremumstart,
	a1.extremumend,

	/*
	 *  Comparisons of the two surveillance extremums within the same person.
	 */

	-- Least and greatest birth dates are in the same fiscal year
	a1.ageequipoise,

	-- Least and greatest birth dates are equal
	a1.birthdateequipoise,

	-- Least and greatest deceased dates are equal
	a1.deceaseddateequipoise,

	-- Birth observed flag is equal in both surveillance extremums
	a1.birthobservationequipoise,

	-- Death observed flag is equal in both surveillance extremums
	a1.deceasedobservationequipoise,

	-- Least and greatest immigration dates are equal
	a1.immigratedateequipoise,

	-- Least and greatest emigration dates are equal
	a1.emigratedateequipoise,

	-- In migration observed flag is equal in both surveillance extremums
	a1.immigrateobservationequipoise,

	-- Out migration observed flag is equal in both surveillance extremums
	a1.emigrateobservationequipoise,

	-- Surveillance extremum start dates are equal
	a1.surveillancestartequipoise,

	-- Surveillance extremum end dates are equal
	a1.surveillanceendequipoise,

	/*
	 *  Census interval properties, the duration is used as the denominator.
	 */

	-- Does the unrectified intersection start on the birthday
	a2.agecoincideinterval,

	-- Does the birthday fall on the start of the fiscal year
	a2.agecoincidecensus,

	-- The start and end of the fiscal year
	a2.censusstart,
	a2.censusend,

	-- The start and end of the person's age year, with the age specified in interval age
	a2.agestart,
	a2.ageend,

	-- The start and end of the intersection of the fiscal year and the person's age year
	a2.intervalstart,
	a2.intervalend,

	-- The intersection rectified by the start and end of the surveillance interval
	a2.durationstart,
	a2.durationend,

	/****************************************************************************************
	 *                                                                                      *
	 * LOOK NO FURTHER! THIS IS YOUR HAZARD RATE DENOMINATOR! SUM THIS WITHIN EACH CORNER   *
	 * CASE! TYPICALLY DIVIDE THE SUM BY 365.25 TO REPORT IN UNITS OF AMOUNT PER PERSON     *
	 * YEAR!                                                                                *
	 *                                                                                      *
	 ****************************************************************************************/
	a2.durationdays,

	-- The age of the person's age year that intersects with the interval
	a2.intervalage,

	-- Birth was observed
	a2.intervalbirth,

	-- Death was observed
	a2.intervaldeceased,

	-- In migration was observed
	a2.intervalimmigrate,

	-- Out migration was observed
	a2.intervalemigrate,

	-- Is this the first census interval
	a2.intervalfirst,

	-- Is this the last census interval
	a2.intervallast,

	-- Total number of census intervals in the partition of the surveillance interval
	a2.intervalcount,

	-- Order of the census interval in the partition of the surveillance interval
	a2.intervalorder,

	/*
	 *  Utilization in the census intervals, used as the numerators.
	 */

	-- Ambulatory care
	COALESCE(a3.ambulatoryminutes, 0) ambulatoryminutes,
	COALESCE(a3.ambulatoryvisits, 0) ambulatoryvisits,
	COALESCE(a3.ambulatorysitedays, 0) ambulatorysitedays,
	COALESCE(a3.ambulatorydays, 0) ambulatorydays,

	-- Inpatient care
	COALESCE(a3.inpatientdays, 0) inpatientdays,
	COALESCE(a3.inpatientadmissions, 0) inpatientadmissions,
	COALESCE(a3.inpatientdischarges, 0) inpatientdischarges,
	COALESCE(a3.inpatientstays, 0) inpatientstays,
	
	-- Laboratory collection
	COALESCE(a3.laboratoryassays, 0) laboratoryassays,
	COALESCE(a3.laboratorysitedays, 0) laboratorysitedays,
	COALESCE(a3.laboratorydays, 0) laboratorydays,

	-- Long term care
	COALESCE(a3.longtermcaredays, 0) longtermcaredays,
	COALESCE(a3.longtermcareadmissions, 0) longtermcareadmissions,
	COALESCE(a3.longtermcaredischarges, 0) longtermcaredischarges,
	COALESCE(a3.longtermcarestays, 0) longtermcarestays,

	/*
	 *  Pharmacy dispensing reports raw counts of dispensed therapeutics, unique combinations
	 *  of pharmacies and days, and unique days, for all dispensed therapeutics, standard
	 *  behind the counter therapeutics, and controlled therapeutics.
	 */

	-- Pharmacy dispensing all therapeutics
	COALESCE(a3.pharmacydailydoses, 0) pharmacydailydoses,
	COALESCE(a3.pharmacytherapeutics, 0) pharmacytherapeutics,
	COALESCE(a3.pharmacysitedays, 0) pharmacysitedays,
	COALESCE(a3.pharmacydays, 0) pharmacydays,

	-- Pharmacy dispensing standard behind the counter prescription therapeutics
	COALESCE(a3.pharmacystandarddailydoses, 0) pharmacystandarddailydoses,
	COALESCE(a3.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
	COALESCE(a3.pharmacystandardsitedays, 0) pharmacystandardsitedays,
	COALESCE(a3.pharmacystandarddays, 0) pharmacystandarddays,

	-- Pharmacy dispensing triple pad prescription controlled or regulated therpeutics
	COALESCE(a3.pharmacycontrolleddailydoses, 0) pharmacycontrolleddailydoses,
	COALESCE(a3.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
	COALESCE(a3.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
	COALESCE(a3.pharmacycontrolleddays, 0) pharmacycontrolleddays,

	/*
	 *  Primary care reports raw counts of procedures, unique combinations of days and
	 *  providers, and unique days, broken down by all providers, anesthesiology, general
	 *  practice, pathology, radiology, and specialists.
	 */

	-- Primary care all utilization
	COALESCE(a3.primarycareprocedures, 0) primarycareprocedures,
	COALESCE(a3.primarycareproviderdays, 0) primarycareproviderdays,
	COALESCE(a3.primarycaredays, 0) primarycaredays,

	-- Primary care anesthesiology utilization
	COALESCE(a3.anesthesiologyprocedures, 0) anesthesiologyprocedures,
	COALESCE(a3.anesthesiologistsdays, 0) anesthesiologistsdays,
	COALESCE(a3.anesthesiologydays, 0) anesthesiologydays,

	-- Primary care consult utilization
	COALESCE(a3.consultprocedures, 0) consultprocedures,
	COALESCE(a3.consultprovidersdays, 0) consultprovidersdays,
	COALESCE(a3.consultdays, 0) consultdays,

	-- Primary care general practice utilization
	COALESCE(a3.generalpracticeprocedures, 0) generalpracticeprocedures,
	COALESCE(a3.generalpractitionersdays, 0) generalpractitionersdays,
	COALESCE(a3.generalpracticedays, 0) generalpracticedays,

	-- Primary care pathology utilization
	COALESCE(a3.pathologyprocedures, 0) pathologyprocedures,
	COALESCE(a3.pathologistsdays, 0) pathologistsdays,
	COALESCE(a3.pathologydays, 0) pathologydays,
	
	-- Primary care radiology utilization
	COALESCE(a3.radiologyprocedures, 0) radiologyprocedures,
	COALESCE(a3.radiologistsdays, 0) radiologistsdays,
	COALESCE(a3.radiologydays, 0) radiologydays,
	
	-- Primary care specialist utilization
	COALESCE(a3.specialtyprocedures, 0) specialtyprocedures,
	COALESCE(a3.specialistsdays, 0) specialistsdays,
	COALESCE(a3.specialtydays, 0) specialtydays,
	
	-- Primary care surgeon utilization
	COALESCE(a3.surgicalprocedures, 0) surgicalprocedures,
	COALESCE(a3.surgeonsdays, 0) surgeonsdays,
	COALESCE(a3.surgerydays, 0) surgerydays,

	-- Supportive living
	COALESCE(a3.supportivelivingdays, 0) supportivelivingdays,
	COALESCE(a3.supportivelivingadmissions, 0) supportivelivingadmissions,
	COALESCE(a3.supportivelivingdischarges, 0) supportivelivingdischarges,
	COALESCE(a3.supportivelivingstays, 0) supportivelivingstays,
	
	-- Last refresh
	a0.censoreddate
FROM

	-- Initial listing of all persons
	ab_hzrd_rts_anlys.personsdemographic a0
	INNER JOIN
	
	-- Every person has two surveillance intervals, representing the extermums of the 
	-- possible observations of age. Report only persons covered by AHCIP, with a birthdate
	ab_hzrd_rts_anlys.personsurveillance a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.albertacoverage = 1
		AND
		a0.leastbirth IS NOT NULL
		AND
		a0.greatestbirth IS NOT NULL
	INNER JOIN
	
	-- Each surveillance interval is partitioned into census intervals, a pair for each fiscal
	-- year, the interval before the birthday, and the interval after. This is the denominator
	-- data in the hazard rates.
	ab_hzrd_rts_anlys.personcensus a2
	ON
		a0.uliabphn = a2.uliabphn
		AND
		a1.cornercase = a2.cornercase
	LEFT JOIN
	
	-- Most, but not all, census intervals will have some form of utilization. This is the
	-- numerator in the hazard rates.
	ab_hzrd_rts_anlys.personutilization a3
	ON
		a0.uliabphn = a3.uliabphn
		AND
		a2.cornercase = a3.cornercase
		AND
		a2.intervalstart = a3.intervalstart
		AND
		a2.intervalend = a3.intervalend;