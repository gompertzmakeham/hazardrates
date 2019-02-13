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

	-- Extent of surveillance observation
	a0.surveillancestart,
	a0.surveillanceend,

	-- Surveillance interval rectified by birth, deceased, and censored dates
	a0.extremumstart,
	a0.extremumend,

	/*
	 *  Comparisons of the two surveillance extremums within the same person.
	 */

	-- Least and greatest birth dates are equal
	a0.birthdateequipoise,

	-- Least and greatest birth dates are in the same fiscal year
	a0.ageequipoise,

	-- Least and greatest deceased dates are equal
	a0.deceaseddateequipoise,

	-- Birth observed flag is equal in both surveillance extremums
	a0.birthequipoise,

	-- Death observed flag is equal in both surveillance extremums
	a0.deceasedequipoise,

	-- In migration observed flag is equal in both surveillance extremums
	a0.immigrateequipoise,

	-- Out migration observed flag is equal in both surveillance extremums
	a0.emigrateequipoise,

	-- Surveillance extremum start dates are equal
	a0.startequipoise,

	-- Surveillance extremum end dates are equal
	a0.endequipoise,

	/*
	 *  Census interval properties, the duration is used as the denominator.
	 */

	-- Does the unrectified intersection start on the birthday
	a1.agecoincideinterval,

	-- Does the birthday fall on the start of the fiscal year
	a1.agecoincidecensus,

	-- The start and end of the fiscal year
	a1.censusstart,
	a1.censusend,

	-- The start and end of the person's age year, with the age specified in interval age
	a1.agestart,
	a1.ageend,

	-- The start and end of the intersection of the fiscal year and the person's age year
	a1.intervalstart,
	a1.intervalend,

	-- The intersection rectified by the start and end of the surveillance interval
	a1.durationstart,
	a1.durationend,

	/****************************************************************************************
	 *                                                                                      *
	 * LOOK NO FURTHER! THIS IS YOUR HAZARD RATE DENOMINATOR! SUM THIS WITHIN EACH CORNER   *
	 * CASE! TYPICALLY DIVIDE THE SUM BY 365.25 TO REPORT IN UNITS OF AMOUNT PER PERSON     *
	 * YEAR!                                                                                *
	 *                                                                                      *
	 ****************************************************************************************/
	a1.durationdays,

	-- The age of the person's age year that intersects with the interval
	a1.intervalage,

	-- Birth was observed
	a1.intervalbirth,

	-- Death was observed
	a1.intervaldeceased,

	-- In migration was observed
	a1.intervalimmigrate,

	-- Out migration was observed
	a1.intervalemigrate,

	-- Is this the first census interval
	a1.intervalfirst,

	-- Is this the last census interval
	a1.intervallast,

	-- Total number of census intervals in the partition of the surveillance interval
	a1.intervalcount,

	-- Order of the census interval in the partition of the surveillance interval
	a1.intervalorder,

	/*
	 *  Utilization in the census intervals, used as the numerators.
	 */

	-- Ambulatory care
	COALESCE(a2.ambulatoryminutes, 0) ambulatoryminutes,
	COALESCE(a2.ambulatoryvisits, 0) ambulatoryvisits,
	COALESCE(a2.ambulatorysitedays, 0) ambulatorysitedays,
	COALESCE(a2.ambulatorydays, 0) ambulatorydays,

	-- Inpatient care
	COALESCE(a2.inpatientdays, 0) inpatientdays,
	COALESCE(a2.inpatientadmissions, 0) inpatientadmissions,
	COALESCE(a2.inpatientdischarges, 0) inpatientdischarges,
	COALESCE(a2.inpatientstays, 0) inpatientstays,
	
	-- Laboratory collection
	COALESCE(a2.laboratoryassays, 0) laboratoryassays,
	COALESCE(a2.laboratorysitedays, 0) laboratorysitedays,
	COALESCE(a2.laboratorydays, 0) laboratorydays,

	-- Long term care
	COALESCE(a2.longtermcaredays, 0) longtermcaredays,
	COALESCE(a2.longtermcareadmissions, 0) longtermcareadmissions,
	COALESCE(a2.longtermcaredischarges, 0) longtermcaredischarges,
	COALESCE(a2.longtermcarestays, 0) longtermcarestays,

	/*
	 *  Pharmacy dispensing reports raw counts of dispensed therapeutics, unique combinations
	 *  of pharmacies and days, and unique days, for all dispensed therapeutics, standard
	 *  behind the counter therapeutics, and controlled therapeutics.
	 */

	-- Pharmacy dispensing all therapeutics
	COALESCE(a2.pharmacydailydoses, 0) pharmacydailydoses,
	COALESCE(a2.pharmacytherapeutics, 0) pharmacytherapeutics,
	COALESCE(a2.pharmacysitedays, 0) pharmacysitedays,
	COALESCE(a2.pharmacydays, 0) pharmacydays,

	-- Pharmacy dispensing standard behind the counter prescription therapeutics
	COALESCE(a2.pharmacydailydoses, 0) pharmacystandarddailydoses,
	COALESCE(a2.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
	COALESCE(a2.pharmacystandardsitedays, 0) pharmacystandardsitedays,
	COALESCE(a2.pharmacystandarddays, 0) pharmacystandarddays,

	-- Pharmacy dispensing triple pad prescription controlled or regulated therpeutics
	COALESCE(a2.pharmacydailydoses, 0) pharmacycontrolleddailydoses,
	COALESCE(a2.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
	COALESCE(a2.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
	COALESCE(a2.pharmacycontrolleddays, 0) pharmacycontrolleddays,

	/*
	 *  Primary care reports raw counts of procedures, unique combinations of days and
	 *  providers, and unique days, broken down by all providers, anesthesiology, general
	 *  practice, pathology, radiology, and specialists.
	 */

	-- Primary care all utilization
	COALESCE(a2.primarycareprocedures, 0) primarycareprocedures,
	COALESCE(a2.primarycareproviderdays, 0) primarycareproviderdays,
	COALESCE(a2.primarycaredays, 0) primarycaredays,

	-- Primary care anesthesiology utilization
	COALESCE(a2.anesthesiologyprocedures, 0) anesthesiologyprocedures,
	COALESCE(a2.anesthesiologistsdays, 0) anesthesiologistsdays,
	COALESCE(a2.anesthesiologydays, 0) anesthesiologydays,

	-- Primary care general practice utilization
	COALESCE(a2.generalpracticeprocedures, 0) generalpracticeprocedures,
	COALESCE(a2.generalpractitionersdays, 0) generalpractitionersdays,
	COALESCE(a2.generalpracticedays, 0) generalpracticedays,

	-- Primary care pathology utilization
	COALESCE(a2.pathologyprocedures, 0) pathologyprocedures,
	COALESCE(a2.pathologistsdays, 0) pathologistsdays,
	COALESCE(a2.pathologydays, 0) pathologydays,
	
	-- Primary care radiology utilization
	COALESCE(a2.radiologyprocedures, 0) radiologyprocedures,
	COALESCE(a2.radiologistsdays, 0) radiologistsdays,
	COALESCE(a2.radiologydays, 0) radiologydays,
	
	-- Primary care specialist utilization
	COALESCE(a2.specialtyprocedures, 0) specialtyprocedures,
	COALESCE(a2.specialistsdays, 0) specialistsdays,
	COALESCE(a2.specialtydays, 0) specialtydays,

	-- Supportive living
	COALESCE(a2.supportivelivingdays, 0) supportivelivingdays,
	COALESCE(a2.supportivelivingadmissions, 0) supportivelivingadmissions,
	COALESCE(a2.supportivelivingdischarges, 0) supportivelivingdischarges,
	COALESCE(a2.supportivelivingstays, 0) supportivelivingstays,
	
	-- Last refresh
	a0.censoreddate
FROM

	-- Every person has two surveillance intervals, representing the extermums of the 
	-- possible observations of age.
	hazardrates.personsurveillance a0
	INNER JOIN
	
	-- Each surveillance interval is partitioned into census intervals, a pair for each fiscal
	-- year, the interval before the birthday, and the interval after. This is the denominator
	-- data in the hazard rates.
	hazardrates.personcensus a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
	LEFT JOIN
	
	-- Most, but not all, census intervals will have some form of utilization. This is the
	-- numerator in the hazard rates.
	hazardrates.personutilization a2
	ON
		a1.uliabphn = a2.uliabphn
		AND
		a1.cornercase = a2.cornercase
		AND
		a1.intervalstart = a2.intervalstart
		AND
		a1.intervalend = a2.intervalend;