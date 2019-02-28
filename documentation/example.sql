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

	-- Ambulatory care
	COALESCE(a1.ambulatoryminutes, 0) ambulatoryminutes,
	COALESCE(a1.ambulatoryvisits, 0) ambulatoryvisits,
	COALESCE(a1.ambulatorysitedays, 0) ambulatorysitedays,
	COALESCE(a1.ambulatorydays, 0) ambulatorydays,

	-- Inpatient care
	COALESCE(a1.inpatientdays, 0) inpatientdays,
	COALESCE(a1.inpatientadmissions, 0) inpatientadmissions,
	COALESCE(a1.inpatientdischarges, 0) inpatientdischarges,
	COALESCE(a1.inpatientstays, 0) inpatientstays,
	
	-- Laboratory collection
	COALESCE(a1.laboratoryassays, 0) laboratoryassays,
	COALESCE(a1.laboratorysitedays, 0) laboratorysitedays,
	COALESCE(a1.laboratorydays, 0) laboratorydays,

	-- Long term care
	COALESCE(a1.longtermcaredays, 0) longtermcaredays,
	COALESCE(a1.longtermcareadmissions, 0) longtermcareadmissions,
	COALESCE(a1.longtermcaredischarges, 0) longtermcaredischarges,
	COALESCE(a1.longtermcarestays, 0) longtermcarestays,

	/*
	 *  Pharmacy dispensing reports raw counts of dispensed therapeutics, unique combinations
	 *  of pharmacies and days, and unique days, for all dispensed therapeutics, standard
	 *  behind the counter therapeutics, and controlled therapeutics.
	 */

	-- Pharmacy dispensing all therapeutics
	COALESCE(a1.pharmacydailydoses, 0) pharmacydailydoses,
	COALESCE(a1.pharmacytherapeutics, 0) pharmacytherapeutics,
	COALESCE(a1.pharmacysitedays, 0) pharmacysitedays,
	COALESCE(a1.pharmacydays, 0) pharmacydays,

	-- Pharmacy dispensing standard behind the counter prescription therapeutics
	COALESCE(a1.pharmacystandarddailydoses, 0) pharmacystandarddailydoses,
	COALESCE(a1.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
	COALESCE(a1.pharmacystandardsitedays, 0) pharmacystandardsitedays,
	COALESCE(a1.pharmacystandarddays, 0) pharmacystandarddays,

	-- Pharmacy dispensing triple pad prescription controlled or regulated therpeutics
	COALESCE(a1.pharmacycontrolleddailydoses, 0) pharmacycontrolleddailydoses,
	COALESCE(a1.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
	COALESCE(a1.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
	COALESCE(a1.pharmacycontrolleddays, 0) pharmacycontrolleddays,

	/*
	 *  Primary care reports raw counts of procedures, unique combinations of days and
	 *  providers, and unique days, broken down by all providers, anesthesiology, general
	 *  practice, pathology, radiology, and specialists.
	 */

	-- Primary care all utilization
	COALESCE(a1.primarycareprocedures, 0) primarycareprocedures,
	COALESCE(a1.primarycareproviderdays, 0) primarycareproviderdays,
	COALESCE(a1.primarycaredays, 0) primarycaredays,

	-- Primary care anesthesiology utilization
	COALESCE(a1.anesthesiologyprocedures, 0) anesthesiologyprocedures,
	COALESCE(a1.anesthesiologistsdays, 0) anesthesiologistsdays,
	COALESCE(a1.anesthesiologydays, 0) anesthesiologydays,

	-- Primary care consult utilization
	COALESCE(a1.consultprocedures, 0) consultprocedures,
	COALESCE(a1.consultprovidersdays, 0) consultprovidersdays,
	COALESCE(a1.consultdays, 0) consultdays,

	-- Primary care general practice utilization
	COALESCE(a1.generalpracticeprocedures, 0) generalpracticeprocedures,
	COALESCE(a1.generalpractitionersdays, 0) generalpractitionersdays,
	COALESCE(a1.generalpracticedays, 0) generalpracticedays,

	-- Primary care pathology utilization
	COALESCE(a1.pathologyprocedures, 0) pathologyprocedures,
	COALESCE(a1.pathologistsdays, 0) pathologistsdays,
	COALESCE(a1.pathologydays, 0) pathologydays,
	
	-- Primary care radiology utilization
	COALESCE(a1.radiologyprocedures, 0) radiologyprocedures,
	COALESCE(a1.radiologistsdays, 0) radiologistsdays,
	COALESCE(a1.radiologydays, 0) radiologydays,
	
	-- Primary care specialist utilization
	COALESCE(a1.specialtyprocedures, 0) specialtyprocedures,
	COALESCE(a1.specialistsdays, 0) specialistsdays,
	COALESCE(a1.specialtydays, 0) specialtydays,
	
	-- Primary care surgeon utilization
	COALESCE(a1.surgicalprocedures, 0) surgicalprocedures,
	COALESCE(a1.surgeonsdays, 0) surgeonsdays,
	COALESCE(a1.surgerydays, 0) surgerydays,

	-- Supportive living
	COALESCE(a1.supportivelivingdays, 0) supportivelivingdays,
	COALESCE(a1.supportivelivingadmissions, 0) supportivelivingadmissions,
	COALESCE(a1.supportivelivingdischarges, 0) supportivelivingdischarges,
	COALESCE(a1.supportivelivingstays, 0) supportivelivingstays,
	
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
	ab_hzrd_rts_anlys.personutilization a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend;