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
	CAST(a0.immigrateobservationequipoise AS INTEGER) immigrateobservationequipoise,

	-- Out migration observed flag is equal in both surveillance extremums
	CAST(a0.emigrateobservationequipoise AS INTEGER) emigrateobservationequipoise,

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

	-- Births
	CAST(COALESCE(a1.livenewborns, 0) AS INTEGER) livenewborns,

	-- Ambulatory care all utilization
	CAST(COALESCE(a1.ambulatoryminutes, 0) AS INTEGER) ambulatoryminutes,
	CAST(COALESCE(a1.ambulatoryvisits, 0) AS INTEGER) ambulatoryvisits,
	CAST(COALESCE(a1.ambulatorysitedays, 0) AS INTEGER) ambulatorysitedays,
	CAST(COALESCE(a1.ambulatorydays, 0) AS INTEGER) ambulatorydays,

	-- Ambulatory care private casualty utilization
	CAST(COALESCE(a1.ambulatoryprivateminutes, 0) AS INTEGER) ambulatoryprivateminutes,
	CAST(COALESCE(a1.ambulatoryprivatevisits, 0) AS INTEGER) ambulatoryprivatevisits,
	CAST(COALESCE(a1.ambulatoryprivatesitedays, 0) AS INTEGER) ambulatoryprivatesitedays,
	CAST(COALESCE(a1.ambulatoryprivatedays, 0) AS INTEGER) ambulatoryprivatedays,

	-- Ambulatory care workplace casualty utilization
	CAST(COALESCE(a1.ambulatoryworkminutes, 0) AS INTEGER) ambulatoryworkminutes,
	CAST(COALESCE(a1.ambulatoryworkvisits, 0) AS INTEGER) ambulatoryworkvisits,
	CAST(COALESCE(a1.ambulatoryworksitedays, 0) AS INTEGER) ambulatoryworksitedays,
	CAST(COALESCE(a1.ambulatoryworkdays, 0) AS INTEGER) ambulatoryworkdays,

	-- Care management
	CAST(COALESCE(a1.caremanagerdays, 0) AS INTEGER) caremanagerdays,
	CAST(COALESCE(a1.caremanagerallocations, 0) AS INTEGER) caremanagerallocations,
	CAST(COALESCE(a1.caremanagerreleases, 0) AS INTEGER) caremanagerreleases,
	CAST(COALESCE(a1.caremanagers, 0) AS INTEGER) caremanagers,

	-- Home care
	CAST(COALESCE(a1.homecareprofessionalservices, 0) AS INTEGER) homecareprofessionalservices,
	CAST(COALESCE(a1.homecaretransitionservices, 0) AS INTEGER) homecaretransitionservices,
	CAST(COALESCE(a1.homecareservices, 0) AS INTEGER) homecareservices,
	CAST(COALESCE(a1.homecareprofessionalvisits, 0) AS INTEGER) homecareprofessionalvisits,
	CAST(COALESCE(a1.homecaretransitionvisits, 0) AS INTEGER) homecaretransitionvisits,
	CAST(COALESCE(a1.homecarevisits, 0) AS INTEGER) homecarevisits,
	CAST(COALESCE(a1.homecareprofessionaldays, 0) AS INTEGER) homecareprofessionaldays,
	CAST(COALESCE(a1.homecaretransitiondays, 0) AS INTEGER) homecaretransitiondays,
	CAST(COALESCE(a1.homecaredays, 0) AS INTEGER) homecaredays,

	-- Inpatient care all utilization
	CAST(COALESCE(a1.inpatientdays, 0) AS INTEGER) inpatientdays,
	CAST(COALESCE(a1.inpatientadmissions, 0) AS INTEGER) inpatientadmissions,
	CAST(COALESCE(a1.inpatientdischarges, 0) AS INTEGER) inpatientdischarges,
	CAST(COALESCE(a1.inpatientstays, 0) AS INTEGER) inpatientstays,

	-- Inpatient care private casualty utilization
	CAST(COALESCE(a1.inpatientprivatedays, 0) AS INTEGER) inpatientprivatedays,
	CAST(COALESCE(a1.inpatientprivateadmissions, 0) AS INTEGER) inpatientprivateadmissions,
	CAST(COALESCE(a1.inpatientprivatedischarges, 0) AS INTEGER) inpatientprivatedischarges,
	CAST(COALESCE(a1.inpatientprivatestays, 0) AS INTEGER) inpatientprivatestays,

	-- Inpatient care workplace casualty utilization
	CAST(COALESCE(a1.inpatientworkdays, 0) AS INTEGER) inpatientworkdays,
	CAST(COALESCE(a1.inpatientworkadmissions, 0) AS INTEGER) inpatientworkadmissions,
	CAST(COALESCE(a1.inpatientworkdischarges, 0) AS INTEGER) inpatientworkdischarges,
	CAST(COALESCE(a1.inpatientworkstays, 0) AS INTEGER) inpatientworkstays,

	-- Laboratory collection
	CAST(COALESCE(a1.laboratoryassays, 0) AS INTEGER) laboratoryassays,
	CAST(COALESCE(a1.laboratorysitedays, 0) AS INTEGER) laboratorysitedays,
	CAST(COALESCE(a1.laboratorydays, 0) AS INTEGER) laboratorydays,

	-- Long term care
	CAST(COALESCE(a1.longtermcaredays, 0) AS INTEGER) longtermcaredays,
	CAST(COALESCE(a1.longtermcareadmissions, 0) AS INTEGER) longtermcareadmissions,
	CAST(COALESCE(a1.longtermcaredischarges, 0) AS INTEGER) longtermcaredischarges,
	CAST(COALESCE(a1.longtermcarestays, 0) AS INTEGER) longtermcarestays,

	/*
	 *  Pharmacy dispensing reports raw counts of dispensed therapeutics, unique combinations
	 *  of pharmacies and days, and unique days, for all dispensed therapeutics, standard
	 *  behind the counter therapeutics, and controlled therapeutics.
	 */

	-- Pharmacy dispensing all therapeutics
	CAST(COALESCE(a1.pharmacydailydoses, 0) AS INTEGER) pharmacydailydoses,
	CAST(COALESCE(a1.pharmacytherapeutics, 0) AS INTEGER) pharmacytherapeutics,
	CAST(COALESCE(a1.pharmacysitedays, 0) AS INTEGER) pharmacysitedays,
	CAST(COALESCE(a1.pharmacydays, 0) AS INTEGER) pharmacydays,

	-- Pharmacy dispensing standard behind the counter prescription therapeutics
	CAST(COALESCE(a1.pharmacystandarddailydoses, 0) AS INTEGER) pharmacystandarddailydoses,
	CAST(COALESCE(a1.pharmacystandardtherapeutics, 0) AS INTEGER) pharmacystandardtherapeutics,
	CAST(COALESCE(a1.pharmacystandardsitedays, 0) AS INTEGER) pharmacystandardsitedays,
	CAST(COALESCE(a1.pharmacystandarddays, 0) AS INTEGER) pharmacystandarddays,

	-- Pharmacy dispensing triple pad prescription controlled or regulated therpeutics
	CAST(COALESCE(a1.pharmacycontrolleddailydoses, 0) AS INTEGER) pharmacycontrolleddailydoses,
	CAST(COALESCE(a1.pharmacycontrolledtherapeutics, 0) AS INTEGER) pharmacycontrolledtherapeutics,
	CAST(COALESCE(a1.pharmacycontrolledsitedays, 0) AS INTEGER) pharmacycontrolledsitedays,
	CAST(COALESCE(a1.pharmacycontrolleddays, 0) AS INTEGER) pharmacycontrolleddays,

	/*
	 *  Primary care reports raw counts of procedures, unique combinations of days and
	 *  providers, and unique days, broken down by all providers, anesthesiology, general
	 *  practice, pathology, radiology, and specialists.
	 */

	-- Primary care all utilization
	CAST(COALESCE(a1.primarycareprocedures, 0) AS INTEGER) primarycareprocedures,
	CAST(COALESCE(a1.primarycareproviderdays, 0) AS INTEGER) primarycareproviderdays,
	CAST(COALESCE(a1.primarycaredays, 0) AS INTEGER) primarycaredays,

	-- Primary care anesthesiology utilization
	CAST(COALESCE(a1.anesthesiologyprocedures, 0) AS INTEGER) anesthesiologyprocedures,
	CAST(COALESCE(a1.anesthesiologistsdays, 0) AS INTEGER) anesthesiologistsdays,
	CAST(COALESCE(a1.anesthesiologydays, 0) AS INTEGER) anesthesiologydays,

	-- Primary care consult utilization
	CAST(COALESCE(a1.consultprocedures, 0) AS INTEGER) consultprocedures,
	CAST(COALESCE(a1.consultprovidersdays, 0) AS INTEGER) consultprovidersdays,
	CAST(COALESCE(a1.consultdays, 0) AS INTEGER) consultdays,

	-- Primary care general practice utilization
	CAST(COALESCE(a1.generalpracticeprocedures, 0) AS INTEGER) generalpracticeprocedures,
	CAST(COALESCE(a1.generalpractitionersdays, 0) AS INTEGER) generalpractitionersdays,
	CAST(COALESCE(a1.generalpracticedays, 0) AS INTEGER) generalpracticedays,

	-- Primary care geriatric utilization
	CAST(COALESCE(a1.geriatricprocedures, 0) AS INTEGER) geriatricprocedures,
	CAST(COALESCE(a1.geriatriciansdays, 0) AS INTEGER) geriatriciansdays,
	CAST(COALESCE(a1.geriatricdays, 0) AS INTEGER) geriatricdays,

	-- Primary care obstetric-gynecoloy utilization
	CAST(COALESCE(a1.obstetricprocedures, 0) AS INTEGER) obstetricprocedures,
	CAST(COALESCE(a1.obstetriciansdays, 0) AS INTEGER) obstetriciansdays,
	CAST(COALESCE(a1.obstetricdays, 0) AS INTEGER) obstetricdays,

	-- Primary care pathology utilization
	CAST(COALESCE(a1.pathologyprocedures, 0) AS INTEGER) pathologyprocedures,
	CAST(COALESCE(a1.pathologistsdays, 0) AS INTEGER) pathologistsdays,
	CAST(COALESCE(a1.pathologydays, 0) AS INTEGER) pathologydays,

	-- Primary care pediatric utilization
	CAST(COALESCE(a1.pediatricprocedures, 0) AS INTEGER) pediatricprocedures,
	CAST(COALESCE(a1.pediatriciansdays, 0) AS INTEGER) pediatriciansdays,
	CAST(COALESCE(a1.pediatricdays, 0) AS INTEGER) pediatricdays,

	-- Primary care pathology utilization
	CAST(COALESCE(a1.pediatricsurgicalprocedures, 0) AS INTEGER) pediatricsurgicalprocedures,
	CAST(COALESCE(a1.pediatricsurgeonsdays, 0) AS INTEGER) pediatricsurgeonsdays,
	CAST(COALESCE(a1.pediatricsurgerydays, 0) AS INTEGER) pediatricsurgerydays,

	-- Primary care psychiatry utilization
	CAST(COALESCE(a1.psychiatryprocedures, 0) AS INTEGER) psychiatryprocedures,
	CAST(COALESCE(a1.psychiatristsdays, 0) AS INTEGER) psychiatristsdays,
	CAST(COALESCE(a1.psychiatrydays, 0) AS INTEGER) psychiatrydays,
	
	-- Primary care radiology utilization
	CAST(COALESCE(a1.radiologyprocedures, 0) AS INTEGER) radiologyprocedures,
	CAST(COALESCE(a1.radiologistsdays, 0) AS INTEGER) radiologistsdays,
	CAST(COALESCE(a1.radiologydays, 0) AS INTEGER) radiologydays,
	
	-- Primary care specialist utilization
	CAST(COALESCE(a1.specialtyprocedures, 0) AS INTEGER) specialtyprocedures,
	CAST(COALESCE(a1.specialistsdays, 0) AS INTEGER) specialistsdays,
	CAST(COALESCE(a1.specialtydays, 0) AS INTEGER) specialtydays,
	
	-- Primary care surgeon utilization
	CAST(COALESCE(a1.surgicalprocedures, 0) AS INTEGER) surgicalprocedures,
	CAST(COALESCE(a1.surgeonsdays, 0) AS INTEGER) surgeonsdays,
	CAST(COALESCE(a1.surgerydays, 0) AS INTEGER) surgerydays,

	-- Supportive living
	CAST(COALESCE(a1.designateddays, 0) AS INTEGER) designateddays,
	CAST(COALESCE(a1.designatedadmissions, 0) AS INTEGER) designatedadmissions,
	CAST(COALESCE(a1.designateddischarges, 0) AS INTEGER) designateddischarges,
	CAST(COALESCE(a1.designatedstays, 0) AS INTEGER) designatedstays,
	CAST(COALESCE(a1.nondesignateddays, 0) AS INTEGER) nondesignateddays,
	CAST(COALESCE(a1.nondesignatedadmissions, 0) AS INTEGER) nondesignatedadmissions,
	CAST(COALESCE(a1.nondesignateddischarges, 0) AS INTEGER) nondesignateddischarges,
	CAST(COALESCE(a1.nondesignatedstays, 0) AS INTEGER) nondesignatedstays,
	CAST(COALESCE(a1.supportivelivingdays, 0) AS INTEGER) supportivelivingdays,
	CAST(COALESCE(a1.supportivelivingadmissions, 0) AS INTEGER) supportivelivingadmissions,
	CAST(COALESCE(a1.supportivelivingdischarges, 0) AS INTEGER) supportivelivingdischarges,
	CAST(COALESCE(a1.supportivelivingstays, 0) AS INTEGER) supportivelivingstays,
	
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
	ab_hzrd_rts_anlys.personutilization a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend;

ALTER TABLE hazardrates ADD CONSTRAINT primaryrates PRIMARY KEY (uliabphn, cornercase, intervalstart, intervalend);