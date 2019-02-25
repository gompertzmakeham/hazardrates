CREATE MATERIALIZED VIEW personutilization NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Link ambulatory to inpatient
	ambulatoryinpatient AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.visitminutes, 0) ambulatoryminutes,
			COALESCE(a0.visitcount, 0) ambulatoryvisits,
			COALESCE(a0.visitsitedays, 0) ambulatorysitedays,
			COALESCE(a0.visitdays, 0) ambulatorydays,
			COALESCE(a1.staydays, 0) inpatientdays,
			COALESCE(a1.admissioncount, 0) inpatientadmissions,
			COALESCE(a1.dischargecount, 0) inpatientdischarges,
			COALESCE(a1.intersectingstays, 0) inpatientstays
		FROM
			censusambulatorycare a0
			FULL JOIN
			censusinpatientcare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in laboratory
	addlaboratory AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a1.assaycount, 0) laboratoryassays,
			COALESCE(a1.collectsitedays, 0) laboratorysitedays,
			COALESCE(a1.collectdays, 0) laboratorydays
		FROM
			ambulatoryinpatient a0
			FULL JOIN
			censuslaboratorycollection a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in long term care
	addlongtermcare AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a1.staydays, 0) longtermcaredays,
			COALESCE(a1.admissioncount, 0) longtermcareadmissions,
			COALESCE(a1.dischargecount, 0) longtermcaredischarges,
			COALESCE(a1.intersectingstays, 0) longtermcarestays
		FROM
			addlaboratory a0
			FULL JOIN
			censuslongtermcare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in pharmacy
	addpharmacy AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcaredays, 0) longtermcaredays,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
			COALESCE(a1.standarddailydoses, 0) pharmacystandarddailydoses,
			COALESCE(a1.controlleddailydoses, 0) pharmacycontrolleddailydoses,
			COALESCE(a1.alldailydoses, 0) pharmacydailydoses,
			COALESCE(a1.standardtherapeutics, 0) pharmacystandardtherapeutics,
			COALESCE(a1.controlledtherapeutics, 0) pharmacycontrolledtherapeutics,
			COALESCE(a1.alltherapeutics, 0) pharmacytherapeutics,
			COALESCE(a1.standardsitedays, 0) pharmacystandardsitedays,
			COALESCE(a1.controlledsitedays, 0) pharmacycontrolledsitedays,
			COALESCE(a1.allsitedays, 0) pharmacysitedays,
			COALESCE(a1.standarddays, 0) pharmacystandarddays,
			COALESCE(a1.controlleddays, 0) pharmacycontrolleddays,
			COALESCE(a1.alldays, 0) pharmacydays
		FROM
			addlongtermcare a0
			FULL JOIN
			censuspharmacydispense a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in primary care
	addprimarycare AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a0.longtermcaredays, 0) longtermcaredays,
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
			COALESCE(a0.pharmacystandarddailydoses, 0) pharmacystandarddailydoses,
			COALESCE(a0.pharmacycontrolleddailydoses, 0) pharmacycontrolleddailydoses,
			COALESCE(a0.pharmacydailydoses, 0) pharmacydailydoses,
			COALESCE(a0.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
			COALESCE(a0.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
			COALESCE(a0.pharmacytherapeutics, 0) pharmacytherapeutics,
			COALESCE(a0.pharmacystandardsitedays, 0) pharmacystandardsitedays,
			COALESCE(a0.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
			COALESCE(a0.pharmacysitedays, 0) pharmacysitedays,
			COALESCE(a0.pharmacystandarddays, 0) pharmacystandarddays,
			COALESCE(a0.pharmacycontrolleddays, 0) pharmacycontrolleddays,
			COALESCE(a0.pharmacydays, 0) pharmacydays,
			COALESCE(a1.anesthesiologyprocedures, 0) anesthesiologyprocedures,
			COALESCE(a1.consultprocedures, 0) consultprocedures,
			COALESCE(a1.generalpracticeprocedures, 0) generalpracticeprocedures,
			COALESCE(a1.pathologyprocedures, 0) pathologyprocedures,
			COALESCE(a1.radiologyprocedures, 0) radiologyprocedures,
			COALESCE(a1.specialtyprocedures, 0) specialtyprocedures,
			COALESCE(a1.surgicalprocedures, 0) surgicalprocedures,
			COALESCE(a1.allprocedures, 0) primarycareprocedures,
			COALESCE(a1.anesthesiologistsdays, 0) anesthesiologistsdays,
			COALESCE(a1.consultprovidersdays, 0) consultprovidersdays,
			COALESCE(a1.generalpractitionersdays, 0) generalpractitionersdays,
			COALESCE(a1.pathologistsdays, 0) pathologistsdays,
			COALESCE(a1.radiologistsdays, 0) radiologistsdays,
			COALESCE(a1.specialistsdays, 0) specialistsdays,
			COALESCE(a1.surgeonsdays, 0) surgeonsdays,
			COALESCE(a1.allproviderdays, 0) primarycareproviderdays,
			COALESCE(a1.anesthesiologydays, 0) anesthesiologydays,
			COALESCE(a1.consultdays, 0) consultdays,
			COALESCE(a1.generalpracticedays, 0) generalpracticedays,
			COALESCE(a1.pathologydays, 0) pathologydays,
			COALESCE(a1.radiologydays, 0) radiologydays,
			COALESCE(a1.specialtydays, 0) specialtydays,
			COALESCE(a1.surgerydays, 0) surgerydays,
			COALESCE(a1.alldays, 0) primarycaredays
		FROM
			addpharmacy a0
			FULL JOIN
			censusprimarycare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	)

-- Final mix supportive living
SELECT
	CAST(COALESCE(a0.uliabphn, a1.uliabphn) AS INTEGER) uliabphn,
	CAST(COALESCE(a0.cornercase, a1.cornercase) AS VARCHAR2(1)) cornercase,
	CAST(COALESCE(a0.intervalstart, a1.intervalstart) AS DATE) intervalstart,
	CAST(COALESCE(a0.intervalend, a1.intervalend) AS DATE) intervalend,
	CAST(COALESCE(a0.ambulatoryminutes, 0) AS INTEGER) ambulatoryminutes,
	CAST(COALESCE(a0.ambulatoryvisits, 0) AS INTEGER) ambulatoryvisits,
	CAST(COALESCE(a0.ambulatorysitedays, 0) AS INTEGER) ambulatorysitedays,
	CAST(COALESCE(a0.ambulatorydays, 0) AS INTEGER) ambulatorydays,
	CAST(COALESCE(a0.inpatientdays, 0) AS INTEGER) inpatientdays,
	CAST(COALESCE(a0.inpatientadmissions, 0) AS INTEGER) inpatientadmissions,
	CAST(COALESCE(a0.inpatientdischarges, 0) AS INTEGER) inpatientdischarges,
	CAST(COALESCE(a0.inpatientstays, 0) AS INTEGER) inpatientstays,
	CAST(COALESCE(a0.laboratoryassays, 0) AS INTEGER) laboratoryassays,
	CAST(COALESCE(a0.laboratorysitedays, 0) AS INTEGER) laboratorysitedays,
	CAST(COALESCE(a0.laboratorydays, 0) AS INTEGER) laboratorydays,
	CAST(COALESCE(a0.longtermcaredays, 0) AS INTEGER) longtermcaredays,
	CAST(COALESCE(a0.longtermcareadmissions, 0) AS INTEGER) longtermcareadmissions,
	CAST(COALESCE(a0.longtermcaredischarges, 0) AS INTEGER) longtermcaredischarges,
	CAST(COALESCE(a0.longtermcarestays, 0) AS INTEGER) longtermcarestays,
	CAST(COALESCE(a0.pharmacystandarddailydoses, 0) AS INTEGER) pharmacystandarddailydoses,
	CAST(COALESCE(a0.pharmacycontrolleddailydoses, 0) AS INTEGER) pharmacycontrolleddailydoses,
	CAST(COALESCE(a0.pharmacydailydoses, 0) AS INTEGER) pharmacydailydoses,
	CAST(COALESCE(a0.pharmacystandardtherapeutics, 0) AS INTEGER) pharmacystandardtherapeutics,
	CAST(COALESCE(a0.pharmacycontrolledtherapeutics, 0) AS INTEGER) pharmacycontrolledtherapeutics,
	CAST(COALESCE(a0.pharmacytherapeutics, 0) AS INTEGER) pharmacytherapeutics,
	CAST(COALESCE(a0.pharmacystandardsitedays, 0) AS INTEGER) pharmacystandardsitedays,
	CAST(COALESCE(a0.pharmacycontrolledsitedays, 0) AS INTEGER) pharmacycontrolledsitedays,
	CAST(COALESCE(a0.pharmacysitedays, 0) AS INTEGER) pharmacysitedays,
	CAST(COALESCE(a0.pharmacystandarddays, 0) AS INTEGER) pharmacystandarddays,
	CAST(COALESCE(a0.pharmacycontrolleddays, 0) AS INTEGER) pharmacycontrolleddays,
	CAST(COALESCE(a0.pharmacydays, 0) AS INTEGER) pharmacydays,
	CAST(COALESCE(a0.anesthesiologyprocedures, 0) AS INTEGER) anesthesiologyprocedures,
	CAST(COALESCE(a0.consultprocedures, 0) AS INTEGER) consultprocedures,
	CAST(COALESCE(a0.generalpracticeprocedures, 0) AS INTEGER) generalpracticeprocedures,
	CAST(COALESCE(a0.pathologyprocedures, 0) AS INTEGER) pathologyprocedures,
	CAST(COALESCE(a0.radiologyprocedures, 0) AS INTEGER) radiologyprocedures,
	CAST(COALESCE(a0.specialtyprocedures, 0) AS INTEGER) specialtyprocedures,
	CAST(COALESCE(a0.surgicalprocedures, 0) AS INTEGER) surgicalprocedures,
	CAST(COALESCE(a0.primarycareprocedures, 0) AS INTEGER) primarycareprocedures,
	CAST(COALESCE(a0.anesthesiologistsdays, 0) AS INTEGER) anesthesiologistsdays,
	CAST(COALESCE(a0.consultprovidersdays, 0) AS INTEGER) consultprovidersdays,
	CAST(COALESCE(a0.generalpractitionersdays, 0) AS INTEGER) generalpractitionersdays,
	CAST(COALESCE(a0.pathologistsdays, 0) AS INTEGER) pathologistsdays,
	CAST(COALESCE(a0.radiologistsdays, 0) AS INTEGER) radiologistsdays,
	CAST(COALESCE(a0.specialistsdays, 0) AS INTEGER) specialistsdays,
	CAST(COALESCE(a0.surgeonsdays, 0) AS INTEGER) surgeonsdays,
	CAST(COALESCE(a0.primarycareproviderdays, 0) AS INTEGER) primarycareproviderdays,
	CAST(COALESCE(a0.anesthesiologydays, 0) AS INTEGER) anesthesiologydays,
	CAST(COALESCE(a0.consultdays, 0) AS INTEGER) consultdays,
	CAST(COALESCE(a0.generalpracticedays, 0) AS INTEGER) generalpracticedays,
	CAST(COALESCE(a0.pathologydays, 0) AS INTEGER) pathologydays,
	CAST(COALESCE(a0.radiologydays, 0) AS INTEGER) radiologydays,
	CAST(COALESCE(a0.specialtydays, 0) AS INTEGER) specialtydays,
	CAST(COALESCE(a0.surgerydays, 0) AS INTEGER) surgerydays,
	CAST(COALESCE(a0.primarycaredays, 0) AS INTEGER) primarycaredays,
	CAST(COALESCE(a1.staydays, 0) AS INTEGER) supportivelivingdays,
	CAST(COALESCE(a1.admissioncount, 0) AS INTEGER) supportivelivingadmissions,
	CAST(COALESCE(a1.dischargecount, 0) AS INTEGER) supportivelivingdischarges,
	CAST(COALESCE(a1.intersectingstays, 0) AS INTEGER) supportivelivingstays
FROM
	addprimarycare a0
	FULL JOIN
	censussupportiveliving a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend;

COMMENT ON MATERIALIZED VIEW personutilization IS 'For every person that at any time was covered by Alberta Healthcare Insurance partition the surviellance interval by the intersections of fiscal years and age years, rectified by the start and end of the surveillance interval.';
COMMENT ON COLUMN personutilization.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personutilization.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personutilization.intervalstart IS 'Closed start of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personutilization.intervalend IS 'Closed end of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personutilization.ambulatoryminutes IS 'Naive sum of ambulatory care minutes that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN personutilization.ambulatoryvisits IS 'Ambulatory care visits in the census interval.';
COMMENT ON COLUMN personutilization.ambulatorysitedays IS 'Unique combinations of days and ambulatory care sites visited in the census interval.';
COMMENT ON COLUMN personutilization.ambulatorydays IS 'Unique days of ambulatory care visits in the census interval.';
COMMENT ON COLUMN personutilization.inpatientdays IS 'Naive sum of inpatient care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.inpatientadmissions IS 'Inpatient care admissions in the census interval.';
COMMENT ON COLUMN personutilization.inpatientdischarges IS 'Inpatient care discharges in the census interval.';
COMMENT ON COLUMN personutilization.inpatientstays IS 'Inpatient care stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.laboratoryassays IS 'Number assays done of laboratory samples collected in the census interval.';
COMMENT ON COLUMN personutilization.laboratorysitedays IS 'Number unique combinations of laboratory collection sites and days in the census interval where the person had a collection taken.';
COMMENT ON COLUMN personutilization.laboratorydays IS 'Number of unique days in the census interval when the person had a laboratory collection taken.';
COMMENT ON COLUMN personutilization.longtermcaredays IS 'Naive sum of long term care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.longtermcareadmissions IS 'Long term care admissions in the census interval.';
COMMENT ON COLUMN personutilization.longtermcaredischarges IS 'Long term care discharges in the census interval.';
COMMENT ON COLUMN personutilization.longtermcarestays IS 'Long term care stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.pharmacystandarddailydoses IS 'Number of daily doses dispensed of therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolleddailydoses IS 'Number of daily doses dispensed of therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacydailydoses IS 'Number of daily doses dispensed of all therapeutics.';
COMMENT ON COLUMN personutilization.pharmacystandardtherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolledtherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacytherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN personutilization.pharmacystandardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacysitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN personutilization.pharmacystandarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacydays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN personutilization.anesthesiologyprocedures IS 'Number of procedures in the census interval delivered by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN personutilization.consultprocedures IS 'Number of procedures in the census interval delivered by a provider when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpracticeprocedures IS 'Number of procedures in the census interval delivered by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologyprocedures IS 'Number of procedures in the census interval delivered by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologyprocedures IS 'Number of procedures in the census interval delivered by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialtyprocedures IS 'Number of procedures in the census interval delivered by a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.surgicalprocedures IS 'Number of procedures in the census interval delivered by a surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycareprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN personutilization.anesthesiologistsdays IS 'Number of unique combinations of anesthesiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.consultprovidersdays IS 'Number of unique combinations of providers and days in the census interval when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpractitionersdays IS 'Number of unique combinations of general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologistsdays IS 'Number of unique combinations of pathologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologistsdays IS 'Number of unique combinations of radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialistsdays IS 'Number of unique combinations of specialists other than an anesthesiologists, general practitioners, pathologists, radiologists, or surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.surgeonsdays IS 'Number of unique combinations of surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycareproviderdays IS 'Number of unique combinations of providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN personutilization.anesthesiologydays IS 'Number of unique days in the census interval when an anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.consultdays IS 'Number of unique days in the census interval when either the provider role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpracticedays IS 'Number of unique days in the census interval when a general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologydays IS 'Number of unique days in the census interval when a pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologydays IS 'Number of unique days in the census interval when a radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialtydays IS 'Number of unique days in the census interval when a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN personutilization.surgerydays IS 'Number of unique days in the census interval when a surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycaredays IS 'Number of unique days in the census interval when the person visited primary care in the community.';
COMMENT ON COLUMN personutilization.supportivelivingdays IS 'Naive sum of supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.supportivelivingadmissions IS 'Supportive living admissions in the census interval.';
COMMENT ON COLUMN personutilization.supportivelivingdischarges IS 'Supportive living discharges in the census interval.';
COMMENT ON COLUMN personutilization.supportivelivingstays IS 'Supportive living stays intersecting with the census interval.';