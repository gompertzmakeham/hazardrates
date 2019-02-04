ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW personutilization NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
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
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
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
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
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
			COALESCE(a1.generalpracticeprocedures, 0) generalpracticeprocedures,
			COALESCE(a1.pathologyprocedures, 0) pathologyprocedures,
			COALESCE(a1.radiologyprocedures, 0) radiologyprocedures,
			COALESCE(a1.specialtyprocedures, 0) specialtyprocedures,
			COALESCE(a1.allprocedures, 0) primarycareprocedures,
			COALESCE(a1.anesthesiologistsdays, 0) anesthesiologistsdays,
			COALESCE(a1.generalpractitionersdays, 0) generalpractitionersdays,
			COALESCE(a1.pathologistsdays, 0) pathologistsdays,
			COALESCE(a1.radiologistsdays, 0) radiologistsdays,
			COALESCE(a1.specialistsdays, 0) specialistsdays,
			COALESCE(a1.allproviderdays, 0) primarycareproviderdays,
			COALESCE(a1.anesthesiologydays, 0) anesthesiologydays,
			COALESCE(a1.generalpracticedays, 0) generalpracticedays,
			COALESCE(a1.pahtologydays, 0) pathologydays,
			COALESCE(a1.radiologistdays, 0) radiologydays,
			COALESCE(a1.specialtydays, 0) specialtydays,
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
	COALESCE(a0.longtermcarestays, 0) longtermcarestays,
	COALESCE(a0.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
	COALESCE(a0.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
	COALESCE(a0.pharmacytherapeutics, 0) pharmacytherapeutics,
	COALESCE(a0.pharmacystandardsitedays, 0) pharmacystandardsitedays,
	COALESCE(a0.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
	COALESCE(a0.pharmacysitedays, 0) pharmacysitedays,
	COALESCE(a0.pharmacystandarddays, 0) pharmacystandarddays,
	COALESCE(a0.pharmacycontrolleddays, 0) pharmacycontrolleddays,
	COALESCE(a0.pharmacydays, 0) pharmacydays,
	COALESCE(a0.anesthesiologyprocedures, 0) anesthesiologyprocedures,
	COALESCE(a0.generalpracticeprocedures, 0) generalpracticeprocedures,
	COALESCE(a0.pathologyprocedures, 0) pathologyprocedures,
	COALESCE(a0.radiologyprocedures, 0) radiologyprocedures,
	COALESCE(a0.specialtyprocedures, 0) specialtyprocedures,
	COALESCE(a0.primarycareprocedures, 0) primarycareprocedures,
	COALESCE(a0.anesthesiologistsdays, 0) anesthesiologistsdays,
	COALESCE(a0.generalpractitionersdays, 0) generalpractitionersdays,
	COALESCE(a0.pathologistsdays, 0) pathologistsdays,
	COALESCE(a0.radiologistsdays, 0) radiologistsdays,
	COALESCE(a0.specialistsdays, 0) specialistsdays,
	COALESCE(a0.primarycareproviderdays, 0) primarycareproviderdays,
	COALESCE(a0.anesthesiologydays, 0) anesthesiologydays,
	COALESCE(a0.generalpracticedays, 0) generalpracticedays,
	COALESCE(a0.pathologydays, 0) pathologydays,
	COALESCE(a0.radiologydays, 0) radiologydays,
	COALESCE(a0.specialtydays, 0) specialtydays,
	COALESCE(a0.primarycaredays, 0) primarycaredays,
	COALESCE(a1.staydays, 0) supportivelivingdays,
	COALESCE(a1.admissioncount, 0) supportivelivingadmissions,
	COALESCE(a1.dischargecount, 0) supportivelivingdischarges,
	COALESCE(a1.intersectingstays, 0) supportivelivingstays
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
COMMENT ON COLUMN personutilization.durationend IS 'Closed end of the interval, rectified to the end of the surveillance interval.';
COMMENT ON COLUMN personutilization.durationdays IS 'Duration of the interval in days, an integer starting at 1, using the convention that the interval is closed so that the duration is end minus start plus one day.';
COMMENT ON COLUMN censusambulatorycare.ambulatoryminutes IS 'Naive sum of minutes that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN censusambulatorycare.ambulatoryvisits IS 'Visits in the census interval.';
COMMENT ON COLUMN censusambulatorycare.ambulatorysitedays IS 'Unique combinations of days and sites visited in the census interval.';
COMMENT ON COLUMN censusambulatorycare.ambulatorydays IS 'Unique days of visits in the census interval.';
COMMENT ON COLUMN censusinpatientcare.inpatientdays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.inpatientadmissions IS 'Admissions in the census interval.';
COMMENT ON COLUMN censusinpatientcare.inpatientdischarges IS 'Discharges in the census interval.';
COMMENT ON COLUMN censusinpatientcare.inpatientstays IS 'Stays intersecting with the census interval.';
COMMENT ON COLUMN censuslaboratorycollection.laboratoryassays IS 'Number assays done of samples collected in the census interval.';
COMMENT ON COLUMN censuslaboratorycollection.laboratorysitedays IS 'Number unique combinations of sites and days in the census interval where the person had a collection taken.';
COMMENT ON COLUMN censuslaboratorycollection.laboratorydays IS 'Number of unique days in the census interval when the person had a collection taken.';
COMMENT ON COLUMN censuslongtermcare.longtermcarestays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censuslongtermcare.longtermcareadmissions IS 'Admissions in the census interval.';
COMMENT ON COLUMN censuslongtermcare.longtermcaredischarges IS 'Discharges in the census interval.';
COMMENT ON COLUMN censuslongtermcare.longtermcarestays IS 'Stays intersecting with the census interval.';
COMMENT ON COLUMN censuspharmacydispense.pharmacystandardtherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacycontrolledtherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacytherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN censuspharmacydispense.pharmacystandardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacycontrolledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacysitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censuspharmacydispense.pharmacystandarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacycontrolleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.pharmacydays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censusprimarycare.anesthesiologyprocedures IS 'Number of procedures in the census interval provided by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticeprocedures IS 'Number of procedures in the census interval provided by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyprocedures IS 'Number of procedures in the census interval provided by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyprocedures IS 'Number of procedures in the census interval provided by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtyprocedures IS 'Number of procedures in the census interval provided by a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.primarycareprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN censusprimarycare.anesthesiologistsdays IS 'Number of unique combinations of anesthesiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpractitionersdays IS 'Number of unique combinations of general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologistsdays IS 'Number of unique combinations of pathologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologistsdays IS 'Number of unique combinations of radiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistsdays IS 'Number of unique combinations of specialists other than an anesthesiologists, general practitioners, pathologists or radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.primarycareproviderdays IS 'Number of unique combinations of providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN censusprimarycare.anesthesiologydays IS 'Number of unique days in the census interval when an anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticedays IS 'Number of unique days in the census interval when a general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologydays IS 'Number of unique days in the census interval when a pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologydays IS 'Number of unique days in the census interval when a radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtydays IS 'Number of unique days in the census interval when a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN censusprimarycare.primarycaredays IS 'Number of unique days in the census interval when the person visited primary care in the community.';
COMMENT ON COLUMN censussupportiveliving.supportivelivingdays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censussupportiveliving.supportivelivingadmissions IS 'Admissions in the census interval.';
COMMENT ON COLUMN censussupportiveliving.supportivelivingdischarges IS 'Discharges in the census interval.';
COMMENT ON COLUMN censussupportiveliving.supportivelivingstays IS 'Stays intersecting with the census interval.';