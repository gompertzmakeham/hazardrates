ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW personutilization NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT
	a0.uliabphn,
	a0.cornercase,
	a0.durationstart,
	a0.durationend,
	a1.visitminutes ambulatoryminutes,
	a1.visitcount ambulatoryvisits,
	a1.visitsitedays ambulatorysitedays,
	a1.visitdays ambulatorydays,
	a2.staydays inpatientdays,
	a2.admissioncount inpatientadmissions,
	a2.dischargecount inpatientdischarges,
	a2.intersectingstays inpatientstays,
	a2.assaycount laboratoryassays,
	a3.collectsitedays laboratorysitedays,
	a3.collectdays laboratorydays,
	a4.staydays longtermcarestays,
	a4.admissioncount longtermcareadmissions,
	a4.dischargecount longtermcaredischarges,
	a4.intersectingstays longtermcarestays,
	a5.standardtherapeutics pharmacystandardtherapeutics,
	a5.controlledtherapeutics pharmacycontrolledtherapeutics,
	a5.alltherapeutics pharmacytherapeutics,
	a5.standardsitedays pharmacystandardsitedays,
	a5.controlledsitedays pharmacycontrolledsitedays,
	a5.allsitedays pharmacysitedays,
	a5.standarddays pharmacystandarddays,
	a5.controlleddays pharmacycontrolleddays,
	a5.alldays pharmacydays,
	a6.anesthesiologyprocedures,
	a6.generalpracticeprocedures,
	a6.pathologyprocedures,
	a6.radiologyprocedures,
	a6.specialtyprocedures,
	a6.allprocedures primarycareprocedures,
	a6.anesthesiologistsdays,
	a6.generalpractitionersdays,
	a6.pathologistsdays,
	a6.radiologistsdays,
	a6.specialistsdays,
	a6.allproviderdays primarycareproviderdays,
	a6.anesthesiologydays,
	a6.generalpracticedays,
	a6.pathologydays,
	a6.radiologydays,
	a6.specialtydays,
	a6.alldays primarycaredays,
	a7.staydays supportivelivingdays,
	a7.admissioncount supportivelivingadmissions,
	a7.dischargecount supportivelivingdischarges,
	a7.intersectingstays supportivelivingstays
FROM
	personcensus a0
	LEFT JOIN
	censusambulatorycare a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend
	LEFT JOIN
	censusinpatientcare a2
	ON
		a0.uliabphn = a2.uliabphn
		AND
		a0.cornercase = a2.cornercase
		AND
		a0.intervalstart = a2.intervalstart
		AND
		a0.intervalend = a2.intervalend
	LEFT JOIN
	censuslaboratorycollection a3
	ON
		a0.uliabphn = a3.uliabphn
		AND
		a0.cornercase = a3.cornercase
		AND
		a0.intervalstart = a3.intervalstart
		AND
		a0.intervalend = a3.intervalend
	LEFT JOIN
	censuslongtermcare a4
	ON
		a0.uliabphn = a4.uliabphn
		AND
		a0.cornercase = a4.cornercase
		AND
		a0.intervalstart = a4.intervalstart
		AND
		a0.intervalend = a4.intervalend
	LEFT JOIN
	censuspharmacydispense a5
	ON
		a0.uliabphn = a5.uliabphn
		AND
		a0.cornercase = a5.cornercase
		AND
		a0.intervalstart = a5.intervalstart
		AND
		a0.intervalend = a5.intervalend
	LEFT JOIN
	censusprimarycare a6
	ON
		a0.uliabphn = a6.uliabphn
		AND
		a0.cornercase = a6.cornercase
		AND
		a0.intervalstart = a6.intervalstart
		AND
		a0.intervalend = a6.intervalend
	LEFT JOIN
	censussupportiveliving a7
	ON
		a0.uliabphn = a7.uliabphn
		AND
		a0.cornercase = a7.cornercase
		AND
		a0.intervalstart = a7.intervalstart
		AND
		a0.intervalend = a7.intervalend;
		
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