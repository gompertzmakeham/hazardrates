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
	a6.allprocedures,
	a6.anesthesiologistsdays,
	a6.generalpractitionersdays,
	a6.pathologistsdays,
	a6.radiologistsdays,
	a6.specialistsdays,
	a6.allproviderdays,
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