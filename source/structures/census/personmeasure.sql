CREATE OR REPLACE VIEW personmeasure AS
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a0.intervalstart AS DATE) intervalstart,
	CAST(a0.intervalend AS DATE) intervalend,
	CAST(a1.measureidentifier AS VARCHAR2(32)) measureidentifier,
	CAST(a1.measuredescription AS VARCHAR2(1024)) measuredescription,
	CAST(a1.measurevalue AS INTEGER) measurevalue
FROM
	personutilization a0
	INNER JOIN
	TABLE
	(
		hazardutilities.generatemeasures
		(
			a0.livenewborns,
			a0.ambulatoryminutes,
			a0.ambulatoryvisits,
			a0.ambulatorysitedays,
			a0.ambulatorydays,
			a0.ambulatoryprivateminutes,
			a0.ambulatoryprivatevisits,
			a0.ambulatoryprivatesitedays,
			a0.ambulatoryprivatedays,
			a0.ambulatoryworkminutes,
			a0.ambulatoryworkvisits,
			a0.ambulatoryworksitedays,
			a0.ambulatoryworkdays,
			a0.inpatientdays,
			a0.inpatientadmissions,
			a0.inpatientdischarges,
			a0.inpatientstays,
			a0.inpatientprivatedays,
			a0.inpatientprivateadmissions,
			a0.inpatientprivatedischarges,
			a0.inpatientprivatestays,
			a0.inpatientworkdays,
			a0.inpatientworkadmissions,
			a0.inpatientworkdischarges,
			a0.inpatientworkstays,
			a0.caremanagerdays,
			a0.caremanagerallocations,
			a0.caremanagerreleases,
			a0.caremanagers,
			a0.homecareprofessionalservices,
			a0.homecaretransitionservices,
			a0.homecareservices,
			a0.homecareprofessionalvisits,
			a0.homecaretransitionvisits,
			a0.homecarevisits,
			a0.homecareprofessionaldays,
			a0.homecaretransitiondays,
			a0.homecaredays,
			a0.laboratoryassays,
			a0.laboratorysitedays,
			a0.laboratorydays,
			a0.longtermcaredays,
			a0.longtermcareadmissions,
			a0.longtermcaredischarges,
			a0.longtermcarestays,
			a0.pharmacystandarddailydoses,
			a0.pharmacycontrolleddailydoses,
			a0.pharmacydailydoses,
			a0.pharmacystandardtherapeutics,
			a0.pharmacycontrolledtherapeutics,
			a0.pharmacytherapeutics,
			a0.pharmacystandardsitedays,
			a0.pharmacycontrolledsitedays,
			a0.pharmacysitedays,
			a0.pharmacystandarddays,
			a0.pharmacycontrolleddays,
			a0.pharmacydays,
			a0.anesthesiologyprocedures,
			a0.consultprocedures,
			a0.generalpracticeprocedures,
			a0.geriatricprocedures,
			a0.obstetricprocedures,
			a0.pathologyprocedures,
			a0.pediatricprocedures,
			a0.pediatricsurgicalprocedures,
			a0.psychiatryprocedures,
			a0.radiologyprocedures,
			a0.specialtyprocedures,
			a0.surgicalprocedures,
			a0.primarycareprocedures,
			a0.anesthesiologistsdays,
			a0.consultprovidersdays,
			a0.generalpractitionersdays,
			a0.geriatriciansdays,
			a0.obstetriciansdays,
			a0.pathologistsdays,
			a0.pediatriciansdays,
			a0.pediatricsurgeonsdays,
			a0.psychiatristsdays,
			a0.radiologistsdays,
			a0.specialistsdays,
			a0.surgeonsdays,
			a0.primarycareproviderdays,
			a0.anesthesiologydays,
			a0.consultdays,
			a0.generalpracticedays,
			a0.geriatricdays,
			a0.obstetricdays,
			a0.pathologydays,
			a0.pediatricdays,
			a0.pediatricsurgerydays,
			a0.psychiatrydays,
			a0.radiologydays,
			a0.specialtydays,
			a0.surgerydays,
			a0.primarycaredays,
			a0.supportivelivingdays,
			a0.supportivelivingadmissions,
			a0.supportivelivingdischarges,
			a0.supportivelivingstays
		)
	) a1
	ON
		a1.measurevalue > 0
WITH READ ONLY;

COMMENT ON TABLE personmeasure IS 'Pivoted listing of utilization measures to one record per person per interval per utilization measure, zero measure records are elided.';
COMMENT ON COLUMN personmeasure.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personmeasure.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personmeasure.intervalstart IS 'Closed start of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personmeasure.intervalend IS 'Closed end of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personmeasure.measureidentifier IS 'Source column name of the utilization measure.';
COMMENT ON COLUMN personmeasure.measuredescription IS 'One sentence description of the utilization measure.';
COMMENT ON COLUMN personmeasure.measurevalue IS 'Value of the utilization measure, zero measure records are elided.';