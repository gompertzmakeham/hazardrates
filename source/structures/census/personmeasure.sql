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
			ambulatoryminutes,
			ambulatoryvisits,
			ambulatorysitedays,
			ambulatorydays,
			inpatientdays,
			inpatientadmissions,
			inpatientdischarges,
			inpatientstays,
			laboratoryassays,
			laboratorysitedays,
			laboratorydays,
			longtermcaredays,
			longtermcareadmissions,
			longtermcaredischarges,
			longtermcarestays,
			pharmacystandarddailydoses,
			pharmacycontrolleddailydoses,
			pharmacydailydoses,
			pharmacystandardtherapeutics,
			pharmacycontrolledtherapeutics,
			pharmacytherapeutics,
			pharmacystandardsitedays,
			pharmacycontrolledsitedays,
			pharmacysitedays,
			pharmacystandarddays,
			pharmacycontrolleddays,
			pharmacydays,
			anesthesiologyprocedures,
			consultprocedures,
			generalpracticeprocedures,
			obstetricprocedures,
			pathologyprocedures,
			radiologyprocedures,
			specialtyprocedures,
			surgicalprocedures,
			primarycareprocedures,
			anesthesiologistsdays,
			consultprovidersdays,
			generalpractitionersdays,
			obstetriciansdays,
			pathologistsdays,
			radiologistsdays,
			specialistsdays,
			surgeonsdays,
			primarycareproviderdays,
			anesthesiologydays,
			consultdays,
			generalpracticedays,
			obstetricdays,
			pathologydays,
			radiologydays,
			specialtydays,
			surgerydays,
			primarycaredays,
			supportivelivingdays,
			supportivelivingadmissions,
			supportivelivingdischarges,
			supportivelivingstays
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