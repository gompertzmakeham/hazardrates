ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censusprimarycare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest primary care
	eventdata AS
	(
		SELECT
			a0.rcpt_uli uliabphn,
			COALESCE
			(
				hazardutilities.cleanprid(a0.prvd_prid),
				hazardutilities.cleanprid(a1.prvd_prid)
			) uliabprid,
			a0.se_start_date visitdate,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'ANES' THEN
					1
				ELSE
					0
			END anesthesiologist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'GP' THEN
					1
				ELSE
					0
			END generalpractitioner,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'PATH' THEN
					1
				ELSE
					0
			END pathologist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'DIRD' THEN
					1
				ELSE
					0
			END radiologist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad IN ('ANES', 'DIRD', 'GP', 'PATH') THEN
					0
				ELSE
					1
			END specialist,
			CASE
				WHEN a0.prvd_role_type_cls = 'PROC' THEN
					1
				ELSE
					0
			END primaryprovider,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = a0.pers_capb_prvd_skill_code_cls THEN
					1
				ELSE
					0
			END primaryskill
		FROM
			ahsdata.ab_claims a0
			LEFT JOIN
			ahsdata.ab_claim_ah_ref_rnd_id a1
			ON
				a0.rnd_id = a1.rnd_id
		WHERE
			COALESCE
			(
				hazardutilities.cleanprid(a0.prvd_prid),
				hazardutilities.cleanprid(a1.prvd_prid)
			) IS NOT NULL
			AND
			COALESCE(a0.pgm_app_ind, 'F') = 'F'
			AND
			a0.fre_actual_paid_amt > 0
			AND
			a0.prvd_in_prov_ind_ad = 'I'
			AND
			a0.hsp_displn_type_cls = 'MEDDS'
			AND
			a0.pgm_subtype_cls IN ('BASCMEDC', 'BASCMEDE', 'BASCMEMS')
			AND
			a0.delv_site_unreg_type_code IS NULL
			AND
			a0.delv_site_type_cls = a0.delv_site_fac_type_code
			AND
			(
				a0.delv_site_functr_type_code = 'POFF'
				OR
				a0.delv_site_functr_code_cls = 'FCC'
				OR
				(a0.delv_site_functr_type_code = 'AMBU' AND a0.delv_site_functr_code_cls = 'CLNC')
				OR
				(
					a0.delv_site_functr_type_code = 'DGTS'
					AND
					a0.delv_site_functr_code_cls IN ('CLAB', 'DIMG', 'ELEC')
					AND
					a0.delv_site_type_cls IN ('DIAG', 'OFFC')
				)
			)
	),

	-- Digest to one record per patient per provider per day
	providerdata AS
	(
		SELECT
			a0.uliabphn,
			a0.uliabprid,
			a0.visitdate,
			MAX(a0.anesthesiologist * a0.primaryprovider * a0.primaryskill) anesthesiologist,
			MAX(a0.generalpractitioner * a0.primaryprovider * a0.primaryskill) generalpractitioner,
			MAX(a0.pathologist * a0.primaryprovider * a0.primaryskill) pathologist,
			MAX(a0.radiologist * a0.primaryprovider * a0.primaryskill) radiologist,
			MAX(a0.specialist * a0.primaryprovider * a0.primaryskill) specialist,
			SUM(a0.anesthesiologist * a0.primaryprovider * a0.primaryskill) anesthesiologyprocedures,
			SUM(a0.generalpractitioner * a0.primaryprovider * a0.primaryskill) generalpracticeprocedures,
			SUM(a0.pathologist * a0.primaryprovider * a0.primaryskill) pathologyprocedures,
			SUM(a0.radiologist * a0.primaryprovider * a0.primaryskill) radiologyprocedures,
			SUM(a0.specialist * a0.primaryprovider * a0.primaryskill) specialtyprocedures,
			COUNT(*) allprocedures
		FROM
			eventdata a0
		GROUP BY
			a0.uliabphn,
			a0.uliabprid,
			a0.visitdate
	),

	-- Digest to one record per patient per day
	daydata AS
	(
		SELECT
			a0.uliabphn,
			a0.visitdate,
			MAX(a0.anesthesiologist) anesthesiologist,
			MAX(a0.generalpractitioner) generalpractitioner,
			MAX(a0.pathologist) pathologist,
			MAX(a0.radiologist) radiologist,
			MAX(a0.specialist) specialist,
			SUM(a0.anesthesiologyprocedures) anesthesiologyprocedures,
			SUM(a0.generalpracticeprocedures) generalpracticeprocedures,
			SUM(a0.pathologyprocedures) pathologyprocedures,
			SUM(a0.radiologyprocedures) radiologyprocedures,
			SUM(a0.specialtyprocedures) specialtyprocedures,
			SUM(a0.allprocedures) allprocedures,
			SUM(a0.anesthesiologist) anesthesiologists,
			SUM(a0.generalpractitioner) generalpractitioners,
			SUM(a0.pathologist) pathologists,
			SUM(a0.radiologist) radiologists,
			SUM(a0.specialist) specialists,
			COUNT(*) allproviders
		FROM
			providerdata a0
		GROUP BY
			a0.uliabphn,
			a0.visitdate
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a1.anesthesiologyprocedures) anesthesiologyprocedures,
	SUM(a1.generalpracticeprocedures) generalpracticeprocedures,
	SUM(a1.pathologyprocedures) pathologyprocedures,
	SUM(a1.radiologyprocedures) radiologyprocedures,
	SUM(a1.specialtyprocedures) specialtyprocedures,
	SUM(a1.allprocedures) allprocedures,
	SUM(a1.anesthesiologists) anesthesiologistsdays,
	SUM(a1.generalpractitioners) generalpractitionersdays,
	SUM(a1.pathologists) pathologistsdays,
	SUM(a1.radiologists) radiologistsdays,
	SUM(a1.specialists) specialistsdays,
	SUM(a1.allproviders) allproviderdays,
	SUM(a1.anesthesiologist) anesthesiologydays,
	SUM(a1.generalpractitioner) generalpracticedays,
	SUM(a1.pathologist) pathologydays,
	SUM(a1.radiologist) radiologydays,
	SUM(a1.specialist) specialtydays,
	COUNT(*) alldays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a1.visitdate BETWEEN a0.extremumstart AND a0.extremumend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.visitdate, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;
	
COMMENT ON MATERIALIZED VIEW censusprimarycare IS 'Utilization of community primary care in census intervals of each person, including physician offices, diagnostic services, day surgery, and anesthesiology.';
COMMENT ON COLUMN censusprimarycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusprimarycare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusprimarycare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.anesthesiologyprocedures IS 'Number of procedures in the census interval provided by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticeprocedures IS 'Number of procedures in the census interval provided by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyprocedures IS 'Number of procedures in the census interval provided by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyprocedures IS 'Number of procedures in the census interval provided by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtyprocedures IS 'Number of procedures in the census interval provided by a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN censusprimarycare.anesthesiologistsdays IS 'Number of unique combinations of anesthesiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpractitionersdays IS 'Number of unique combinations of general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologistsdays IS 'Number of unique combinations of pathologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologistsdays IS 'Number of unique combinations of radiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistsdays IS 'Number of unique combinations of specialists other than an anesthesiologists, general practitioners, pathologists or radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allproviderdays IS 'Number of unique combinations of providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN censusprimarycare.anesthesiologydays IS 'Number of unique days in the census interval when an anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticedays IS 'Number of unique days in the census interval when a general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologydays IS 'Number of unique days in the census interval when a pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologydays IS 'Number of unique days in the census interval when a radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtydays IS 'Number of unique days in the census interval when a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN censusprimarycare.alldays IS 'Number of unique days in the census interval when the person visited primary care in the community.';