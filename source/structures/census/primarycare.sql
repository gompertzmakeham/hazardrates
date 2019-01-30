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
	)

-- Digest primary care
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	COUNT
	(
		DISTINCT
		CASE a1.anesthesiologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.visitdate
			ELSE
				NULL
		END
	) anesthesiologydays,
	COUNT
	(
		DISTINCT
		CASE a1.anesthesiologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')
			ELSE
				NULL
		END
	) anesthesiologyproviderdays,
	SUM(a1.anesthesiologist * a1.primaryprovider * a1.primaryskill) anesthesiologyprocedures,
	COUNT
	(
		DISTINCT
		CASE a1.generalpractitioner * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.visitdate
			ELSE
				NULL
		END
	) gpdays,
	COUNT
	(
		DISTINCT
		CASE a1.generalpractitioner * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')
			ELSE
				NULL
		END
	) gpproviderdays,
	SUM(a1.generalpractitioner * a1.primaryprovider * a1.primaryskill) gpprocedures,
	COUNT
	(
		DISTINCT
		CASE a1.pathologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.visitdate
			ELSE
				NULL
		END
	) pathologydays,
	COUNT
	(
		DISTINCT
		CASE a1.pathologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')
			ELSE
				NULL
		END
	) pathologyproviderdays,
	SUM(a1.pathologist * a1.primaryprovider * a1.primaryskill) pathologyprocedures,
	COUNT
	(
		DISTINCT
		CASE a1.radiologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.visitdate
			ELSE
				NULL
		END
	) radiologydays,
	COUNT
	(
		DISTINCT
		CASE a1.radiologist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')
			ELSE
				NULL
		END
	) radiologyproviderdays,
	SUM(a1.radiologist * a1.primaryprovider * a1.primaryskill) radiologyprocedures,
	COUNT
	(
		DISTINCT
		CASE a1.specialist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.visitdate
			ELSE
				NULL
		END
	) specialistdays,
	COUNT
	(
		DISTINCT
		CASE a1.specialist * a1.primaryprovider * a1.primaryskill
			WHEN 1 THEN
				a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')
			ELSE
				NULL
		END
	) specialistproviderdays,
	SUM(a1.specialist * a1.primaryprovider * a1.primaryskill) specialistprocedures,
	COUNT(DISTINCT a1.visitdate) alldays,
	COUNT(DISTINCT a1.uliabprid || '-' || to_char(a1.visitdate, 'YYYYMMDD')) allproviderdays,
	COUNT(*) allprocedures
FROM
	personsurveillance a0
	INNER JOIN
	eventdata a1
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
COMMENT ON COLUMN censusprimarycare.anesthesiologydays IS 'Number of unique days in the census interval when an anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.anesthesiologyproviderdays IS 'Number of unique combinations of anesthesiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.anesthesiologyprocedures IS 'Number of procedures in the census interval provided by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN censusprimarycare.gpdays IS 'Number of unique days in the census interval when a general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.gpproviderdays IS 'Number of unique combinations of general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.gpprocedures IS 'Number of procedures in the census interval provided by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologydays IS 'Number of unique days in the census interval when a pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyproviderdays IS 'Number of unique combinations of pathologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyprocedures IS 'Number of procedures in the census interval provided by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologydays IS 'Number of unique days in the census interval when a radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyproviderdays IS 'Number of unique combinations of radiologist and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyprocedures IS 'Number of procedures in the census interval provided by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistdays IS 'Number of unique days in the census interval when a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistproviderdays IS 'Number of unique combinations of specialists other than an anesthesiologists, general practitioners, pathologists or radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistprocedures IS 'Number of procedures in the census interval provided by a specialist other than an anesthesiologist, general practitioner, pathologist or radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.alldays IS 'Number of unique days in the census interval when the person visited primary care in the community.';
COMMENT ON COLUMN censusprimarycare.allproviderdays IS 'Number of unique combinations of providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN censusprimarycare.allprocedures IS 'Number of primary care procedures in the census interval.';