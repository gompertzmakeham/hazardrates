CREATE MATERIALIZED VIEW censusprimarycare NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest primary care
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
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
				WHEN a0.pers_capb_prvd_spec_ad IN ('ANES', 'DIRD', 'GP', 'PATH', 'GNSG', 'NUSG', 'ORTH', 'PDSG', 'PLAS', 'THOR') THEN
					0
				ELSE
					1
			END specialist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad IN ('GNSG', 'NUSG', 'ORTH', 'PDSG', 'PLAS', 'THOR') THEN
					1
				ELSE
					0
			END surgeon,
			CASE
				WHEN a0.prvd_role_type_cls = 'PROC' AND a0.pers_capb_prvd_spec_ad = a0.pers_capb_prvd_skill_code_cls THEN
					1
				ELSE
					0
			END primaryprovider,
			CASE
				WHEN a0.prvd_role_type_cls = 'PROC' AND a0.pers_capb_prvd_spec_ad = a0.pers_capb_prvd_skill_code_cls THEN
					0
				ELSE
					1
			END consultprovider
		FROM
			ahsdrrconform.ab_claims a0
			LEFT JOIN
			ahsdrrconform.ab_claim_ah_ref_rnd_id a1
			ON
				a0.rnd_id = a1.rnd_id
		WHERE
			hazardutilities.cleanphn(a0.rcpt_uli) IS NOT NULL
			AND
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
			AND
			a0.se_end_date BETWEEN a0.se_start_date AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(a0.rcpt_dob, a0.se_start_date) <= a0.se_end_date
	),

	-- Digest to one record per patient per provider per day
	providerdata AS
	(
		SELECT
			a0.uliabphn,
			a0.uliabprid,
			a0.visitdate,
			MAX(a0.anesthesiologist * a0.primaryprovider) anesthesiologist,
			MAX(a0.consultprovider) consult,
			MAX(a0.generalpractitioner * a0.primaryprovider) generalpractitioner,
			MAX(a0.pathologist * a0.primaryprovider) pathologist,
			MAX(a0.radiologist * a0.primaryprovider) radiologist,
			MAX(a0.specialist * a0.primaryprovider) specialist,
			MAX(a0.surgeon * a0.primaryprovider) surgeon,
			SUM(a0.anesthesiologist * a0.primaryprovider) anesthesiologyprocedures,
			SUM(a0.consultprovider) consultprocedures,
			SUM(a0.generalpractitioner * a0.primaryprovider) generalpracticeprocedures,
			SUM(a0.pathologist * a0.primaryprovider) pathologyprocedures,
			SUM(a0.radiologist * a0.primaryprovider) radiologyprocedures,
			SUM(a0.specialist * a0.primaryprovider) specialtyprocedures,
			SUM(a0.surgeon * a0.primaryprovider) surgicalprocedures,
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
			MAX(a0.consult) consult,
			MAX(a0.generalpractitioner) generalpractitioner,
			MAX(a0.pathologist) pathologist,
			MAX(a0.radiologist) radiologist,
			MAX(a0.specialist) specialist,
			MAX(a0.surgeon) surgeon,
			SUM(a0.anesthesiologyprocedures) anesthesiologyprocedures,
			SUM(a0.consultprocedures) consultprocedures,
			SUM(a0.generalpracticeprocedures) generalpracticeprocedures,
			SUM(a0.pathologyprocedures) pathologyprocedures,
			SUM(a0.radiologyprocedures) radiologyprocedures,
			SUM(a0.specialtyprocedures) specialtyprocedures,
			SUM(a0.surgicalprocedures) surgicalprocedures,
			SUM(a0.allprocedures) allprocedures,
			SUM(a0.anesthesiologist) anesthesiologists,
			SUM(a0.consult) consultproviders,
			SUM(a0.generalpractitioner) generalpractitioners,
			SUM(a0.pathologist) pathologists,
			SUM(a0.radiologist) radiologists,
			SUM(a0.specialist) specialists,
			SUM(a0.surgeon) surgeons,
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
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a1.anesthesiologyprocedures) AS INTEGER) anesthesiologyprocedures,
	CAST(SUM(a1.consultprocedures) AS INTEGER) consultprocedures,
	CAST(SUM(a1.generalpracticeprocedures) AS INTEGER) generalpracticeprocedures,
	CAST(SUM(a1.pathologyprocedures) AS INTEGER) pathologyprocedures,
	CAST(SUM(a1.radiologyprocedures) AS INTEGER) radiologyprocedures,
	CAST(SUM(a1.specialtyprocedures) AS INTEGER) specialtyprocedures,
	CAST(SUM(a1.surgicalprocedures) AS INTEGER) surgicalprocedures,
	CAST(SUM(a1.allprocedures) AS INTEGER) allprocedures,
	CAST(SUM(a1.anesthesiologists) AS INTEGER) anesthesiologistsdays,
	CAST(SUM(a1.consultproviders) AS INTEGER) consultprovidersdays,
	CAST(SUM(a1.generalpractitioners) AS INTEGER) generalpractitionersdays,
	CAST(SUM(a1.pathologists) AS INTEGER) pathologistsdays,
	CAST(SUM(a1.radiologists) AS INTEGER) radiologistsdays,
	CAST(SUM(a1.specialists) AS INTEGER) specialistsdays,
	CAST(SUM(a1.surgeons) AS INTEGER) surgeonsdays,
	CAST(SUM(a1.allproviders) AS INTEGER) allproviderdays,
	CAST(SUM(a1.anesthesiologist) AS INTEGER) anesthesiologydays,
	CAST(SUM(a1.consult) AS INTEGER) consultdays,
	CAST(SUM(a1.generalpractitioner) AS INTEGER) generalpracticedays,
	CAST(SUM(a1.pathologist) AS INTEGER) pathologydays,
	CAST(SUM(a1.radiologist) AS INTEGER) radiologydays,
	CAST(SUM(a1.specialist) AS INTEGER) specialtydays,
	CAST(SUM(a1.surgeon) AS INTEGER) surgerydays,
	CAST(COUNT(*) AS INTEGER) alldays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.visitdate, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censusprimarycare IS 'Utilization of community primary care in census intervals of each person, including physician offices, diagnostic services, day surgery, and outpatient anesthesiology.';
COMMENT ON COLUMN censusprimarycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusprimarycare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusprimarycare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.anesthesiologyprocedures IS 'Number of procedures in the census interval delivered by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultprocedures IS 'Number of procedures in the census interval delivered by a provider when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticeprocedures IS 'Number of procedures in the census interval delivered by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyprocedures IS 'Number of procedures in the census interval delivered by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyprocedures IS 'Number of procedures in the census interval delivered by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtyprocedures IS 'Number of procedures in the census interval delivered by a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgicalprocedures IS 'Number of procedures in the census interval delivered by a surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN censusprimarycare.anesthesiologistsdays IS 'Number of unique combinations of anesthesiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultprovidersdays IS 'Number of unique combinations of providers and days in the census interval when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpractitionersdays IS 'Number of unique combinations of general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologistsdays IS 'Number of unique combinations of pathologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologistsdays IS 'Number of unique combinations of radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistsdays IS 'Number of unique combinations of specialists other than an anesthesiologists, general practitioners, pathologists, radiologists, or surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgeonsdays IS 'Number of unique combinations of surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allproviderdays IS 'Number of unique combinations of providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN censusprimarycare.anesthesiologydays IS 'Number of unique days in the census interval when an anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultdays IS 'Number of unique days in the census interval when either the provider role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticedays IS 'Number of unique days in the census interval when a general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologydays IS 'Number of unique days in the census interval when a pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologydays IS 'Number of unique days in the census interval when a radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtydays IS 'Number of unique days in the census interval when a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgerydays IS 'Number of unique days in the census interval when a surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.alldays IS 'Number of unique days in the census interval when the person visited primary care in the community.';