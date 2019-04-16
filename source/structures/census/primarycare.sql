CREATE MATERIALIZED VIEW censusprimarycare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
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
				WHEN a0.pers_capb_prvd_spec_ad = 'OBGY' THEN
					1
				ELSE
					0
			END obstetrician,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'PATH' THEN
					1
				ELSE
					0
			END pathologist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'PSYC' THEN
					1
				ELSE
					0
			END psychiatrist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad = 'DIRD' THEN
					1
				ELSE
					0
			END radiologist,
			CASE
				WHEN a0.pers_capb_prvd_spec_ad IN ('ANES', 'DIRD', 'GP', 'OBGY', 'PATH', 'PSYC', 'GNSG', 'NUSG', 'ORTH', 'PDSG', 'PLAS', 'THOR') THEN
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
			MAX(a0.obstetrician * a0.primaryprovider) obstetrician,
			MAX(a0.pathologist * a0.primaryprovider) pathologist,
			MAX(a0.psychiatrist * a0.primaryprovider) psychiatrist,
			MAX(a0.radiologist * a0.primaryprovider) radiologist,
			MAX(a0.specialist * a0.primaryprovider) specialist,
			MAX(a0.surgeon * a0.primaryprovider) surgeon,
			SUM(a0.anesthesiologist * a0.primaryprovider) anesthesiologyprocedures,
			SUM(a0.consultprovider) consultprocedures,
			SUM(a0.generalpractitioner * a0.primaryprovider) generalpracticeprocedures,
			SUM(a0.obstetrician * a0.primaryprovider) obstetricprocedures,
			SUM(a0.pathologist * a0.primaryprovider) pathologyprocedures,
			SUM(a0.psychiatrist * a0.primaryprovider) psychiatryprocedures,
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
			MAX(a0.obstetrician) obstetrician,
			MAX(a0.pathologist) pathologist,
			MAX(a0.psychiatrist) psychiatrist,
			MAX(a0.radiologist) radiologist,
			MAX(a0.specialist) specialist,
			MAX(a0.surgeon) surgeon,
			SUM(a0.anesthesiologyprocedures) anesthesiologyprocedures,
			SUM(a0.consultprocedures) consultprocedures,
			SUM(a0.generalpracticeprocedures) generalpracticeprocedures,
			SUM(a0.obstetricprocedures) obstetricprocedures,
			SUM(a0.pathologyprocedures) pathologyprocedures,
			SUM(a0.psychiatryprocedures) psychiatryprocedures,
			SUM(a0.radiologyprocedures) radiologyprocedures,
			SUM(a0.specialtyprocedures) specialtyprocedures,
			SUM(a0.surgicalprocedures) surgicalprocedures,
			SUM(a0.allprocedures) allprocedures,
			SUM(a0.anesthesiologist) anesthesiologists,
			SUM(a0.consult) consultproviders,
			SUM(a0.generalpractitioner) generalpractitioners,
			SUM(a0.obstetrician) obstetricians,
			SUM(a0.pathologist) pathologists,
			SUM(a0.psychiatrist) psychiatrists,
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
	CAST(SUM(a1.obstetricprocedures) AS INTEGER) obstetricprocedures,
	CAST(SUM(a1.pathologyprocedures) AS INTEGER) pathologyprocedures,
	CAST(SUM(a1.psychiatryprocedures) AS INTEGER) psychiatryprocedures,
	CAST(SUM(a1.radiologyprocedures) AS INTEGER) radiologyprocedures,
	CAST(SUM(a1.specialtyprocedures) AS INTEGER) specialtyprocedures,
	CAST(SUM(a1.surgicalprocedures) AS INTEGER) surgicalprocedures,
	CAST(SUM(a1.allprocedures) AS INTEGER) allprocedures,
	CAST(SUM(a1.anesthesiologists) AS INTEGER) anesthesiologistsdays,
	CAST(SUM(a1.consultproviders) AS INTEGER) consultprovidersdays,
	CAST(SUM(a1.generalpractitioners) AS INTEGER) generalpractitionersdays,
	CAST(SUM(a1.obstetricians) AS INTEGER) obstetriciansdays,
	CAST(SUM(a1.pathologists) AS INTEGER) pathologistsdays,
	CAST(SUM(a1.psychiatrists) AS INTEGER) psychiatristsdays,
	CAST(SUM(a1.radiologists) AS INTEGER) radiologistsdays,
	CAST(SUM(a1.specialists) AS INTEGER) specialistsdays,
	CAST(SUM(a1.surgeons) AS INTEGER) surgeonsdays,
	CAST(SUM(a1.allproviders) AS INTEGER) allproviderdays,
	CAST(SUM(a1.anesthesiologist) AS INTEGER) anesthesiologydays,
	CAST(SUM(a1.consult) AS INTEGER) consultdays,
	CAST(SUM(a1.generalpractitioner) AS INTEGER) generalpracticedays,
	CAST(SUM(a1.obstetrician) AS INTEGER) obstetricdays,
	CAST(SUM(a1.pathologist) AS INTEGER) pathologydays,
	CAST(SUM(a1.psychiatrist) AS INTEGER) psychiatrydays,
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
		AND
		a1.visitdate BETWEEN a0.extremumstart AND a0.extremumend
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
COMMENT ON COLUMN censusprimarycare.anesthesiologyprocedures IS 'Number of primary care procedures in the census interval delivered by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultprocedures IS 'Number of primary care procedures in the census interval delivered by a provider when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticeprocedures IS 'Number of primary care procedures in the census interval delivered by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.obstetricprocedures IS 'Number of primary care procedures in the census interval delivered by a obstetrician-gynecologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologyprocedures IS 'Number of primary care procedures in the census interval delivered by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.psychiatryprocedures IS 'Number of primary care procedures in the census interval delivered by a psychiatrist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologyprocedures IS 'Number of primary care procedures in the census interval delivered by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtyprocedures IS 'Number of primary care procedures in the census interval delivered by a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgicalprocedures IS 'Number of primary care procedures in the census interval delivered by a surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN censusprimarycare.anesthesiologistsdays IS 'Number of unique combinations of primary care anesthesiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultprovidersdays IS 'Number of unique combinations of primary care providers and days in the census interval when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpractitionersdays IS 'Number of unique combinations of primary care general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.obstetriciansdays IS 'Number of unique combinations of primary care obstetrician-gynecologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologistsdays IS 'Number of unique combinations of primary care pathologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.psychiatristsdays IS 'Number of unique combinations of primary care psychiatrists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologistsdays IS 'Number of unique combinations of primary care radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialistsdays IS 'Number of unique combinations of primary care specialists other than an anesthesiologists, general practitioners, pathologists, radiologists, or surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgeonsdays IS 'Number of unique combinations of primary care surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.allproviderdays IS 'Number of unique combinations of primary care providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN censusprimarycare.anesthesiologydays IS 'Number of unique days in the census interval when a primary care anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.consultdays IS 'Number of unique days in the census interval when either the primary care provider role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN censusprimarycare.generalpracticedays IS 'Number of unique days in the census interval when a primary care general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.obstetricdays IS 'Number of unique days in the census interval when a primary care obstetrician-gynecologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.pathologydays IS 'Number of unique days in the census interval when a primary care pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.psychiatrydays IS 'Number of unique days in the census interval when a primary care psychiatrist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.radiologydays IS 'Number of unique days in the census interval when a primary care radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.specialtydays IS 'Number of unique days in the census interval when a primary care specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN censusprimarycare.surgerydays IS 'Number of unique days in the census interval when a primary care surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN censusprimarycare.alldays IS 'Number of unique days in the census interval when the person visited primary care in the community.';