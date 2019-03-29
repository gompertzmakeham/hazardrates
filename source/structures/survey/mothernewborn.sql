CREATE MATERIALIZED VIEW surveymothernewborn NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest mother-newborn pairs
	primarydata AS
	(
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			ab_hzrd_rts_anlys.hazardutilities.cleansex(a0.rcpt_gender_code) sex,
			a0.rcpt_dob birthdate,
			COALESCE
			(
				ab_hzrd_rts_anlys.hazardutilities.cleanprid(a0.prvd_prid),
				ab_hzrd_rts_anlys.hazardutilities.cleanprid(a1.prvd_prid)
			) uliabprid,
			a0.se_start_date startdate,
			a0.se_end_date enddate,
			a0.addr_delv_site_postal_code sitepostalcode,
			a0.addr_rcpt_postal_code_ad residencepostalcode,
			a0.addr_rcpt_postal_code_drvd_ad deliverypostalcode,
			CASE
				WHEN a0.se_birth_outcome IS NOT NULL THEN
					1
				ElSE
					0
			END newborn
		FROM
			ahsdrrconform.ab_claims a0
			LEFT JOIN
			ahsdrrconform.ab_claim_ah_ref_rnd_id a1
			ON
				a0.rnd_id = a1.rnd_id
		WHERE
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.rcpt_uli) IS NOT NULL
	),

	-- Consolidate mother-newbord pairs
	eventdata AS
	(

		-- Mother-newborn pairs from primary care;
		SELECT
			a0.uliabphn maternalphn,
			a1.uliabphn infantphn
		FROM
			primarydata a0
			INNER JOIN
			primarydata a1
			ON
				a0.newborn = 0
				AND
				a1.newborn = 1
				AND
				a0.sex = 'F'
				AND
				a0.uliabprid = a1.uliabprid
				AND
				a0.startdate = a1.startdate
				AND
				a0.enddate = a1.enddate
				AND
				a0.sitepostalcode = a1.sitepostalcode
				AND
				a0.residencepostalcode = a1.residencepostalcode
				AND
				a0.deliverypostalcode = a1.deliverypostalcode
				AND
				a0.birthdate < a1.birthdate
		UNION ALL

		-- Mother-newborn pairs from inpatient care
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.phn) maternalphn,
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.phn) infantphn
		FROM
			ahsdrrdeliver.ahs_ip_doctor_dx a0
			INNER JOIN
			ahsdrrdeliver.ahs_ip_doctor_dx a1
			ON
				ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.phn) IS NOT NULL
				AND
				ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.phn) IS NOT NULL
				AND
				a0.chartno = a1.mat_nb_cht
				AND
				a0.mat_nb_cht = a1.chartno
				AND
				a0.inst = a1.inst
				AND
				a0.admitcat <> 'N'
				AND
				a0.entrycode <> 'N'
				AND
				(a1.admitcat = 'N' OR a1.entrycode = 'N')
		UNION ALL

		-- Mother-newborn pairs from births 2000 to 2016
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) maternalphn,
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) infantphn
		FROM
			vital_stats_dsp.ex_ah_bth_2000_2016 a0
		WHERE
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
			AND
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
		UNION ALL

		-- Mother-newborn pairs from births 2015 to 2017
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) maternalphn,
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) infantphn
		FROM
			vital_stats_dsp.ahs_bth_2015_2017 a0
		WHERE
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
			AND
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
		UNION ALL

		-- Mother-newborn pairs from births 2000 to 2015
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) maternalphn,
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) infantphn
		FROM
			vital_stats_dsp.ex_vs_bth_2000_2015 a0
		WHERE
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) IS NOT NULL
			AND
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) IS NOT NULL
	)
	
-- Digest to one record per newborn
SELECT
	CAST(MAX(a0.maternalphn) AS INTEGER) maternalphn,
	CAST(a0.infantphn AS INTEGER) infantphn
FROM
	eventdata a0
GROUP BY
	a0.infantphn;

COMMENT ON MATERIALIZED VIEW surveymothernewborn IS 'Unique newborns by provincial health number, with corresponding maternal provincial health number.';
COMMENT ON COLUMN surveymothernewborn.maternalphn IS 'Unique lifetime identifier of the mother, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveymothernewborn.infantphn IS 'Unique lifetime identifier of the newborn, Alberta provincial healthcare number.';