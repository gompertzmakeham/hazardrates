CREATE MATERIALIZED VIEW surveymothernewborn NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Vital statistics 2000 to 2015
	ingest2015 AS
	(
		SELECT
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) maternalphn,
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) infantphn
		FROM
			vital_stats_dsp.ex_vs_bth_2000_2015 a0
		WHERE
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) IS NOT NULL
			AND
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) IS NOT NULL
		GROUP BY
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m),
			ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c)
	),

	-- Vital statistics 2000 to 2016
	ingest2016 AS
	(
		SELECT
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)) maternalphn,
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)) infantphn
		FROM
			ingest2015 a0
			FULL JOIN
			vital_stats_dsp.ex_ah_bth_2000_2016 a1
			ON
				a0.infantphn = ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)
		WHERE
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)) IS NOT NULL
			AND
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)) IS NOT NULL
		GROUP BY
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)),
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1))
	),

	-- Vital statistics 2015 to 2017
	ingest2017 AS
	(
		SELECT
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)) maternalphn,
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)) infantphn
		FROM
			ingest2016 a0
			FULL JOIN
			vital_stats_dsp.ahs_bth_2015_2017 a1
			ON
				a0.infantphn = ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)
		WHERE
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)) IS NOT NULL
			AND
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1)) IS NOT NULL
		GROUP BY
			COALESCE(a0.maternalphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_m_1)),
			COALESCE(a0.infantphn, ab_hzrd_rts_anlys.hazardutilities.cleanphn(a1.stkh_num_1))
	)
	
-- Digest to one record per newborn
SELECT
	CAST(a0.maternalphn AS INTEGER) maternalphn,
	CAST(a0.infantphn AS INTEGER) infantphn,
	CAST(TRUNC(SYSDATE, 'MM') AS DATE) censoreddate
FROM
	ingest2017 a0;

COMMENT ON MATERIALIZED VIEW surveymothernewborn IS 'Unique newborns by provincial health number, with corresponding maternal provincial health number.';
COMMENT ON COLUMN surveymothernewborn.maternalphn IS 'Unique lifetime identifier of the mother, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveymothernewborn.infantphn IS 'Unique lifetime identifier of the newborn, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveymothernewborn.censoreddate IS 'First day of the month of the last refresh of the data.';