-- Duplicate newborns in 2000-2016 vital statistics
SELECT
	COUNT(DISTINCT hazardutilities.cleanphn(a0.stkh_num_m_1)) OVER (PARTITION BY hazardutilities.cleanphn(a0.stkh_num_1)) duplicatcount,
	hazardutilities.cleanphn(a0.stkh_num_m_1) motherphn,
	hazardutilities.cleanphn(a0.stkh_num_1) infantphn,
	a0.*
FROM
	vital_stats_dsp.ex_ah_bth_2000_2016 a0
WHERE
	hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
	AND
	hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Duplicate newborns in 2015-2017 vital statistics
SELECT
	COUNT(DISTINCT hazardutilities.cleanphn(a0.stkh_num_m_1)) OVER (PARTITION BY hazardutilities.cleanphn(a0.stkh_num_1)) duplicatcount,
	hazardutilities.cleanphn(a0.stkh_num_m_1) motherphn,
	hazardutilities.cleanphn(a0.stkh_num_1) infantphn,
	a0.*
FROM
	vital_stats_dsp.ahs_bth_2015_2017 a0
WHERE
	hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
	AND
	hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Duplicate newborns in 2000-2015 vital statistics
SELECT
	COUNT(DISTINCT hazardutilities.cleanphn(a0.primary_uli_m)) OVER (PARTITION BY hazardutilities.cleanphn(a0.primary_uli_c)) duplicatcount,
	hazardutilities.cleanphn(a0.primary_uli_m) motherphn,
	hazardutilities.cleanphn(a0.primary_uli_c) infantphn,
	a0.*
FROM
	vital_stats_dsp.ex_vs_bth_2000_2015 a0
WHERE
	hazardutilities.cleanphn(a0.primary_uli_m) IS NOT NULL
	AND
	hazardutilities.cleanphn(a0.primary_uli_c) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Mothers and newborns in inpatient
SELECT
	hazardutilities.cleanphn(a0.phn) uliabphn,
	hazardutilities.cleaninpatient(a0.inst) facilityidentifier, 
	a0.chartno patientchart,
	a0.mat_nb_cht referencechart,
	hazardutilities.cleandate(a0.admitdate || COALESCE(a0.admittime, '0000'), 'yyyymmddhh24mi') admitdate,
	a0.admitcat admissionroute,
	a0.entrycode entryroute,
	a0.inst_zone facilityzone,
	a0.inst_reg facitityregion,
	a0.seqnum sequenceidentifier,
	a0.*
FROM
	ahsdrrdeliver.ahs_ip_doctor_dx a0
WHERE
	hazardutilities.cleanphn(a0.phn) IS NOT NULL
	AND
	hazardutilities.cleaninpatient(a0.inst) IS NOT NULL
	AND
	a0.resppay IN ('01', '02', '05')
	AND
	a0.mat_nb_cht IS NOT NULL
	AND
	hazardutilities.cleandate(a0.admitdate || COALESCE(a0.admittime, '0000'), 'yyyymmddhh24mi') IS NOT NULL
ORDER BY
	10 ASC NULLS FIRST;

-- Primary care new borns
WITH
	ingestdata AS
	(
		SELECT
			CASE SUM(CASE WHEN a0.se_birth_outcome IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY substr(a0.claim_id_cls, 1, 13))
				WHEN 0 THEN
					0
				ELSE
					1
			END nearnewborn,
			a0.*
		FROM
			ahsdrrconform.ab_claims a0
	)
SELECT
	a0.*
FROM
	ingestdata a0
WHERE
	a0.nearnewborn = 1
ORDER BY
	a0.claim_id_cls;