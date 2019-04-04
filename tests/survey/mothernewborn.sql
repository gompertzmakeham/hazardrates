-- Duplicate newborns in 2000-2016 vital statistics
SELECT
	COUNT(DISTINCT ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1)) OVER (PARTITION BY ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1)) duplicatcount,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) motherphn,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) infantphn,
	a0.*
FROM
	vital_stats_dsp.ex_ah_bth_2000_2016 a0
WHERE
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
	AND
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Duplicate newborns in 2015-2017 vital statistics
SELECT
	COUNT(DISTINCT ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1)) OVER (PARTITION BY ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1)) duplicatcount,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) motherphn,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) infantphn,
	a0.*
FROM
	vital_stats_dsp.ahs_bth_2015_2017 a0
WHERE
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_m_1) IS NOT NULL
	AND
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Duplicate newborns in 2000-2015 vital statistics
SELECT
	COUNT(DISTINCT ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m)) OVER (PARTITION BY ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c)) duplicatcount,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) motherphn,
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) infantphn,
	a0.*
FROM
	vital_stats_dsp.ex_vs_bth_2000_2015 a0
WHERE
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_m) IS NOT NULL
	AND
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.primary_uli_c) IS NOT NULL
ORDER BY
	1 DESC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST;

-- Mothers and newborns in inpatient
SELECT
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.phn) uliabphn,
	ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(a0.inst) facilityidentifier, 
	a0.chartno patientchart,
	a0.mat_nb_cht referencechart,
	ab_hzrd_rts_anlys.hazardutilities.cleandate(a0.admitdate || COALESCE(a0.admittime, '0000'), 'yyyymmddhh24mi') admitdate,
	a0.admitcat admissionroute,
	a0.entrycode entryroute,
	a0.inst_zone facilityzone,
	a0.inst_reg facitityregion,
	a0.seqnum sequenceidentifier,
	a0.*
FROM
	ahsdrrdeliver.ahs_ip_doctor_dx a0
WHERE
	ab_hzrd_rts_anlys.hazardutilities.cleanphn(a0.phn) IS NOT NULL
	AND
	ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(a0.inst) IS NOT NULL
	AND
	a0.resppay IN ('01', '02', '05')
	AND
	a0.mat_nb_cht IS NOT NULL
	AND
	ab_hzrd_rts_anlys.hazardutilities.cleandate(a0.admitdate || COALESCE(a0.admittime, '0000'), 'yyyymmddhh24mi') IS NOT NULL
ORDER BY
	10 ASC NULLS FIRST;