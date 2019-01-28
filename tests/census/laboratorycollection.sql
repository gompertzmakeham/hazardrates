-- Client types and collection locations
SELECT
	'LF' sourcesystem,
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm,
	COUNT(*) assaycount
FROM
	ahsdata.lab_lf a0
GROUP BY
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm
UNION ALL
SELECT
	'ML' sourcesystem,
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm,
	COUNT(*) assaycount
FROM
	ahsdata.lab_ml a0
GROUP BY
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm
UNION ALL
SELECT
	'MT' sourcesystem,
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm,
	COUNT(*) assaycount
FROM
	ahsdata.lab_mt a0
GROUP BY
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm
UNION ALL
SELECT
	'SQ' sourcesystem,
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm,
	COUNT(*) assaycount
FROM
	ahsdata.lab_sq a0
GROUP BY
	a0.clnt_type,
	a0.ordr_sts_cd,
	a0.ordr_sts_nm,
	a0.clnt_fac_cd,
	a0.clnt_fac_nm,
	a0.clnt_loc_cd,
	a0.clnt_loc_nm,
	a0.clct_loc_cd,
	a0.clct_loc_nm
ORDER BY
	1 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	7 ASC NULLS FIRST,
	9 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST;