ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW personutilization NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT
	a0.*,
	a1.*,
	a2.*,
	a3.*,
	a4.*,
	a5.*,
	-- a6.*,
	a7.*
FROM
	personcensus a0
	LEFT JOIN
	censusambulatorycare a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend
	LEFT JOIN
	censusinpatientcare a2
	ON
		a0.uliabphn = a2.uliabphn
		AND
		a0.cornercase = a2.cornercase
		AND
		a0.intervalstart = a2.intervalstart
		AND
		a0.intervalend = a2.intervalend
	LEFT JOIN
	censuslaboratorycollection a3
	ON
		a0.uliabphn = a3.uliabphn
		AND
		a0.cornercase = a3.cornercase
		AND
		a0.intervalstart = a3.intervalstart
		AND
		a0.intervalend = a3.intervalend
	LEFT JOIN
	censuslongtermcare a4
	ON
		a0.uliabphn = a4.uliabphn
		AND
		a0.cornercase = a4.cornercase
		AND
		a0.intervalstart = a4.intervalstart
		AND
		a0.intervalend = a4.intervalend
	LEFT JOIN
	censuspharmacydispense a5
	ON
		a0.uliabphn = a5.uliabphn
		AND
		a0.cornercase = a5.cornercase
		AND
		a0.intervalstart = a5.intervalstart
		AND
		a0.intervalend = a5.intervalend/*
	LEFT JOIN
	censusprimarycare a6
	ON
		a0.uliabphn = a6.uliabphn
		AND
		a0.cornercase = a6.cornercase
		AND
		a0.intervalstart = a6.intervalstart
		AND
		a0.intervalend = a6.intervalend*/
	LEFT JOIN
	censussupportiveliving a7
	ON
		a0.uliabphn = a7.uliabphn
		AND
		a0.cornercase = a7.cornercase
		AND
		a0.intervalstart = a7.intervalstart
		AND
		a0.intervalend = a7.intervalend;