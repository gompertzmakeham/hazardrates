CREATE MATERIALIZED VIEW censuslongtermcare NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a2.durationdays) staydays,
	SUM(CASE WHEN a1.admit_date BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) admissioncount,
	SUM(CASE WHEN a1.discharge_date BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) dischargecount
FROM
	personsurveillance a0
	INNER JOIN
	TABLE(continuing_care.accis.get_adt) a1
	ON
		a0.uliabphn = a1.uli_ab_phn
		AND
		a1.current_client_type_code = '10'
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatecensus
		(
			greatest(a0.extremumstart, a1.admit_date),
			least(a0.extremumend, COALESCE(a1.discharge_date, a0.extremumend)),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;