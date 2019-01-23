CREATE MATERIALIZED VIEW censusinpatientcare NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a2.durationdays) staydays,
	SUM(CASE WHEN hazardutilities.cleandate(a1.admitdate) BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) admissioncount,
	SUM(CASE WHEN hazardutilities.cleandate(a1.disdate) BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) dischargecount
FROM
	personsurveillance a0
	INNER JOIN
	ahsdata.ahs_ip_doc_dx_w_lloyd a1
	ON
		a0.uliabphn = hazardutilities.cleanphn(a1.phn)
		AND
		a1.admitcat = 'U'
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatecensus
		(
			greatest(a0.extremumstart, hazardutilities.cleandate(a1.admitdate)),
			least(a0.extremumend, hazardutilities.cleandate(a1.disdate)),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;