ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censusinpatientcare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a2.durationdays) staydays,
	SUM(a2.evententry) admissioncount,
	SUM(a2.eventexit) dischargecount,
	COUNT(*) intersectingstays
FROM
	personsurveillance a0
	INNER JOIN
	ahsdata.ahs_ip_doc_dx_w_lloyd a1
	ON
		a0.uliabphn = hazardutilities.cleanphn(a1.phn)
		AND
		a1.admitcat = 'U'
		AND
		hazardutilities.cleaninpatient(a1.inst) IS NOT NULL
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

COMMENT ON MATERIALIZED VIEW censusinpatientcare IS 'Utilization of inpatient care in census intervals of each person.';
COMMENT ON COLUMN censusinpatientcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusinpatientcare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusinpatientcare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.staydays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.admissioncount IS 'Admissions in the census interval.';
COMMENT ON COLUMN censusinpatientcare.dischargecount IS 'Discharges in the census interval.';
COMMENT ON COLUMN censusinpatientcare.intersectingstays IS 'Stays intersecting with the census interval.';