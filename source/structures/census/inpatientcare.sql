CREATE MATERIALIZED VIEW censusinpatientcare NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
SELECT
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a2.durationdays) AS INTEGER) staydays,
	CAST(SUM(a2.intervalfirst) AS INTEGER) admissioncount,
	CAST(SUM(a2.intervallast) AS INTEGER) dischargecount,
	CAST(COUNT(*) AS INTEGER) intersectingstays
FROM
	personsurveillance a0
	INNER JOIN
	ahsdrrdeliver.ahs_ip_doctor_dx a1
	ON
		a1.resppay = '01'
		AND
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
			hazardutilities.cleandate(a1.admitdate),
			hazardutilities.cleandate(a1.disdate),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censusinpatientcare IS 'Utilization of unplanned, urgent, or emergency inpatient care in census intervals of each person.';
COMMENT ON COLUMN censusinpatientcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusinpatientcare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusinpatientcare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.staydays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.admissioncount IS 'Admissions in the census interval.';
COMMENT ON COLUMN censusinpatientcare.dischargecount IS 'Discharges in the census interval.';
COMMENT ON COLUMN censusinpatientcare.intersectingstays IS 'Stays intersecting with the census interval.';