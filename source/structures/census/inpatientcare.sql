CREATE MATERIALIZED VIEW censusinpatientcare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all inpatient care events
	eventdata AS
	(
		SELECT
		
			/*+ cardinality(a2, 1) */
			a0.uliabphn,
			a0.cornercase,
			a2.intervalstart,
			a2.intervalend,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1 + least(a0.extremumend, a2.durationend) - greatest(a0.extremumstart, a2.durationstart)
				ELSE
					0
			END staydays,
			CASE
				WHEN a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END admissioncount,
			CASE
				WHEN a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END dischargecount,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END intersectingstays,
			CASE a1.resppay
				WHEN '02' THEN 
					1
				ELSE
					0
			END workcasualty,
			CASE a1.resppay
				WHEN '02' THEN 
					0
				ELSE
					1
			END privatecasualty
		FROM
			personsurveillance a0
			INNER JOIN
			ahsdrrdeliver.ahs_ip_doctor_dx a1
			ON
				a1.resppay IN ('01', '02', '05')
				AND
				a0.uliabphn = hazardutilities.cleanphn(a1.phn)
				AND
				a1.admitcat = 'U'
				AND
				hazardutilities.cleaninpatient(a1.inst) IS NOT NULL
				AND
				hazardutilities.cleandate(a1.disdate) BETWEEN hazardutilities.cleandate(a1.admitdate) AND TRUNC(SYSDATE, 'MM')
				AND
				COALESCE(hazardutilities.cleandate(a1.birthdate), hazardutilities.cleandate(a1.admitdate)) <= hazardutilities.cleandate(a1.disdate)
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
	)
	
-- Digest to one record per person per census interval partitioning the surveillance span
SELECT
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a0.intervalstart AS DATE) intervalstart,
	CAST(a0.intervalend AS DATE) intervalend,
	CAST(SUM(a0.workcasualty * a0.staydays) AS INTEGER) workstaydays,
	CAST(SUM(a0.privatecasualty * a0.staydays) AS INTEGER) privatestaydays,
	CAST(SUM(a0.staydays) AS INTEGER) staydays,
	CAST(SUM(a0.workcasualty * a0.admissioncount) AS INTEGER) workadmissioncount,
	CAST(SUM(a0.privatecasualty * a0.admissioncount) AS INTEGER) privateadmissioncount,
	CAST(SUM(a0.admissioncount) AS INTEGER) admissioncount,
	CAST(SUM(a0.workcasualty * a0.dischargecount) AS INTEGER) workdischargecount,
	CAST(SUM(a0.privatecasualty * a0.dischargecount) AS INTEGER) privatedischargecount,
	CAST(SUM(a0.dischargecount) AS INTEGER) dischargecount,
	CAST(SUM(a0.workcasualty * a0.intersectingstays) AS INTEGER) workintersectingstays,
	CAST(SUM(a0.privatecasualty * a0.intersectingstays) AS INTEGER) privateintersectingstays,
	CAST(SUM(a0.intersectingstays) AS INTEGER) intersectingstays
FROM
	eventdata a0
WHERE
	a0.staydays > 0
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a0.intervalstart,
	a0.intervalend;

COMMENT ON MATERIALIZED VIEW censusinpatientcare IS 'Utilization of unplanned, urgent, or emergency inpatient care in census intervals of each person.';
COMMENT ON COLUMN censusinpatientcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusinpatientcare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusinpatientcare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusinpatientcare.workstaydays IS 'Naive sum of emergency inpatient care days, for workplace casualties, that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.privatestaydays IS 'Naive sum of emergency inpatient care days, for private casualties, that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.staydays IS 'Naive sum of emergency inpatient care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censusinpatientcare.workadmissioncount IS 'Emergency inpatient care admissions, for workplace casualties, in the census interval.';
COMMENT ON COLUMN censusinpatientcare.privateadmissioncount IS 'Emergency inpatient care admissions, for private casualties, in the census interval.';
COMMENT ON COLUMN censusinpatientcare.admissioncount IS 'Emergency inpatient care admissions in the census interval.';
COMMENT ON COLUMN censusinpatientcare.workdischargecount IS 'Emergency inpatient care discharges, for workplace casualties, in the census interval.';
COMMENT ON COLUMN censusinpatientcare.privatedischargecount IS 'Emergency inpatient care discharges, for private casualties, in the census interval.';
COMMENT ON COLUMN censusinpatientcare.dischargecount IS 'Emergency inpatient care discharges in the census interval.';
COMMENT ON COLUMN censusinpatientcare.workintersectingstays IS 'Emergency inpatient care stays, for workplace casualties, intersecting with the census interval.';
COMMENT ON COLUMN censusinpatientcare.privateintersectingstays IS 'Emergency inpatient care stays, for private casualties, intersecting with the census interval.';
COMMENT ON COLUMN censusinpatientcare.intersectingstays IS 'Emergency inpatient care stays intersecting with the census interval.';