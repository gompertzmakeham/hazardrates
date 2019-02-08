CREATE MATERIALIZED VIEW censusambulatorycare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all ambulatory care events
	eventdata AS
	(

		-- AACRS historic
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleandate(a0.visdate) visitdate,
			hazardutilities.cleanambulatory(a0.inst) siteidentifier,
			a0.los_minutes visitminutes
		FROM
			ahsdata.ahs_ambulatory a0
		WHERE
			hazardutilities.cleanambulatory(a0.inst) IS NOT NULL
			AND
			hazardutilities.cleandate(a0.visdate) IS NOT NULL
			AND
			a0.los_minutes > 0
			AND
			a0.abstract_type IN ('A', 'E', 'U')
			AND
			a0.vismode = 1
		UNION ALL

		-- NACRS current
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleandate(a0.visit_date) visitdate,
			hazardutilities.cleanambulatory(a0.inst) siteidentifier,
			a0.visit_los_minutes visitminutes
		FROM
			ahsdata.ahs_nacrs_main a0
		WHERE
			hazardutilities.cleanambulatory(a0.inst) IS NOT NULL
			AND
			hazardutilities.cleandate(a0.visit_date) IS NOT NULL
			AND
			a0.visit_los_minutes > 0
			AND
			a0.abstract_type IN ('A', 'E', 'U')
			AND
			a0.visit_mode = 1
	),

	-- Digest to one record per person per day per institution
	sitedata AS
	(
		SELECT
			a0.uliabphn,
			a0.visitdate,
			a0.siteidentifier,
			SUM(a0.visitminutes) visitminutes,
			COUNT(*) visitcount
		FROM
			eventdata a0
		GROUP BY
			a0.uliabphn,
			a0.visitdate,
			a0.siteidentifier
	),

	-- Digest to one record per person per day
	daydata AS
	(
		SELECT
			a0.uliabphn,
			a0.visitdate,
			SUM(a0.visitminutes) visitminutes,
			SUM(a0.visitcount) visitcount,
			COUNT(*) sitecount
		FROM
			sitedata a0
		GROUP BY
			a0.uliabphn,
			a0.visitdate
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a1.visitminutes) AS INTEGER) visitminutes,
	CAST(SUM(a1.visitcount) AS INTEGER) visitcount,
	CAST(SUM(a1.sitecount) AS INTEGER) visitsitedays,
	CAST(COUNT(*) AS INTEGER) visitdays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a1.visitdate BETWEEN a0.extremumstart AND a0.extremumend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.visitdate, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censusambulatorycare IS 'Utilization of unplanned, urgent, or emergency ambulatory care in census intervals of each person.';
COMMENT ON COLUMN censusambulatorycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusambulatorycare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusambulatorycare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusambulatorycare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusambulatorycare.visitminutes IS 'Naive sum of minutes that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN censusambulatorycare.visitcount IS 'Visits in the census interval.';
COMMENT ON COLUMN censusambulatorycare.visitsitedays IS 'Unique combinations of days and sites visited in the census interval.';
COMMENT ON COLUMN censusambulatorycare.visitdays IS 'Unique days of visits in the census interval.';