CREATE MATERIALIZED VIEW censusambulatorycare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all ambulatory care events
	eventdata AS
	(

		-- AACRS historic
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleandate(a0.visdate) visitdate,
			hazardutilities.cleanambulatory(a0.inst) siteidentifier,
			a0.los_minutes visitminutes,
			CASE a0.resppay
				WHEN '02' THEN 
					1
				ELSE
					0
			END workcasualty,
			CASE a0.resppay
				WHEN '02' THEN 
					0
				ELSE
					1
			END privatecasualty
		FROM
			ahsdrrdeliver.ahs_ambulatory a0
		WHERE
			hazardutilities.cleanphn(a0.phn) IS NOT NULL
			AND
			a0.resppay IN ('01', '02', '05')
			AND
			hazardutilities.cleanambulatory(a0.inst) IS NOT NULL
			AND
			a0.los_minutes > 0
			AND
			a0.abstract_type IN ('A', 'E', 'U')
			AND
			a0.vismode = 1
			AND
			hazardutilities.cleandate(a0.visdate) <= TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(hazardutilities.cleandate(a0.disdate), TRUNC(SYSDATE, 'MM')) <= TRUNC(SYSDATE, 'MM')
			AND
			(
				hazardutilities.cleandate(a0.birthdate) IS NULL
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.visdate)
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.disdate)
			)
		UNION ALL

		-- NACRS current
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleandate(a0.visit_date) visitdate,
			hazardutilities.cleanambulatory(a0.inst) siteidentifier,
			a0.visit_los_minutes visitminutes,
			CASE a0.resppay
				WHEN '02' THEN 
					1
				ELSE
					0
			END workcasualty,
			CASE a0.resppay
				WHEN '02' THEN 
					0
				ELSE
					1
			END privatecasualty
		FROM
			ahsdrrdeliver.ahs_nacrs_tab a0
		WHERE
			hazardutilities.cleanphn(a0.phn) IS NOT NULL
			AND
			a0.resppay IN ('01', '02', '05')
			AND
			hazardutilities.cleanambulatory(a0.inst) IS NOT NULL
			AND
			a0.visit_los_minutes > 0
			AND
			a0.abstract_type IN ('A', 'E', 'U')
			AND
			a0.visit_mode = 1
			AND
			hazardutilities.cleandate(a0.visit_date) <= TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(hazardutilities.cleandate(a0.disp_date), TRUNC(SYSDATE, 'MM')) <= TRUNC(SYSDATE, 'MM')
			AND
			(
				hazardutilities.cleandate(a0.birthdate) IS NULL
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.visit_date)
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.disp_date)
			)
	),

	-- Digest to one record per person per day per institution
	sitedata AS
	(
		SELECT
			a0.uliabphn,
			a0.visitdate,
			a0.siteidentifier,
			MAX(a0.workcasualty) workcasualty,
			MAX(a0.privatecasualty) privatecasualty,
			SUM(a0.workcasualty * a0.visitminutes) workvisitminutes,
			SUM(a0.privatecasualty * a0.visitminutes) privatevisitminutes,
			SUM(a0.visitminutes) visitminutes,
			SUM(a0.workcasualty) workvisitcount,
			SUM(a0.privatecasualty) privatevisitcount,
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
			MAX(a0.workcasualty) workcasualty,
			MAX(a0.privatecasualty) privatecasualty,
			SUM(a0.workvisitminutes) workvisitminutes,
			SUM(a0.privatevisitminutes) privatevisitminutes,
			SUM(a0.visitminutes) visitminutes,
			SUM(a0.workvisitcount) workvisitcount,
			SUM(a0.privatevisitcount) privatevisitcount,
			SUM(a0.visitcount) visitcount,
			SUM(a0.workcasualty) worksitecount,
			SUM(a0.privatecasualty) privatesitecount,
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
	CAST(SUM(a1.workvisitminutes) AS INTEGER) workvisitminutes,
	CAST(SUM(a1.privatevisitminutes) AS INTEGER) privatevisitminutes,
	CAST(SUM(a1.visitminutes) AS INTEGER) visitminutes,
	CAST(SUM(a1.workvisitcount) AS INTEGER) workvisitcount,
	CAST(SUM(a1.privatevisitcount) AS INTEGER) privatevisitcount,
	CAST(SUM(a1.visitcount) AS INTEGER) visitcount,
	CAST(SUM(a1.worksitecount) AS INTEGER) workvisitsitedays,
	CAST(SUM(a1.privatesitecount) AS INTEGER) privatevisitsitedays,
	CAST(SUM(a1.sitecount) AS INTEGER) visitsitedays,
	CAST(SUM(a0.workcasualty) AS INTEGER) workvisitdays,
	CAST(SUM(a0.privatecasualty) AS INTEGER) privatevisitdays,
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
COMMENT ON COLUMN censusambulatorycare.workvisitminutes IS 'Naive sum of emergency ambulatory care minutes, for workplace casualties, that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN censusambulatorycare.privatevisitminutes IS 'Naive sum of emergency ambulatory care minutes, for private casualties, that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN censusambulatorycare.visitminutes IS 'Naive sum of emergency ambulatory care minutes that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN censusambulatorycare.workvisitcount IS 'Emergency ambulatory care visits, for workplace casualties, in the census interval.';
COMMENT ON COLUMN censusambulatorycare.privatevisitcount IS 'Emergency ambulatory care visits in the census interval.';
COMMENT ON COLUMN censusambulatorycare.visitcount IS 'Emergency ambulatory care visits in the census interval.';
COMMENT ON COLUMN censusambulatorycare.workvisitsitedays IS 'Unique combinations of days and ambulatory care sites visited for a workplace casualty emergency in the census interval.';
COMMENT ON COLUMN censusambulatorycare.privatevisitsitedays IS 'Unique combinations of days and ambulatory care sites visited for a private casualty emergency in the census interval.';
COMMENT ON COLUMN censusambulatorycare.visitsitedays IS 'Unique combinations of days and ambulatory care sites visited for an emergency in the census interval.';
COMMENT ON COLUMN censusambulatorycare.workvisitdays IS 'Unique days of ambulatory care visits for a workplace casualty emergency in the census interval.';
COMMENT ON COLUMN censusambulatorycare.privatevisitdays IS 'Unique days of ambulatory care visits for a private casualty emergency in the census interval.';
COMMENT ON COLUMN censusambulatorycare.visitdays IS 'Unique days of ambulatory care visits for an emergency in the census interval.';