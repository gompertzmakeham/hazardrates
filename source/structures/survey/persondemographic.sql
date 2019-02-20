CREATE MATERIALIZED VIEW persondemographic NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all sources of person events
	ingestevents AS
	(
		SELECT a0.* FROM surveyambulatorycare a0 UNION ALL
		SELECT a0.* FROM surveyannualregistry a0 UNION ALL
		SELECT a0.* FROM surveycontinuingcare a0 UNION ALL
		SELECT a0.* FROM surveyinpatientcare a0 UNION ALL
		SELECT a0.* FROM surveylaboratorycollection a0 UNION ALL
		SELECT a0.* FROM surveypharmacydispense a0 UNION ALL
		SELECT a0.* FROM surveyprimarycare a0 UNION ALL
		SELECT a0.* FROM surveyvitalstatistics a0
	),

	-- Digest to one record per person
	digestevents AS
	(
		SELECT
			a0.uliabphn,
			COALESCE(MIN(a0.sex), 'O') sex,
			COALESCE(MAX(a0.firstnations), 0) firstnations,
			MIN(a0.leastbirth) leastbirth,
			MAX(a0.greatestbirth) greatestbirth,
			MIN(a0.leastdeceased) leastdeceased,
			MAX(a0.greatestdeceased) greatestdeceased,
			MIN(a0.leastservice) leastservice,
			MAX(a0.greatestservice) greatestservice,
			MIN(a0.leastsurveillancestart) leastsurveillancestart,
			MIN(a0.leastsurveillanceend) leastsurveillanceend,
			MAX(a0.greatestsurveillancestart) greatestsurveillancestart,
			MAX(a0.greatestsurveillanceend) greatestsurveillanceend,
			COALESCE(MAX(a0.surveillancebirth), 0) surveillancebirth,
			COALESCE(MAX(a0.surveillancedeceased), 0) surveillancedeceased,
			COALESCE(MAX(a0.surveillanceimmigrate), 0) surveillanceimmigrate,
			COALESCE(MAX(a0.surveillanceemigrate), 0) surveillanceemigrate,
			COALESCE(MIN(a0.albertacoverage), 1) albertacoverage,
			MAX(a0.censoreddate) censoreddate
		FROM
			ingestevents a0
		GROUP BY
			a0.uliabphn
	),

	-- Rectify life events
	lifeevents AS
	(
		SELECT
			a0.uliabphn,
			a0.sex,
			a0.firstnations,
			
			-- Estimate unobserved birth dates
			CASE a0.surveillancebirth
				WHEN 1 THEN
					COALESCE(a0.leastbirth, a0.leastsurveillancestart)
				ELSE
					a0.leastbirth
			END leastbirth,
			CASE a0.surveillancebirth
				WHEN 1 THEN
					COALESCE(a0.greatestbirth, least(a0.leastservice, a0.leastsurveillanceend))
				ELSE
					a0.greatestbirth
			END greatestbirth,

			-- Estimate unobserved deceased dates
			CASE a0.surveillancedeceased
				WHEN 1 THEN
					COALESCE(a0.leastdeceased, greatest(a0.greatestservice, a0.greatestsurveillancestart))
				ELSE
					a0.leastdeceased
			END leastdeceased,
			CASE a0.surveillancedeceased
				WHEN 1 THEN
					COALESCE(a0.greatestdeceased, a0.greatestsurveillanceend)
				ELSE
					a0.greatestdeceased
			END greatestdeceased,
			
			-- Pass through
			a0.leastservice,
			a0.greatestservice,
			a0.leastsurveillancestart,
			a0.leastsurveillanceend,
			a0.greatestsurveillancestart,
			a0.greatestsurveillanceend,
			a0.surveillancebirth,
			a0.surveillancedeceased,
			a0.surveillanceimmigrate,
			a0.surveillanceemigrate,
			a0.albertacoverage,
			a0.censoreddate
		FROM
			digestevents a0
	)
	
-- Rectify surveillance events
SELECT
	a0.uliabphn,
	a0.sex,
	a0.firstnations,
	a0.leastbirth,
	a0.greatestbirth,
	a0.leastdeceased,
	a0.greatestdeceased,
	a0.leastsurveillancestart surveillancestart,
	a0.greatestsurveillanceend surveillanceend,

	-- Birth indicators
	CASE
		WHEN a0.leastsurveillancestart <= a0.leastbirth THEN
			1
		ELSE
			0
	END leastsurveillancebirth,
	CASE
		WHEN a0.leastsurveillancestart <= a0.greatestbirth THEN
			1
		ELSE
			0
	END greatestsurveillancebirth,

	-- Deceased indicators
	CASE
		WHEN a0.leastdeceased <= a0.greatestsurveillanceend THEN
			1
		ELSE
			0
	END leastsurveillancedeceased,
	CASE
		WHEN a0.greatestdeceased <= a0.greatestsurveillanceend THEN
			1
		ELSE
			0
	END greatestsurveillancedeceased,
	
	-- Immigration indicators
	CASE
		WHEN a0.leastsurveillancestart <= a0.leastbirth THEN
			0
		ELSE
			a0.surveillanceimmigrate
	END leastsurveillanceimmigrate,
	CASE
		WHEN a0.leastsurveillancestart <= a0.greatestbirth THEN
			0
		ELSE
			a0.surveillanceimmigrate
	END greatestsurveillanceimmigrate,

	-- Emigration indicators
	CASE
		WHEN a0.leastdeceased <= a0.greatestsurveillanceend THEN
			0
		ELSE
			a0.surveillanceemigrate
	END leastsurveillanceemigrate,
	CASE
		WHEN a0.greatestdeceased <= a0.greatestsurveillanceend THEN
			0
		ELSE
			a0.surveillanceemigrate
	END greatestsurveillanceemigrate,
	
	-- Pass through
	a0.albertacoverage,
	a0.censoreddate
FROM
	lifeevents a0;