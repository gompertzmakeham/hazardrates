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
	)

-- Rectify life events
SELECT

	/*+ cardinality(a1, 1) */
	a0.uliabphn,
	a0.sex,
	a0.firstnations,
	a1.leastbirth,
	a1.greatestbirth,
	a1.leastdeceased,
	a1.greatestdeceased,
	a1.leastimmigrate,
	a1.greatestimmigrate,
	a1.leastemigrate,
	a1.greatestemigrate,
	a1.surveillancestart,
	a1.surveillanceend,
	a1.birthdateequipoise,
	a1.deceaseddateequipoise,
	a1.birthobservationequipoise,
	a1.deceasedobservationequipoise,
	a1.immigratedateequipoise,
	a1.emigratedateequipoise,
	a1.immigrateobservationequipoise,
	a1.emigrateobservationequipoise,
	a1.surveillancestartequipoise,
	a1.surveillanceendequipoise,
	a1.ageequipoise,
	a0.albertacoverage,
	a0.censoreddate
FROM
	digestevents a0
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatedemographic
		(
			a0.leastbirth,
			a0.greatestbirth,
			a0.leastdeceased,
			a0.greatestdeceased,
			a0.leastservice,
			a0.greatestservice,
			a0.leastsurveillancestart,
			a0.leastsurveillanceend,
			a0.greatestsurveillancestart,
			a0.greatestsurveillanceend,
			a0.surveillancebirth,
			a0.surveillancedeceased,
			a0.surveillanceimmigrate,
			a0.surveillanceemigrate
		)
	) a1;