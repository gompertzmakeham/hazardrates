CREATE MATERIALIZED VIEW censuslaboratorycollection NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest laboratory collections
	eventdata AS
	(
		-- Laboratory Fusion
		SELECT
			'Fusion' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.collect_dt collectdate,
			'MDHL' siteidentifier,
			'Medicine Hat Diagnostic Laboratories' sitedescription
		FROM
			ahsdrrconform.cf_lab_labfusion a0
		WHERE
			(substr(a0.clnt_bill_id, 1, 2) = 'AB' OR substr(a0.clnt_bill_id, 1, 1) = '0')
			AND
			a0.clnt_type = 'OP'
		UNION ALL

		-- Laboratory Meditech
		SELECT
			'Meditech' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.clct_dt collectdate,
			a0.clct_loc_cd siteidentifier,
			a0.clct_loc_nm sitedescription
		FROM
			ahsdrrconform.lab_mt a0
		WHERE
			substr(a0.clnt_bill_cd, 1, 3) = 'AHC'
			AND
			a0.clnt_type IN ('Outpatient', 'Recurring', 'Referred', 'Surgical Day Care')
			AND
			a0.ordr_sts_cd = 'COMP'
			AND
			a0.clct_loc_cd IS NOT NULL
		UNION ALL

		-- Laboratory Millenium
		SELECT
			'Millenium' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.collect_dt collectdate,
			a0.collect_location_id siteidentifier,
			a0.collect_location_desc sitedescription
		FROM
			ahsdrrconform.cf_lab_millennium a0
		WHERE
			a0.clnt_bill_id IN ('AB PHN', 'REFERRED IN SPECIMEN', 'ZAADL', 'ZBLUE CROSS', 'ZLANDED IMMIGRANT', 'ZPERSONAL HEALTH NUMBER', 'ZSOCIAL SERVICES')
			AND
			a0.clnt_type IN ('Community', 'Day Surgery', 'Home Visit', 'Outpatient', 'Pre - Admit', 'Pre Day Care', 'Prereg', 'Recurring', 'Referred-In Specimen', 'Waitlist Outpatient')
			AND
			a0.collect_location_id IS NOT NULL
		UNION ALL

		-- Laboratory Sunquest
		SELECT
			'Sunquest' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.clct_dt collectdate,
			a0.clnt_fac_cd siteidentifier,
			a0.clnt_fac_nm sitedescription
		FROM
			ahsdrrconform.lab_sq a0
		WHERE
			a0.clnt_type IN ('Home Care', 'Outpatient', 'Outpatient NL')
			AND
			a0.ordr_sts_cd = 'RE'
			AND
			a0.clnt_fac_cd IS NOT NULL
	),

	-- Digest to one record per patient per day per laboratory
	sitedata AS
	(
		SELECT
			a0.uliabphn,
			a0.sourcesystem,
			a0.siteidentifier,
			a0.collectdate,
			COUNT(*) assaycount
		FROM
			eventdata a0
		GROUP BY
			a0.uliabphn,
			a0.sourcesystem,
			a0.siteidentifier,
			a0.collectdate
	),

	-- Digest to one record per patient per day
	daydata AS
	(
		SELECT
			a0.uliabphn,
			a0.collectdate,
			SUM(a0.assaycount) assaycount,
			COUNT(*) sitecount
		FROM
			sitedata a0
		GROUP BY
			a0.uliabphn,
			a0.collectdate
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a1.assaycount) AS INTEGER) assaycount,
	CAST(SUM(a1.sitecount) AS INTEGER) collectsitedays,
	CAST(COUNT(*) AS INTEGER) collectdays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.collectdate, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censuslaboratorycollection IS 'Utilization of community laboratories in census intervals of each person.';
COMMENT ON COLUMN censuslaboratorycollection.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censuslaboratorycollection.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censuslaboratorycollection.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslaboratorycollection.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslaboratorycollection.assaycount IS 'Number assays done of samples collected in the census interval.';
COMMENT ON COLUMN censuslaboratorycollection.collectsitedays IS 'Number unique combinations of sites and days in the census interval where the person had a collection taken.';
COMMENT ON COLUMN censuslaboratorycollection.collectdays IS 'Number of unique days in the census interval when the person had a collection taken.';