ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censuslaboratorycollection NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest laboratory collections
	eventdata AS
	(
		-- Laboratory Fusion
		SELECT
			'Fusion' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.clct_dt collectdate,
			'MDHL' siteidentifier,
			'Medicine Hat Diagnostic Laboratories' sitedescription
		FROM
			ahsdata.lab_lf a0
		WHERE
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
			ahsdata.lab_mt a0
		WHERE
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
			a0.clct_dt collectdate,
			a0.clct_loc_cd siteidentifier,
			a0.clct_loc_nm sitedescription
		FROM
			ahsdata.lab_ml a0
		WHERE
			a0.clnt_type IN ('Community', 'Day Surgery', 'Home Visit', 'Outpatient', 'Pre - Admit', 'Pre Day Care', 'Prereg', 'Recurring', 'Referred-In Specimen', 'Waitlist Outpatient')
			AND
			a0.clct_loc_cd IS NOT NULL
		UNION ALL

		-- Laboratory Sunquest
		SELECT
			'Sunquest' sourcesystem,
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			a0.clct_dt collectdate,
			a0.clnt_fac_cd siteidentifier,
			a0.clnt_fac_nm sitedescription
		FROM
			ahsdata.lab_sq a0
		WHERE
			a0.clnt_type IN ('Home Care', 'Outpatient', 'Outpatient NL')
			AND
			a0.ordr_sts_cd = 'RE'
			AND
			a0.clnt_fac_cd IS NOT NULL
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	COUNT(DISTINCT a1.collectdate) collectdays,
	COUNT(DISTINCT a1.sourcesystem || '-' || a1.siteidentifier || '-' || to_char(a1.collectdate, 'YYYYMMDD')) collectsitedays,
	COUNT(*) assaycount
FROM
	personsurveillance a0
	INNER JOIN
	eventdata a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a1.collectdate BETWEEN a0.extremumstart AND a0.extremumend
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
COMMENT ON COLUMN censuslaboratorycollection.collectdays IS 'Number of unique days in the census interval when the person had a collection taken.';
COMMENT ON COLUMN censuslaboratorycollection.collectsitedays IS 'Number unique combinations of sites and days in the census interval where the person had a collection taken.';
COMMENT ON COLUMN censuslaboratorycollection.assaycount IS 'Number assays done of samples collected in the census interval.';