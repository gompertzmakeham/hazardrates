ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW surveylaboratorycollection NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest laboratory collections
	eventdata AS
	(

		-- Laboratory Fusion
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt servicestart,
			a0.clct_dt serviceend,

			-- Fiscal year boundaries
			hazardutilities.fiscalstart(a0.clct_dt) surveillancestart,
			hazardutilities.fiscalend(a0.clct_dt) surveillanceend,

			-- Postal code determines residency
			CASE
				WHEN substr(UPPER(a0.clnt_bill_cd), 1, 2) = 'AB' THEN
					1
				WHEN substr(UPPER(a0.clnt_bill_cd), 1, 1) = '0' THEN
					1
				WHEN substr(UPPER(a0.clnt_postal_code), 1, 1) = 'T' THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.lab_lf a0
		WHERE
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL
		
		-- Laboratory Meditech
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt servicestart,
			a0.clct_dt serviceend,

			-- Fiscal year boundaries
			hazardutilities.fiscalstart(a0.clct_dt) surveillancestart,
			hazardutilities.fiscalend(a0.clct_dt) surveillanceend,

			-- Postal code determines residency
			CASE
				WHEN substr(UPPER(a0.clnt_bill_cd), 1, 3) = 'AHC' THEN
					1
				WHEN substr(UPPER(a0.clnt_postal_code), 1, 1) = 'T' THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.lab_mt a0
		WHERE
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Laboratory Millenium
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt servicestart,
			a0.clct_dt serviceend,

			-- Fiscal year boundaries
			hazardutilities.fiscalstart(a0.clct_dt) surveillancestart,
			hazardutilities.fiscalend(a0.clct_dt) surveillanceend,

			-- Postal code determines residency
			CASE
				WHEN UPPER(a0.clnt_bill_cd) IN ('AB PHN', 'REFERRED IN SPECIMEN', 'ZAADL', 'ZBLUE CROSS', 'ZLANDED IMMIGRANT', 'ZPERSONAL HEALTH NUMBER', 'ZSOCIAL SERVICES') THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.lab_ml a0
		WHERE
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Laboratory Sunquest
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt servicestart,
			a0.clct_dt serviceend,

			-- Fiscal year boundaries
			hazardutilities.fiscalstart(a0.clct_dt) surveillancestart,
			hazardutilities.fiscalend(a0.clct_dt) surveillanceend,

			-- Postal code determines residency
			CASE
				WHEN COALESCE(substr(UPPER(a0.clnt_postal_code), 1, 1), 'T') = 'T' THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.lab_sq a0
		WHERE
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
	)

-- Digest to one record per person
SELECT
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(MIN(a0.sex) AS VARCHAR2(1)) sex,
	CAST(MAX(a0.firstnations) AS INTEGER) firstnations,
	CAST(MIN(a0.birthdate) AS DATE) leastbirth,
	CAST(MAX(a0.birthdate) AS DATE) greatestbirth,
	CAST(MIN(a0.deceaseddate) AS DATE) leastdeceased,
	CAST(MAX(a0.deceaseddate) AS DATE) greatestdeceased,
	CAST(MIN(a0.servicestart) AS DATE) servicestart,
	CAST(MAX(a0.serviceend) AS DATE) serviceend,
	CAST(MIN(a0.surveillancestart) AS DATE) surveillancestart,
	CAST(MAX(a0.surveillanceend) AS DATE) surveillanceend,
	CAST(MAX(a0.surveillancestart) AS DATE)greateststart,
	CAST(MIN(a0.surveillanceend) AS DATE) leastend,
	CAST(MAX(a0.surveillancebirth) AS INTEGER) surveillancebirth,
	CAST(MAX(a0.surveillancedeceased) AS INTEGER) surveillancedeceased,
	CAST(MAX(a0.surveillanceimmigrate) AS INTEGER) surveillanceimmigrate,
	CAST(MAX(a0.surveillanceemigrate) AS INTEGER) surveillanceemigrate,
	CAST(MIN(a0.albertacoverage) AS INTEGER) albertacoverage,
	CAST(TRUNC(SYSDATE, 'MM') AS DATE) censoreddate
FROM
	eventdata a0
GROUP BY
	a0.uliabphn;

COMMENT ON MATERIALIZED VIEW surveylaboratorycollection IS 'Unique persons by provincial health number, from laboratory sample collections.';
COMMENT ON COLUMN surveylaboratorycollection.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveylaboratorycollection.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveylaboratorycollection.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveylaboratorycollection.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveylaboratorycollection.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveylaboratorycollection.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveylaboratorycollection.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveylaboratorycollection.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveylaboratorycollection.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.censoreddate IS 'First day of the month of the last refresh of the data.';