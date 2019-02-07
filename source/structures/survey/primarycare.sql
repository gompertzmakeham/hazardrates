CREATE MATERIALIZED VIEW surveyprimarycare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest physician reimbursement claims
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			hazardutilities.cleansex(a0.rcpt_gender_code) sex,
			a0.rcpt_dob birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.se_start_date servicestart,
			a0.se_end_date serviceend,

			-- Fiscal year boundaries
			hazardutilities.fiscalstart(a0.se_start_date) surveillancestart,
			hazardutilities.fiscalend(a0.se_end_date) surveillanceend,

			-- Coverage determines residency
			CASE UPPER(a0.pgm_subtype_cls)
				WHEN 'BASRMEDR' THEN
					0
				ELSE
					1
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.se_start_date <= a0.rcpt_dob THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.ab_claims a0
		WHERE
			a0.se_end_date BETWEEN a0.se_start_date AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(a0.rcpt_dob, a0.se_start_date) <= a0.se_end_date
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

COMMENT ON MATERIALIZED VIEW surveyprimarycare IS 'Unique persons by provincial health number, from physician reimbursement claims.';
COMMENT ON COLUMN surveyprimarycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyprimarycare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyprimarycare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyprimarycare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyprimarycare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyprimarycare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyprimarycare.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyprimarycare.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyprimarycare.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.censoreddate IS 'First day of the month of the last refresh of the data.';