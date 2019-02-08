CREATE MATERIALIZED VIEW surveypharmacydispense NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest community pharmacy dispense events
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			hazardutilities.cleansex(a0.rcpt_gender_cd) sex,
			a0.rcpt_dob birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.dspn_date servicestart,
			a0.dspn_date serviceend,

			-- Calendar year boundaries
			hazardutilities.calendarstart(a0.dspn_date) surveillancestart,
			hazardutilities.calendarend(a0.dspn_date) surveillanceend,

			-- Postal code determines residency
			CASE substr(UPPER(a0.rcpt_postal_cd), 1, 1)
				WHEN 'T' THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.dspn_date = a0.rcpt_dob THEN
					1
				ELSE
					CAST(NULL AS INTEGER)
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.pin_dspn a0
		WHERE
			a0.dspn_date BETWEEN COALESCE(a0.rcpt_dob, a0.dspn_date) AND TRUNC(SYSDATE, 'MM')
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

COMMENT ON MATERIALIZED VIEW surveypharmacydispense IS 'Unique persons by provincial health number, from community pharmacy dispense events.';
COMMENT ON COLUMN surveypharmacydispense.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveypharmacydispense.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveypharmacydispense.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveypharmacydispense.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveypharmacydispense.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveypharmacydispense.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveypharmacydispense.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveypharmacydispense.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveypharmacydispense.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.censoreddate IS 'First day of the month of the last refresh of the data.';