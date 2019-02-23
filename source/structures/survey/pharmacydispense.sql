CREATE MATERIALIZED VIEW surveypharmacydispense NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest community pharmacy dispense events, surveillance refresh is weekly
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			hazardutilities.cleansex(a0.rcpt_gender_cd) sex,
			a0.rcpt_dob birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.dspn_date leastservice,
			a0.dspn_date greatestservice,

			-- Week boundaries or least service
			hazardutilities.weekstart(a0.dspn_date) leastsurveillancestart,
			hazardutilities.weekend(a0.dspn_date) leastsurveillanceend,

			-- Week boundaries or greatest service
			hazardutilities.weekstart(a0.dspn_date) greatestsurveillancestart,
			hazardutilities.weekend(a0.dspn_date) greatestsurveillanceend,

			-- Coverage unknown
			CAST(NULL AS INTEGER) albertacoverage,
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
			ahsdrrconform.cf_pin_dspn a0
		WHERE
			hazardutilities.cleanphn(a0.rcpt_uli) IS NOT NULL
			AND
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
	CAST(MIN(a0.leastservice) AS DATE) leastservice,
	CAST(MAX(a0.greatestservice) AS DATE) greatestservice,
	CAST(MIN(a0.leastsurveillancestart) AS DATE) leastsurveillancestart,
	CAST(MIN(a0.leastsurveillanceend) AS DATE) leastsurveillanceend,
	CAST(MAX(a0.greatestsurveillancestart) AS DATE) greatestsurveillancestart,
	CAST(MAX(a0.greatestsurveillanceend) AS DATE) greatestsurveillanceend,
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
COMMENT ON COLUMN surveypharmacydispense.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveypharmacydispense.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveypharmacydispense.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.leastsurveillanceend IS 'End date of the least observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveypharmacydispense.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveypharmacydispense.censoreddate IS 'First day of the month of the last refresh of the data.';