CREATE MATERIALIZED VIEW surveyprimarycare NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest physician reimbursement claims, surveillance refresh is quarterly
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			hazardutilities.cleansex(a0.rcpt_gender_code) sex,
			a0.rcpt_dob birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.se_start_date leastservice,
			a0.se_end_date greatestservice,

			-- Quarter boundaries of least service
			hazardutilities.quarterstart(a0.se_start_date) leastsurveillancestart,
			hazardutilities.quarterend(a0.se_start_date) leastsurveillanceend,

			-- Quarter boundaries of greatest service
			hazardutilities.quarterstart(a0.se_end_date) greatestsurveillancestart,
			hazardutilities.quarterend(a0.se_end_date) greatestsurveillanceend,

			-- Coverage by insurer
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
			ahsdrrconform.ab_claims a0
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

COMMENT ON MATERIALIZED VIEW surveyprimarycare IS 'Unique persons by provincial health number, from physician reimbursement claims.';
COMMENT ON COLUMN surveyprimarycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyprimarycare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyprimarycare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyprimarycare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyprimarycare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyprimarycare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyprimarycare.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyprimarycare.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyprimarycare.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.leastsurveillanceend IS 'End date of the leastobservation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyprimarycare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyprimarycare.censoreddate IS 'First day of the month of the last refresh of the data.';