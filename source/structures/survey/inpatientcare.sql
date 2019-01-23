CREATE MATERIALIZED VIEW surveyinpatientcare NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest inpatient
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			hazardutilities.cleandate(a0.birthdate) birthdate,

			-- Exact death is the date of stay
			CASE
				WHEN a0.deador = 'Y' THEN
					hazardutilities.cleandate(a0.disdate)
				WHEN a0.deadscu = 'Y' THEN
					hazardutilities.cleandate(a0.disdate)
				WHEN a0.disp IN ('07', '08', '09', '66', '67', '72', '73', '74') THEN
					hazardutilities.cleandate(a0.disdate)
				ELSE
					CAST(NULL AS DATE)
			END deceaseddate,

			-- Service boundaries
			CASE
				WHEN a0.admitcat = 'N' THEN
					least
					(
						hazardutilities.cleandate(a0.admitdate),
						COALESCE(hazardutilities.cleandate(a0.birthdate), hazardutilities.cleandate(a0.admitdate))
					)
				WHEN a0.entrycode = 'N' THEN
					least
					(
						hazardutilities.cleandate(a0.admitdate),
						COALESCE(hazardutilities.cleandate(a0.birthdate), hazardutilities.cleandate(a0.admitdate))
					)
				ELSE
					hazardutilities.cleandate(a0.admitdate)
			END servicestart,
			hazardutilities.cleandate(a0.disdate) serviceend,

			-- Fiscal year boundaries
			CASE
				WHEN a0.admitcat = 'N' THEN
					least
					(
						hazardutilities.fiscalstart(a0.admitdate),
						COALESCE(hazardutilities.fiscalstart(a0.birthdate), hazardutilities.fiscalstart(a0.admitdate))
					)
				WHEN a0.entrycode = 'N' THEN
					least
					(
						hazardutilities.fiscalstart(a0.admitdate),
						COALESCE(hazardutilities.fiscalstart(a0.birthdate), hazardutilities.fiscalstart(a0.admitdate))
					)
				ELSE
					hazardutilities.fiscalstart(a0.admitdate)
			END surveillancestart,
			hazardutilities.fiscalend(a0.disdate) surveillanceend,

			-- Coverage determines residency
			CASE a0.resppay
				WHEN '01' THEN
					1
				ELSE
					0
			END albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN hazardutilities.cleandate(a0.admitdate) <= hazardutilities.cleandate(a0.birthdate) THEN
					1
				WHEN a0.admitcat = 'N' THEN
					1
				WHEN a0.entrycode = 'N' THEN
					1
				ELSE
					0
			END surveillancebirth,
			
			-- Death observed
			CASE
				WHEN a0.deador = 'Y' THEN
					1
				WHEN a0.deadscu = 'Y' THEN
					1
				WHEN a0.disp IN ('07', '08', '09', '66', '67', '72', '73', '74') THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdata.ahs_ip_doc_dx_w_lloyd a0
		WHERE
			hazardutilities.cleandate(a0.disdate) BETWEEN hazardutilities.cleandate(a0.admitdate) AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(hazardutilities.cleandate(a0.birthdate), hazardutilities.cleandate(a0.admitdate)) <= hazardutilities.cleandate(a0.disdate)
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

COMMENT ON MATERIALIZED VIEW surveyinpatientcare IS 'Unique persons by provincial health number, from inpatient care discharge abstracts.';
COMMENT ON COLUMN surveyinpatientcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyinpatientcare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyinpatientcare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyinpatientcare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyinpatientcare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyinpatientcare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyinpatientcare.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyinpatientcare.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyinpatientcare.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.censoreddate IS 'First day of the month of the last refresh of the data.';