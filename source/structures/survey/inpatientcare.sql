CREATE MATERIALIZED VIEW surveyinpatientcare NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest inpatient, surveillance refresh is monthly
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
			hazardutilities.cleandate(a0.admitdate) leastservice,
			hazardutilities.cleandate(a0.disdate) greatestservice,

			-- Month boundaries of least service
			hazardutilities.monthstart(a0.admitdate) leastsurveillancestart,
			hazardutilities.monthend(a0.admitdate) leastsurveillanceend,

			-- Month boundaries of greatest service
			hazardutilities.monthstart(a0.disdate) greatestsurveillancestart,
			hazardutilities.monthend(a0.disdate) greatestsurveillanceend,

			-- Coverage by insurer
			1 albertacoverage,
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
			ahsdrrdeliver.ahs_ip_doctor_dx a0
		WHERE
			a0.resppay = '01'
			AND
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

COMMENT ON MATERIALIZED VIEW surveyinpatientcare IS 'Unique persons by provincial health number, from inpatient care discharge abstracts.';
COMMENT ON COLUMN surveyinpatientcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyinpatientcare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyinpatientcare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyinpatientcare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyinpatientcare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyinpatientcare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyinpatientcare.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyinpatientcare.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyinpatientcare.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.leastsurveillanceend IS 'End date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyinpatientcare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyinpatientcare.censoreddate IS 'First day of the month of the last refresh of the data.';