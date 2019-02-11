CREATE MATERIALIZED VIEW surveyambulatorycare NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all ambulatory care events
	eventdata AS
	(

		-- AACRS historic
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			hazardutilities.cleandate(a0.birthdate) birthdate,

			-- Exact death is the date of stay
			CASE
				WHEN a0.disp IN ('7', '8') THEN
					greatest
					(
						hazardutilities.cleandate(a0.visdate),
						COALESCE(hazardutilities.cleandate(a0.disdate), hazardutilities.cleandate(a0.visdate))
					)
				ELSE
					CAST(NULL AS DATE)
			END deceaseddate,

			-- Service boundaries 
			least
			(
				hazardutilities.cleandate(a0.visdate),
				COALESCE(hazardutilities.cleandate(a0.disdate), hazardutilities.cleandate(a0.visdate))
			) servicestart,
			greatest
			(
				hazardutilities.cleandate(a0.visdate),
				COALESCE(hazardutilities.cleandate(a0.disdate), hazardutilities.cleandate(a0.visdate))
			) serviceend,

			-- Fiscal year boundaries
			least
			(
				hazardutilities.fiscalstart(a0.visdate),
				COALESCE(hazardutilities.fiscalstart(a0.disdate), hazardutilities.fiscalstart(a0.visdate))
			) surveillancestart,
			greatest
			(
				hazardutilities.fiscalend(a0.visdate),
				COALESCE(hazardutilities.fiscalend(a0.disdate), hazardutilities.fiscalend(a0.visdate))
			) surveillanceend,

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
				WHEN hazardutilities.cleandate(a0.visdate) <= hazardutilities.cleandate(a0.birthdate) THEN
					1
				WHEN hazardutilities.cleandate(a0.disdate) <= hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
			END surveillancebirth,
			
			-- Death observed
			CASE
				WHEN a0.disp IN ('7', '8') THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrdeliver.ahs_ambulatory@local.world a0
		WHERE
			hazardutilities.cleandate(a0.visdate) <= TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(hazardutilities.cleandate(a0.disdate), TRUNC(SYSDATE, 'MM')) <= TRUNC(SYSDATE, 'MM')
			AND
			(
				hazardutilities.cleandate(a0.birthdate) IS NULL
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.visdate)
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.disdate)
			)
		UNION ALL

		-- NACRS current
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleansex(a0.gender) sex,
			hazardutilities.cleandate(a0.birthdate) birthdate,

			-- Exact death is the date of stay
			CASE
				WHEN a0.disposition IN ('10', '11', '71', '72', '73', '74') THEN
					greatest
					(
						hazardutilities.cleandate(a0.visit_date),
						COALESCE(hazardutilities.cleandate(a0.disp_date), hazardutilities.cleandate(a0.visit_date))
					)
				ELSE
					CAST(NULL AS DATE)
			END deceaseddate,

			-- Service boundaries 
			least
			(
				hazardutilities.cleandate(a0.visit_date),
				COALESCE(hazardutilities.cleandate(a0.disp_date), hazardutilities.cleandate(a0.visit_date))
			) servicestart,
			greatest
			(
				hazardutilities.cleandate(a0.visit_date),
				COALESCE(hazardutilities.cleandate(a0.disp_date), hazardutilities.cleandate(a0.visit_date))
			) serviceend,

			-- Fiscal year boundaries
			least
			(
				hazardutilities.fiscalstart(a0.visit_date),
				COALESCE(hazardutilities.fiscalstart(a0.disp_date), hazardutilities.fiscalstart(a0.visit_date))
			) surveillancestart,
			greatest
			(
				hazardutilities.fiscalend(a0.visit_date),
				COALESCE(hazardutilities.fiscalend(a0.disp_date), hazardutilities.fiscalend(a0.visit_date))
			) surveillanceend,

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
				WHEN hazardutilities.cleandate(a0.visit_date) <= hazardutilities.cleandate(a0.birthdate) THEN
					1
				WHEN hazardutilities.cleandate(a0.disp_date) <= hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
			END surveillancebirth,
			
			-- Death observed
			CASE
				WHEN a0.disposition IN ('10', '11', '71', '72', '73', '74') THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrdeliver.ahs_nacrs_tab@local.world a0
		WHERE
			hazardutilities.cleandate(a0.visit_date) <= TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(hazardutilities.cleandate(a0.disp_date), TRUNC(SYSDATE, 'MM')) <= TRUNC(SYSDATE, 'MM')
			AND
			(
				hazardutilities.cleandate(a0.birthdate) IS NULL
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.visit_date)
				OR
				hazardutilities.cleandate(a0.birthdate) <= hazardutilities.cleandate(a0.disp_date)
			)
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

COMMENT ON MATERIALIZED VIEW surveyambulatorycare IS 'Unique persons by provincial health number, from ambulatory care discharge abstracts.';
COMMENT ON COLUMN surveyambulatorycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyambulatorycare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyambulatorycare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyambulatorycare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyambulatorycare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyambulatorycare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyambulatorycare.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyambulatorycare.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyambulatorycare.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.censoreddate IS 'First day of the month of the last refresh of the data.';