CREATE MATERIALIZED VIEW surveyambulatorycare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all ambulatory care events, surveillance refresh is monthly
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
			) leastservice,
			greatest
			(
				hazardutilities.cleandate(a0.visdate),
				COALESCE(hazardutilities.cleandate(a0.disdate), hazardutilities.cleandate(a0.visdate))
			) greatestservice,

			-- Month boundaries of least service
			least
			(
				hazardutilities.monthstart(a0.visdate),
				COALESCE(hazardutilities.monthstart(a0.disdate), hazardutilities.monthstart(a0.visdate))
			) leastsurveillancestart,
			least
			(
				hazardutilities.monthend(a0.visdate),
				COALESCE(hazardutilities.monthend(a0.disdate), hazardutilities.monthend(a0.visdate))
			) leastsurveillanceend,

			-- Month boundaries of greatest service
			greatest
			(
				hazardutilities.monthstart(a0.visdate),
				COALESCE(hazardutilities.monthstart(a0.disdate), hazardutilities.monthstart(a0.visdate))
			) greatestsurveillancestart,
			greatest
			(
				hazardutilities.monthend(a0.visdate),
				COALESCE(hazardutilities.monthend(a0.disdate), hazardutilities.monthend(a0.visdate))
			) greatestsurveillanceend,

			-- Coverage by insurer
			1 albertacoverage,
			CASE a0.resppay
				WHEN '05' THEN
					1
				ELSE
					0
			END firstnations,
			
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
			ahsdrrdeliver.ahs_ambulatory a0
		WHERE
			hazardutilities.cleanphn(a0.phn) IS NOT NULL
			AND
			a0.resppay IN ('01', '02', '05')
			AND
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
			) leastservice,
			greatest
			(
				hazardutilities.cleandate(a0.visit_date),
				COALESCE(hazardutilities.cleandate(a0.disp_date), hazardutilities.cleandate(a0.visit_date))
			) greatestservice,

			-- Month boundaries of least service
			least
			(
				hazardutilities.monthstart(a0.visit_date),
				COALESCE(hazardutilities.monthstart(a0.disp_date), hazardutilities.monthstart(a0.visit_date))
			) leastsurveillancestart,
			least
			(
				hazardutilities.monthend(a0.visit_date),
				COALESCE(hazardutilities.monthend(a0.disp_date), hazardutilities.monthend(a0.visit_date))
			) leastsurveillanceend,

			-- Month boundaries of greatest service
			greatest
			(
				hazardutilities.monthstart(a0.visit_date),
				COALESCE(hazardutilities.monthstart(a0.disp_date), hazardutilities.monthstart(a0.visit_date))
			) greatestsurveillancestart,
			greatest
			(
				hazardutilities.monthend(a0.visit_date),
				COALESCE(hazardutilities.monthend(a0.disp_date), hazardutilities.monthend(a0.visit_date))
			) greatestsurveillanceend,

			-- Coverage by insurer
			1 albertacoverage,
			CASE a0.resppay
				WHEN '05' THEN
					1
				ELSE
					0
			END firstnations,
			
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
			ahsdrrdeliver.ahs_nacrs_tab a0
		WHERE
			hazardutilities.cleanphn(a0.phn) IS NOT NULL
			AND
			a0.resppay IN ('01', '02', '05')
			AND
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

COMMENT ON MATERIALIZED VIEW surveyambulatorycare IS 'Unique persons by provincial health number, from ambulatory care discharge abstracts.';
COMMENT ON COLUMN surveyambulatorycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyambulatorycare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyambulatorycare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyambulatorycare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyambulatorycare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyambulatorycare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyambulatorycare.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyambulatorycare.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyambulatorycare.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.leastsurveillanceend IS 'End date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyambulatorycare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyambulatorycare.censoreddate IS 'First day of the month of the last refresh of the data.';