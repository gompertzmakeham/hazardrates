ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW surveycontinuingcare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all continuing care events
	eventdata AS
	(

		-- Home care and supportive living
		SELECT
			hazardutilities.cleanphn(a0.uli_ab_phn) uliabphn,
			hazardutilities.cleansex(a0.reported_gender) sex,
			a0.birth_date birthdate,
			a0.deceased_date deceaseddate,

			-- Service boundaries
			a0.first_appear_date servicestart,
			a0.last_known_date serviceend,

			-- Calendar year boundaries
			hazardutilities.calendarstart(a0.first_appear_date) surveillancestart,
			hazardutilities.calendarend(a0.last_known_date) surveillanceend,
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.first_appear_date <= a0.birth_date THEN
					1
				ELSE
					0
			END surveillancebirth,
			
			-- Death observed
			CASE
				WHEN a0.deceased_date IS NOT NULL THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			TABLE(continuing_care.home_care.get_client) a0
		WHERE
			a0.last_known_date BETWEEN a0.first_appear_date AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(a0.birth_date, a0.last_known_date) <= a0.last_known_date
			AND
			a0.first_appear_date <= COALESCE(a0.deceased_date, a0.first_appear_date)
		UNION ALL

		-- Long term care
		SELECT
		
			/*+ parrallel(8) */
			hazardutilities.cleanphn(a0.uli_ab_phn) uliabphn,
			hazardutilities.cleansex(a0.reported_gender) sex,
			a0.birth_date birthdate,
			
			-- Exact deceased date
			CASE a0.discharge_disposition_code
				WHEN '11' THEN
					a0.discharge_date
				ELSE
					NULL
			END deceaseddate,

			-- Service boundaries
			a0.admit_date servicestart,
			COALESCE(a0.discharge_date, TRUNC(SYSDATE, 'MM')) serviceend,

			-- Calendar year boundaries
			hazardutilities.calendarstart(a0.admit_date) surveillancestart,
			COALESCE
			(
				hazardutilities.calendarend(a0.discharge_date),
				hazardutilities.calendarend(TRUNC(SYSDATE, 'MM'))
			) surveillanceend,
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.admit_date <= a0.birth_date THEN
					1
				ELSE
					0
			END surveillancebirth,
			
			-- Death observed
			CASE a0.discharge_disposition_code
				WHEN '11' THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			TABLE(continuing_care.accis.get_adt) a0
		WHERE
			COALESCE(a0.discharge_date, TRUNC(SYSDATE, 'MM')) BETWEEN a0.admit_date AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(a0.birth_date, a0.admit_date) <= COALESCE(a0.discharge_date, TRUNC(SYSDATE, 'MM'))
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

COMMENT ON MATERIALIZED VIEW surveycontinuingcare IS 'Unique persons by provincial health number, from continuing care.';
COMMENT ON COLUMN surveycontinuingcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveycontinuingcare.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveycontinuingcare.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveycontinuingcare.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveycontinuingcare.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveycontinuingcare.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveycontinuingcare.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveycontinuingcare.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveycontinuingcare.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveycontinuingcare.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveycontinuingcare.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveycontinuingcare.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveycontinuingcare.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveycontinuingcare.censoreddate IS 'First day of the month of the last refresh of the data.';