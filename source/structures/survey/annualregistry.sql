ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW surveyannualregistry NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest registry census
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.phn) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			a0.birth_dt birthdate,

			-- Exact death is the end date
			CASE
				WHEN a0.death_ind = '1' THEN
					a0.pers_reap_end_date
				WHEN a0.pers_reap_end_rsn_code = 'D' THEN
					a0.pers_reap_end_date
				ELSE
					CAST(NULL AS DATE)
			END deceaseddate,

			-- Service boundaries
			CASE
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') <= a0.birth_dt THEN
					a0.birth_dt
				WHEN a0.pers_reap_end_date IS NOT NULL THEN
					a0.pers_reap_end_date
				ELSE
					NULL
			END servicestart,
			CASE
				WHEN a0.pers_reap_end_date IS NOT NULL THEN
					hazardutilities.cleandate(a0.pers_reap_end_date)
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') <= a0.birth_dt THEN
					a0.birth_dt
				ELSE
					NULL
			END serviceend,

			-- Fiscal year boundaries
			CASE
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') <= a0.birth_dt THEN
					hazardutilities.fiscalstart(a0.birth_dt)
				WHEN a0.pers_reap_end_date IS NOT NULL THEN
					hazardutilities.fiscalstart(a0.pers_reap_end_date)
				ELSE
					hazardutilities.cleandate(a0.fye - 1 || '0401')
			END surveillancestart,
			CASE
				WHEN a0.pers_reap_end_date IS NOT NULL THEN
					hazardutilities.fiscalend(a0.pers_reap_end_date)
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') <= a0.birth_dt THEN
					hazardutilities.fiscalend(a0.birth_dt)
				ELSE
					hazardutilities.cleandate(a0.fye || '0331')
			END surveillanceend,
			1 albertacoverage,

			-- Any indication of aboriginal, first nations, indigineous, Metis, or Inuit status
			CASE
				WHEN a0.aborg_sai = '1' THEN
					1
				WHEN a0.aborg_sai_src IN ('1', 'Y') THEN
					1
				WHEN a0.alt_prem_arrangement = 'A' THEN
					1
				ELSE
					0
			END firstnations,

			-- Birth observed
			CASE
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') <= a0.birth_dt THEN
					1
				WHEN a0.birth_dt IS NULL AND a0.birth_ind = '1' THEN
					1
				ELSE
					0
			END surveillancebirth,

			-- Death observed
			CASE
				WHEN a0.death_ind = '1' THEN
					1
				WHEN a0.pers_reap_end_rsn_code = 'D' THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CASE in_migration_ind WHEN '1' THEN 1 ELSE 0 END surveillanceimmigrate,
			CASE out_migration_ind WHEN '1' THEN 1 ELSE 0 END surveillanceemigrate
		FROM
			ahsdata.provincial_registry a0
		WHERE
			COALESCE(a0.pers_reap_end_date, a0.birth_dt, TRUNC(SYSDATE, 'MM')) BETWEEN COALESCE(a0.birth_dt, a0.pers_reap_end_date, TRUNC(SYSDATE, 'MM')) AND TRUNC(SYSDATE, 'MM')
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

COMMENT ON MATERIALIZED VIEW surveyannualregistry IS 'Unique persons by provincial health number, from fiscal annual registry census.';
COMMENT ON COLUMN surveyannualregistry.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyannualregistry.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyannualregistry.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyannualregistry.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyannualregistry.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyannualregistry.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyannualregistry.servicestart IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyannualregistry.serviceend IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyannualregistry.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyannualregistry.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN surveyannualregistry.greateststart IS 'Last start date of the observation bounds of the person.';
COMMENT ON COLUMN surveyannualregistry.leastend IS 'First end date of the observation bounds of the person.';
COMMENT ON COLUMN surveyannualregistry.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyannualregistry.censoreddate IS 'First day of the month of the last refresh of the data.';