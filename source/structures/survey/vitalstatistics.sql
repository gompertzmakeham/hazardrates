CREATE MATERIALIZED VIEW surveyvitalstatistics NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all vital statistics, surveillance refresh is calendar annually
	eventdata AS
	(

		-- Births 2000 to 2016
		SELECT
			hazardutilities.cleanphn(a0.stkh_num_1) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			a0.birth_date birthdate,
			a0.oop_death_ deceaseddate,

			-- Service boundaries
			a0.birth_date leastservice,
			COALESCE(a0.oop_death_, a0.birth_date) greatestservice,

			-- Calendar year boundaries of least service
			hazardutilities.calendarstart(a0.birth_date) leastsurveillancestart,
			hazardutilities.calendarend(a0.birth_date) leastsurveillanceend,

			-- Calendar year boundaries of greatest service
			COALESCE
			(
				hazardutilities.calendarstart(a0.oop_death_),
				hazardutilities.calendarstart(a0.birth_date)
			) greatestsurveillancestart,
			COALESCE
			(
				hazardutilities.calendarend(a0.oop_death_),
				hazardutilities.calendarend(a0.birth_date)
			) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			1 surveillancebirth,
			
			-- Death observed
			CASE
				WHEN a0.oop_death_ IS NOT NULL THEN
					1
				ELSE
					0
			END surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ex_ah_bth_2000_2016 a0
		WHERE
			hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
			AND
			COALESCE(a0.oop_death_, TRUNC(SYSDATE, 'MM')) BETWEEN a0.birth_date AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Births 2015 to 2017
		SELECT
			hazardutilities.cleanphn(a0.stkh_num_1) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			a0.birth_date birthdate,
			CAST(NULL AS DATE) deceaseddate,
			
			-- Service boundaries
			a0.birth_date leastservice,
			a0.birth_date greatestservice,
			
			-- Calendar year boundaries of least service
			hazardutilities.calendarstart(a0.birth_date) leastsurveillancestart,
			hazardutilities.calendarend(a0.birth_date) leastsurveillanceend,
			
			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.birth_date) greatestsurveillancestart,
			hazardutilities.calendarend(a0.birth_date) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			1 surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ahs_bth_2015_2017 a0
		WHERE
			hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
			AND
			a0.birth_date <= TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Births 2000 to 2015
		SELECT
			hazardutilities.cleanphn(a0.primary_uli_c) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			a0.birth_date birthdate,
			CAST(NULL AS DATE) deceaseddate,
			
			-- Service boundaries
			a0.birth_date leastservice,
			a0.birth_date greatestservice,
			
			-- Calendar year boundaries of least service
			hazardutilities.calendarstart(a0.birth_date) leastsurveillancestart,
			hazardutilities.calendarend(a0.birth_date) leastsurveillanceend,
			
			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.birth_date) greatestsurveillancestart,
			hazardutilities.calendarend(a0.birth_date) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			1 surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ex_vs_bth_2000_2015 a0
		WHERE
			hazardutilities.cleanphn(a0.primary_uli_c) IS NOT NULL
			AND
			a0.birth_date <= TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Deaths 2003 to 2010
		SELECT
			hazardutilities.cleanphn(a0.stkh_num_1) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					CAST(NULL AS DATE)
				ELSE
					a0.birth_date
			END birthdate,
			a0.dethdate deceaseddate,

			-- Service boundaries
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					a0.dethdate
				WHEN a0.birth_date IS NULL THEN
					a0.dethdate
				WHEN a0.inf_deth = 1 THEN
					a0.birth_date
				WHEN a0.neonatal = 1 THEN
					a0.birth_date
				WHEN a0.early_neo = 1 THEN
					a0.birth_date
				ELSE
					a0.dethdate
			END leastservice,
			a0.dethdate greatestservice,
			
			-- Calendar year boundaries of least service
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				ELSE
					hazardutilities.calendarstart(a0.dethdate)
			END leastsurveillancestart,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				ELSE
					hazardutilities.calendarend(a0.dethdate)
			END leastsurveillanceend,

			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.dethdate) greatestsurveillancestart,
			hazardutilities.calendarend(a0.dethdate) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.inf_deth = 1 THEN
					1
				WHEN a0.neonatal = 1 THEN
					1
				WHEN a0.early_neo = 1 THEN
					1
				ELSE
					0
			END surveillancebirth,
			1 surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ahs_dth_2003_2010 a0
		WHERE
			hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
			AND
			a0.dethdate BETWEEN COALESCE(a0.birth_date, a0.dethdate) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Deaths 2010 to 2016
		SELECT
			hazardutilities.cleanphn(a0.stkh_num_1) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					CAST(NULL AS DATE)
				ELSE
					a0.birth_date
			END birthdate,
			a0.dethdate deceaseddate,

			-- Service boundaries
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					a0.dethdate
				WHEN a0.birth_date IS NULL THEN
					a0.dethdate
				WHEN a0.inf_deth = 1 THEN
					a0.birth_date
				WHEN a0.neonatal = 1 THEN
					a0.birth_date
				WHEN a0.early_neo = 1 THEN
					a0.birth_date
				ELSE
					a0.dethdate
			END leastservice,
			a0.dethdate greatestservice,
			
			-- Calendar year boundaries of least service
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				ELSE
					hazardutilities.calendarstart(a0.dethdate)
			END leastsurveillancestart,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				ELSE
					hazardutilities.calendarend(a0.dethdate)
			END leastsurveillanceend,

			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.dethdate) greatestsurveillancestart,
			hazardutilities.calendarend(a0.dethdate) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.inf_deth = 1 THEN
					1
				WHEN a0.neonatal = 1 THEN
					1
				WHEN a0.early_neo = 1 THEN
					1
				ELSE
					0
			END surveillancebirth,
			1 surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ex_ah_dth_2010_2016 a0
		WHERE
			hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
			AND
			a0.dethdate BETWEEN COALESCE(a0.birth_date, a0.dethdate) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Deaths 2015 to 2017
		SELECT
			hazardutilities.cleanphn(a0.stkh_num_1) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					CAST(NULL AS DATE)
				ELSE
					a0.birth_date
			END birthdate,
			a0.dethdate deceaseddate,

			-- Service boundaries
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					a0.dethdate
				WHEN a0.birth_date IS NULL THEN
					a0.dethdate
				WHEN a0.inf_deth = 1 THEN
					a0.birth_date
				WHEN a0.neonatal = 1 THEN
					a0.birth_date
				WHEN a0.early_neo = 1 THEN
					a0.birth_date
				ELSE
					a0.dethdate
			END leastservice,
			a0.dethdate greatestservice,
			
			-- Calendar year boundaries of least service
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				ELSE
					hazardutilities.calendarstart(a0.dethdate)
			END leastsurveillancestart,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				ELSE
					hazardutilities.calendarend(a0.dethdate)
			END leastsurveillanceend,

			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.dethdate) greatestsurveillancestart,
			hazardutilities.calendarend(a0.dethdate) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.inf_deth = 1 THEN
					1
				WHEN a0.neonatal = 1 THEN
					1
				WHEN a0.early_neo = 1 THEN
					1
				ELSE
					0
			END surveillancebirth,
			1 surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ahs_dth_2015_2017 a0
		WHERE
			hazardutilities.cleanphn(a0.stkh_num_1) IS NOT NULL
			AND
			a0.dethdate BETWEEN COALESCE(a0.birth_date, a0.dethdate) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Deaths early
		SELECT
			hazardutilities.cleanphn(a0.primary_uli) uliabphn,
			hazardutilities.cleansex(a0.sex) sex,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					CAST(NULL AS DATE)
				ELSE
					a0.birth_date
			END birthdate,
			a0.dethdate deceaseddate,

			-- Service boundaries
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					a0.dethdate
				WHEN a0.birth_date IS NULL THEN
					a0.dethdate
				WHEN a0.inf_deth = 1 THEN
					a0.birth_date
				WHEN a0.neonatal = 1 THEN
					a0.birth_date
				WHEN a0.early_neo = 1 THEN
					a0.birth_date
				ELSE
					a0.dethdate
			END leastservice,
			a0.dethdate greatestservice,
			
			-- Calendar year boundaries of least service
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarstart(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarstart(a0.birth_date)
				ELSE
					hazardutilities.calendarstart(a0.dethdate)
			END leastsurveillancestart,
			CASE
				WHEN a0.birth_date < add_months(a0.dethdate, -12 * (1 + COALESCE(a0.age_yrs, a0.age))) THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.birth_date IS NULL THEN
					hazardutilities.calendarend(a0.dethdate)
				WHEN a0.inf_deth = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.neonatal = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				WHEN a0.early_neo = 1 THEN
					hazardutilities.calendarend(a0.birth_date)
				ELSE
					hazardutilities.calendarend(a0.dethdate)
			END leastsurveillanceend,

			-- Calendar year boundaries of greatest service
			hazardutilities.calendarstart(a0.dethdate) greatestsurveillancestart,
			hazardutilities.calendarend(a0.dethdate) greatestsurveillanceend,

			-- Definitive coverage
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observed
			CASE
				WHEN a0.inf_deth = 1 THEN
					1
				WHEN a0.neonatal = 1 THEN
					1
				WHEN a0.early_neo = 1 THEN
					1
				ELSE
					0
			END surveillancebirth,
			1 surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			vital_stats_dsp.ex_vs_deaths_phn a0
		WHERE
			hazardutilities.cleanphn(a0.primary_uli) IS NOT NULL
			AND
			a0.dethdate BETWEEN COALESCE(a0.birth_date, a0.dethdate) AND TRUNC(SYSDATE, 'MM')
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

COMMENT ON MATERIALIZED VIEW surveyvitalstatistics IS 'Unique persons by provincial health number, from vital statistics.';
COMMENT ON COLUMN surveyvitalstatistics.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveyvitalstatistics.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveyvitalstatistics.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveyvitalstatistics.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveyvitalstatistics.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveyvitalstatistics.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveyvitalstatistics.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveyvitalstatistics.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveyvitalstatistics.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyvitalstatistics.leastsurveillanceend IS 'End date of the least observation bounds of the person.';
COMMENT ON COLUMN surveyvitalstatistics.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyvitalstatistics.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveyvitalstatistics.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveyvitalstatistics.censoreddate IS 'First day of the month of the last refresh of the data.';