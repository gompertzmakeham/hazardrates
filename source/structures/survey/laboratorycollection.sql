CREATE MATERIALIZED VIEW surveylaboratorycollection NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest laboratory collections, surveillance refresh is weekly
	eventdata AS
	(

		-- Laboratory Fusion
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gender) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.collect_dt leastservice,
			a0.collect_dt greatestservice,

			-- Week boundaries of least service
			hazardutilities.weekstart(a0.collect_dt) leastsurveillancestart,
			hazardutilities.weekend(a0.collect_dt) leastsurveillanceend,

			-- Week boundaries of greatest service
			hazardutilities.weekstart(a0.collect_dt) greatestsurveillancestart,
			hazardutilities.weekend(a0.collect_dt) greatestsurveillanceend,

			-- Coverage can be asserted by not refuted
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.collect_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrconform.cf_lab_labfusion a0
		WHERE
			hazardutilities.cleanphn(a0.clnt_phn) IS NOT NULL
			AND
			(substr(a0.clnt_bill_id, 1, 2) = 'AB' OR substr(a0.clnt_bill_id, 1, 1) = '0')
			AND
			a0.collect_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.collect_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL
		
		-- Laboratory Meditech
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt leastservice,
			a0.clct_dt greatestservice,

			-- Week boundaries of least service
			hazardutilities.weekstart(a0.clct_dt) leastsurveillancestart,
			hazardutilities.weekend(a0.clct_dt) leastsurveillanceend,

			-- Week boundaries of greatest service
			hazardutilities.weekstart(a0.clct_dt) greatestsurveillancestart,
			hazardutilities.weekend(a0.clct_dt) greatestsurveillanceend,

			-- Coverage can be asserted by not refuted
			1 albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrconform.lab_mt a0
		WHERE
			hazardutilities.cleanphn(a0.clnt_phn) IS NOT NULL
			AND
			substr(a0.clnt_bill_cd, 1, 3) = 'AHC'
			AND
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Laboratory Millenium
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gender) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.collect_dt leastservice,
			a0.collect_dt greatestservice,

			-- Week boundaries of least service
			hazardutilities.weekstart(a0.collect_dt) leastsurveillancestart,
			hazardutilities.weekend(a0.collect_dt) leastsurveillanceend,

			-- Week boundaries of greatest service
			hazardutilities.weekstart(a0.collect_dt) greatestsurveillancestart,
			hazardutilities.weekend(a0.collect_dt) greatestsurveillanceend,

			-- Coverage can be asserted by not refuted
			1 albertacoverage,
			CASE a0.clnt_bill_id
				WHEN 'ZINDIAN AFFAIRS' THEN
					1
				ELSE
					0
			END firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.collect_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrconform.cf_lab_millennium a0
		WHERE
			hazardutilities.cleanphn(a0.clnt_phn) IS NOT NULL
			AND
			a0.clnt_bill_id IN 
			(
				'AB PHN',
				'REFERRED IN SPECIMEN',
				'ZAADL',
				'ZBLUE CROSS',
				'ZINDIAN AFFAIRS',
				'ZLANDED IMMIGRANT',
				'ZPERSONAL HEALTH NUMBER',
				'ZSOCIAL SERVICES'
			)
			AND
			a0.collect_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.collect_dt) AND TRUNC(SYSDATE, 'MM')
		UNION ALL

		-- Laboratory Sunquest
		SELECT
			hazardutilities.cleanphn(a0.clnt_phn) uliabphn,
			hazardutilities.cleansex(a0.clnt_gndr) sex,
			a0.clnt_birth_dt birthdate,
			CAST(NULL AS DATE) deceaseddate,

			-- Service boundaries
			a0.clct_dt leastservice,
			a0.clct_dt greatestservice,

			-- Week boundaries of least service
			hazardutilities.weekstart(a0.clct_dt) leastsurveillancestart,
			hazardutilities.weekend(a0.clct_dt) leastsurveillanceend,

			-- Week boundaries of greatest service
			hazardutilities.weekstart(a0.clct_dt) greatestsurveillancestart,
			hazardutilities.weekend(a0.clct_dt) greatestsurveillanceend,

			-- Coverage unknown
			CAST(NULL AS INTEGER) albertacoverage,
			CAST(NULL AS INTEGER) firstnations,
			
			-- Birth observation
			CASE
				WHEN a0.clct_dt = a0.clnt_birth_dt THEN
					1
				ELSE
					0
			END surveillancebirth,
			CAST(NULL AS INTEGER) surveillancedeceased,
			CAST(NULL AS INTEGER) surveillanceimmigrate,
			CAST(NULL AS INTEGER) surveillanceemigrate
		FROM
			ahsdrrconform.lab_sq a0
		WHERE
			hazardutilities.cleanphn(a0.clnt_phn) IS NOT NULL
			AND
			a0.clct_dt BETWEEN COALESCE(a0.clnt_birth_dt, a0.clct_dt) AND TRUNC(SYSDATE, 'MM')
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

COMMENT ON MATERIALIZED VIEW surveylaboratorycollection IS 'Unique persons by provincial health number, from laboratory sample collections.';
COMMENT ON COLUMN surveylaboratorycollection.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN surveylaboratorycollection.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN surveylaboratorycollection.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.leastbirth IS 'Earliest recorded birth date.';
COMMENT ON COLUMN surveylaboratorycollection.greatestbirth IS 'Latest recorded birth date.';
COMMENT ON COLUMN surveylaboratorycollection.leastdeceased IS 'Earliest recorded deceased date.';
COMMENT ON COLUMN surveylaboratorycollection.greatestdeceased IS 'Latest recorded deceased date.';
COMMENT ON COLUMN surveylaboratorycollection.leastservice IS 'Earliest healthcare adminstrative record.';
COMMENT ON COLUMN surveylaboratorycollection.greatestservice IS 'Latest healthcare adminstrative record.';
COMMENT ON COLUMN surveylaboratorycollection.leastsurveillancestart IS 'Start date of the least observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.leastsurveillanceend IS 'End date of the least observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.greatestsurveillancestart IS 'Start date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.greatestsurveillanceend IS 'End date of the greatest observation bounds of the person.';
COMMENT ON COLUMN surveylaboratorycollection.surveillancebirth IS 'Birth observed in the surveillance interval: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillancedeceased IS 'Death observed in the surveillance: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.albertacoverage IS 'All the records inidcated coverage by Alberta Health Insurance: 1 yes, 0 no.';
COMMENT ON COLUMN surveylaboratorycollection.censoreddate IS 'First day of the month of the last refresh of the data.';