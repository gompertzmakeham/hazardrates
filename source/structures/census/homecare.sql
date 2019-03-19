CREATE MATERIALIZED VIEW censushomecare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest home care events
	eventdata AS
	(
		SELECT
			a0.uli_ab_phn uliabphn,
			a0.former_region providerregion,
			a0.staff_id provideridentifier,
			TRUNC(a0.open_from_date) visitdate
		FROM
			TABLE(continuing_care.home_care.get_activity) a0
		WHERE
			a0.home_care_affiliation = 'HOME CARE SERVICES'
			AND
			a0.care_management_affiliation = 'CARE MANAGEMENT'
			AND
			a0.provider_affiliation = 'STAFF PROVIDED SERVICES'
			AND
			a0.occupation_affiliation IN
			(
				'REGISTERED NURSE',
				'LICENSED PRACTICAL NURSE',
				'REGISTERED PSYCHIATRIC NURSE',
				'NURSE PRACTITIONER',
				'OCCUPATIONAL THERAPIST',
				'PHYSIOTHERAPIST',
				'SPEECH LANGUAGE PATHOLOGIST',
				'RESPIRATORY THERAPIST',
				'RECREATION THERAPIST',
				'SOCIAL WORKER',
				'REGISTERED DIETITIAN/NUTRITIONIST',
				'PHARMACIST',
				'OTHER REGULATED HEALTH PROFESSIONAL'
			)
			AND
			a0.activity_affiliation IN
			(
				'ALBERTA AIDS TO DAILY LIVING (AADL) PROGRAM',
				'CASE MANAGEMENT SERVICES',
				'FORMAL ASSESSMENT',
				'PROFESSIONAL HEALTH SERVICES'
			)
			AND
			a0.activity_task_duration > 0
			AND
			a0.visit_days > 0
			AND
			a0.close_to_date BETWEEN a0.open_from_date AND TRUNC(SYSDATE, 'MM')
			AND
			COALESCE(a0.birth_date, a0.open_from_date) <= a0.close_to_date
	),

	-- Digest to one record per patient per provider per day
	providerdata AS
	(
		SELECT
			a0.uliabphn,
			a0.providerregion,
			a0.provideridentifier,
			a0.visitdate,
			COUNT(*) allactivities
		FROM
			eventdata a0
		GROUP BY
			a0.uliabphn,
			a0.providerregion,
			a0.provideridentifier,
			a0.visitdate
	),

	-- Digest to one record per patient per day
	daydata AS
	(
		SELECT
			a0.uliabphn,
			a0.visitdate,
			SUM(a0.allactivities) allactivities,
			COUNT(*) allproviders
		FROM
			providerdata a0
		GROUP BY
			a0.uliabphn,
			a0.visitdate
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a1.allactivities) AS INTEGER) allactivities,
	CAST(SUM(a1.allproviders) AS INTEGER) allproviderdays,
	CAST(COUNT(*) AS INTEGER) alldays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a1.visitdate BETWEEN a0.extremumstart AND a0.extremumend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.visitdate, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censushomecare IS 'Utilization of home care registered or regulated professional services.';
COMMENT ON COLUMN censushomecare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censushomecare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censushomecare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censushomecare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censushomecare.allactivities IS 'Number of registered or regulated professional home care activities in the census interval.';
COMMENT ON COLUMN censushomecare.allproviderdays IS 'Number of unique combinations of registered or regulated professional home care providers and unique days in the census interval when the person utilized home care registered or regulated professional services.';
COMMENT ON COLUMN censushomecare.alldays IS 'Number of unique days in the census interval when the person visited utilized home care registered or regulated professional services.';