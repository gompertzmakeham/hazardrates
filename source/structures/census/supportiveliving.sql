CREATE MATERIALIZED VIEW censussupportiveliving NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all supportive living events
	eventdata AS
	(
		SELECT
		
			/*+ cardinality(a2, 1) */
			a0.uliabphn,
			a0.cornercase,
			a2.intervalstart,
			a2.intervalend,
			CASE
				WHEN a1.delivery_setting_affiliation <> 'NON-DESIGNATED SUPPORTIVE LIVING' AND greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1 + least(a0.extremumend, a2.durationend) - greatest(a0.extremumstart, a2.durationstart)
				ELSE
					0
			END designateddays,
			CASE
				WHEN a1.delivery_setting_affiliation <> 'NON-DESIGNATED SUPPORTIVE LIVING' AND a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END designatedadmissions,
			CASE
				WHEN a1.delivery_setting_affiliation <> 'NON-DESIGNATED SUPPORTIVE LIVING' AND a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END designateddischarges,
			CASE
				WHEN a1.delivery_setting_affiliation <> 'NON-DESIGNATED SUPPORTIVE LIVING' AND greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END designatedstays,
			CASE
				WHEN a1.delivery_setting_affiliation = 'NON-DESIGNATED SUPPORTIVE LIVING' AND greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1 + least(a0.extremumend, a2.durationend) - greatest(a0.extremumstart, a2.durationstart)
				ELSE
					0
			END nondesignateddays,
			CASE
				WHEN a1.delivery_setting_affiliation = 'NON-DESIGNATED SUPPORTIVE LIVING' AND a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END nondesignatedadmissions,
			CASE
				WHEN a1.delivery_setting_affiliation = 'NON-DESIGNATED SUPPORTIVE LIVING' AND a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END nondesignateddischarges,
			CASE
				WHEN a1.delivery_setting_affiliation = 'NON-DESIGNATED SUPPORTIVE LIVING' AND greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END nondesignatedstays,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1 + least(a0.extremumend, a2.durationend) - greatest(a0.extremumstart, a2.durationstart)
				ELSE
					0
			END staydays,
			CASE
				WHEN a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END admissioncount,
			CASE
				WHEN a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END dischargecount,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END intersectingstays
		FROM
			personsurveillance a0
			INNER JOIN
			TABLE(continuing_care.home_care.get_residency) a1
			ON
				a0.uliabphn = a1.uli_ab_phn
				AND
				a1.delivery_setting_affiliation IN 
				(
					'NON-DESIGNATED SUPPORTIVE LIVING',
					'SUPPORTIVE LIVING LEVEL 3',
					'SUPPORTIVE LIVING LEVEL 4',
					'SUPPORTIVE LIVING LEVEL 4 DEMENTIA')
				AND
				COALESCE(a1.exit_to_date, TRUNC(SYSDATE, 'MM')) BETWEEN a1.entry_from_date AND TRUNC(SYSDATE, 'MM')
				AND
				COALESCE(a1.birth_date, a1.entry_from_date) <= COALESCE(a1.exit_to_date, TRUNC(SYSDATE, 'MM'))
			CROSS JOIN
			TABLE
			(
				hazardutilities.generatecensus
				(
					a1.entry_from_date,
					COALESCE(a1.exit_to_date, TRUNC(SYSDATE, 'MM')),
					a0.birthdate
				)
			) a2
	)
	
-- Digest to one record per person per census interval partitioning the surveillance span
SELECT
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a0.intervalstart AS DATE) intervalstart,
	CAST(a0.intervalend AS DATE) intervalend,
	CAST(SUM(a0.designateddays) AS INTEGER) designateddays,
	CAST(SUM(a0.designatedadmissions) AS INTEGER) designatedadmissions,
	CAST(SUM(a0.designateddischarges) AS INTEGER) designateddischarges,
	CAST(SUM(a0.designatedstays) AS INTEGER) designatedstays,
	CAST(SUM(a0.nondesignateddays) AS INTEGER) nondesignateddays,
	CAST(SUM(a0.nondesignatedadmissions) AS INTEGER) nondesignatedadmissions,
	CAST(SUM(a0.nondesignateddischarges) AS INTEGER) nondesignateddischarges,
	CAST(SUM(a0.nondesignatedstays) AS INTEGER) nondesignatedstays,
	CAST(SUM(a0.staydays) AS INTEGER) staydays,
	CAST(SUM(a0.admissioncount) AS INTEGER) admissioncount,
	CAST(SUM(a0.dischargecount) AS INTEGER) dischargecount,
	CAST(SUM(a0.intersectingstays) AS INTEGER) intersectingstays
FROM
	eventdata a0
WHERE
	a0.staydays > 0
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a0.intervalstart,
	a0.intervalend;

COMMENT ON MATERIALIZED VIEW censussupportiveliving IS 'Utilization of designated supportive living levels 3, 4, and 4 dementia in census intervals of each person.';
COMMENT ON COLUMN censussupportiveliving.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censussupportiveliving.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censussupportiveliving.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censussupportiveliving.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censussupportiveliving.designateddays IS 'Naive sum of designated supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censussupportiveliving.designatedadmissions IS 'Designated supportive living admissions in the census interval.';
COMMENT ON COLUMN censussupportiveliving.designateddischarges IS 'Designated supportive living discharges in the census interval.';
COMMENT ON COLUMN censussupportiveliving.designatedstays IS 'Designated supportive living stays intersecting with the census interval.';
COMMENT ON COLUMN censussupportiveliving.nondesignateddays IS 'Naive sum of non-ddesignated supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censussupportiveliving.nondesignatedadmissions IS 'Non-designated supportive living admissions in the census interval.';
COMMENT ON COLUMN censussupportiveliving.nondesignateddischarges IS 'Non-designated supportive living discharges in the census interval.';
COMMENT ON COLUMN censussupportiveliving.nondesignatedstays IS 'Non-designated supportive living stays intersecting with the census interval.';
COMMENT ON COLUMN censussupportiveliving.staydays IS 'Naive sum of supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censussupportiveliving.admissioncount IS 'Supportive living admissions in the census interval.';
COMMENT ON COLUMN censussupportiveliving.dischargecount IS 'Supportive living discharges in the census interval.';
COMMENT ON COLUMN censussupportiveliving.intersectingstays IS 'Supportive living stays intersecting with the census interval.';