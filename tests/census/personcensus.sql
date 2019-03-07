WITH
	intervaldata AS
	(
		SELECT
			a0.uliabphn,
			a0.cornercase,
			a0.durationstart,
			a0.durationend,
			a0.intervalfirst,
			a0.intervallast,
			a0.intervalbirth,
			a0.intervaldeceased,
			a0.intervalimmigrate,
			a0.intervalemigrate,
			CASE a0.intervalorder
				WHEN 1 THEN
					1
				ELSE
					0
			END orderfirst,
			CASE a0.intervalorder
				WHEN a0.intervalcount THEN
					1
				ELSE
					0
			END orderlast
		FROM
			ab_hzrd_rts_anlys.personcensus a0
	)
SELECT
	a0.intervalfirst,
	a0.orderfirst,
	a0.intervalbirth,
	a0.intervalimmigrate,
	a0.intervallast,
	a0.orderlast,
	a0.intervaldeceased,
	a0.intervalemigrate,
	COUNT(*) intervals
FROM
	intervaldata a0
GROUP BY
	a0.intervalfirst,
	a0.orderfirst,
	a0.intervalbirth,
	a0.intervalimmigrate,
	a0.intervallast,
	a0.orderlast,
	a0.intervaldeceased,
	a0.intervalemigrate
ORDER BY
	1 DESC NULLS FIRST,
	2 DESC NULLS FIRST,
	3 DESC NULLS FIRST,
	4 DESC NULLS FIRST,
	5 DESC NULLS FIRST,
	6 DESC NULLS FIRST,
	7 DESC NULLS FIRST,
	8 DESC NULLS FIRST;
	
SELECT
	CASE LEAD(a0.durationstart, 1, 1 + a0.durationend) OVER (PARTITION BY a0.uliabphn, a0.cornercase ORDER BY a0.durationstart ASC NULLS FIRST)
		WHEN 1 + a0.durationend THEN
			1
		ELSE
			0
	END nextcontiguous,
	a0.*
FROM
	ab_hzrd_rts_anlys.personcensus a0
ORDER BY
	1 ASC NULLS FIRST;