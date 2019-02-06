-- Date utilities
SELECT 'hazardutilities.cleandate(''abcd'', ''YYYYMMDD'')' functioncall, hazardutilities.cleandate('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleandate(''20181121'', ''abcd'')' functioncall, hazardutilities.cleandate('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleandate(''20181121'', ''YYYYMMDD'')' functioncall, hazardutilities.cleandate('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleandate(''abcd'')' functioncall, hazardutilities.cleandate('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleandate(''20181121'')' functioncall, hazardutilities.cleandate('20181121') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, hazardutilities.fiscalstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, hazardutilities.fiscalend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(''abcd'', ''YYYYMMDD'')' functioncall, hazardutilities.fiscalstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(''20181121'', ''abcd'')' functioncall, hazardutilities.fiscalstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(''20181121'', ''YYYYMMDD'')' functioncall, hazardutilities.fiscalstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(''abcd'')' functioncall, hazardutilities.fiscalstart('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalstart(''20181121'')' functioncall, hazardutilities.fiscalstart('20181121') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(''abcd'', ''YYYYMMDD'')' functioncall, hazardutilities.fiscalend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(''20181121'', ''abcd'')' functioncall, hazardutilities.fiscalend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(''20181121'', ''YYYYMMDD'')' functioncall, hazardutilities.fiscalend('20181121', 'YYYYMMDD') camllresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(''abcd'')' functioncall, hazardutilities.fiscalend('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.fiscalend(''20181121'')' functioncall, hazardutilities.fiscalend('20181121') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, hazardutilities.calendarstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, hazardutilities.calendarend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(''abcd'', ''YYYYMMDD'')' functioncall, hazardutilities.calendarstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(''20181121'', ''abcd'')' functioncall, hazardutilities.calendarstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(''20181121'', ''YYYYMMDD'')' functioncall, hazardutilities.calendarstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(''abcd'')' functioncall, hazardutilities.calendarstart('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarstart(''20181121'')' functioncall, hazardutilities.calendarstart('20181121') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(''abcd'', ''YYYYMMDD'')' functioncall, hazardutilities.calendarend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(''20181121'', ''abcd'')' functioncall, hazardutilities.calendarend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(''20181121'', ''YYYYMMDD'')' functioncall, hazardutilities.calendarend('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(''abcd'')' functioncall, hazardutilities.calendarend('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.calendarend(''20181121'')' functioncall, hazardutilities.calendarend('20181121') callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TRUNC(SYSDATE), SYSDATE)' functioncall, hazardutilities.yearanniversary(TRUNC(SYSDATE), SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TRUNC(SYSDATE), SYSDATE + 1)' functioncall, hazardutilities.yearanniversary(TRUNC(SYSDATE), SYSDATE + 1) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TRUNC(SYSDATE) - 1000, SYSDATE)' functioncall, hazardutilities.yearanniversary(TRUNC(SYSDATE) - 1000, SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearanniversary(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20050301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearanniversary(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20050301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TRUNC(SYSDATE))' functioncall, hazardutilities.yearend(TRUNC(SYSDATE)) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20000229'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20000229', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20000228'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20000228', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20010228'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20010228', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20030228'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20030228', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'hazardutilities.yearend(TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, hazardutilities.yearend(TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual;

-- Number utilities
SELECT 'hazardutilities.ageyears(add_months(SYSDATE, -360), SYSDATE)' functioncall, hazardutilities.ageyears(add_months(SYSDATE, -360), SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.ageyears(add_months(SYSDATE, -360) + 23, SYSDATE)' functioncall, hazardutilities.ageyears(add_months(SYSDATE, -360) + 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.ageyears(add_months(SYSDATE, -360) - 23, SYSDATE)' functioncall, hazardutilities.ageyears(add_months(SYSDATE, -360) - 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.ageyears(add_months(SYSDATE, -12) + 23, SYSDATE)' functioncall, hazardutilities.ageyears(add_months(SYSDATE, -12) + 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.ageyears(add_months(SYSDATE, -12), SYSDATE)' functioncall, hazardutilities.ageyears(add_months(SYSDATE, -12), SYSDATE) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninteger(''987654321'')' functioncall, hazardutilities.cleaninteger('987654321') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninteger(''abcd'')' functioncall, hazardutilities.cleaninteger('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninteger(''12345a67890'')' functioncall, hazardutilities.cleaninteger('12345a67890') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(2)' functioncall, hazardutilities.cleanphn(2) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(1000000002)' functioncall, hazardutilities.cleanphn(1000000002) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(987654321)' functioncall, hazardutilities.cleanphn(987654321) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(''abcd'')' functioncall, hazardutilities.cleanphn('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(''12345a67890'')' functioncall, hazardutilities.cleanphn('12345a67890') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanphn(''a123b456c789'')' functioncall, hazardutilities.cleanphn('a123b456c789d') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninpatient(''abcd'')' functioncall, hazardutilities.cleaninpatient('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninpatient(''87000'')' functioncall, hazardutilities.cleaninpatient('87000') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninpatient(''80-700'')' functioncall, hazardutilities.cleaninpatient('80-700') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleaninpatient(''80700'')' functioncall, hazardutilities.cleaninpatient('80700') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanambulatory(''abcd'')' functioncall, hazardutilities.cleanambulatory('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanambulatory(''87000'')' functioncall, hazardutilities.cleanambulatory('87000') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanambulatory(''88-700'')' functioncall, hazardutilities.cleanambulatory('88-700') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanambulatory(''88700'')' functioncall, hazardutilities.cleanambulatory('88700') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(2)' functioncall, hazardutilities.cleanprid(2) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(1000000002)' functioncall, hazardutilities.cleanprid(1000000002) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(987654321)' functioncall, hazardutilities.cleanprid(987654321) callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(''abcd'')' functioncall, hazardutilities.cleanprid('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(''12345a67890'')' functioncall, hazardutilities.cleanprid('12345a67890') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleanprid(''a123b456c789'')' functioncall, hazardutilities.cleanprid('a123b456c789d') callresult FROM dual;

-- String utilities
SELECT 'hazardutilities.cleansex(''abcd'')' functioncall, hazardutilities.cleansex('abcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleansex(''abmcd'')' functioncall, hazardutilities.cleansex('abmcd') callresult FROM dual UNION ALL
SELECT 'hazardutilities.cleansex(''abfmcd'')' functioncall, hazardutilities.cleansex('abfmcd') callresult FROM dual;

-- One day event census generation
WITH
	testdata AS
	(
		SELECT hazardutilities.fiscalstart(SYSDATE) -275 birthdate, hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(hazardutilities.fiscalstart(SYSDATE) - 275, -360) birthdate, hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(hazardutilities.fiscalstart(SYSDATE), -360) birthdate, hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(hazardutilities.fiscalstart(SYSDATE), -360) - 315 birthdate, hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(hazardutilities.fiscalstart(SYSDATE), -360) - 182 birthdate, hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual
	)
SELECT
	a0.*,
	a1.*
FROM
	testdata a0
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(eventdate, birthdate)) a1;
	
-- To do: Interval event census generation