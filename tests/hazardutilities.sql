-- Date utilities
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleandate(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleandate('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleandate(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleandate('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleandate(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleandate('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleandate(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleandate('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleandate(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleandate('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalstart(''19930331'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalstart('19930331') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('20181121', 'YYYYMMDD') camllresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.fiscalend(''19930331'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.fiscalend('19930331') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarstart(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarstart('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.calendarend(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.calendarend('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterstart(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterstart('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.quarterend(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.quarterend('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthstart(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthstart('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.monthend(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.monthend('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(TRUNC(SYSDATE, ''MM'') - 100)' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend(TRUNC(SYSDATE, 'MM') - 100) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekstart(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekstart('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(''abcd'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend('abcd', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(''20181121'', ''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend('20181121', 'abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(''20181121'', ''YYYYMMDD'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend('20181121', 'YYYYMMDD') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.weekend(''20181121'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.weekend('20181121') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE), SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE), SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE), SYSDATE + 1)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE), SYSDATE + 1) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE) - 1000, SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TRUNC(SYSDATE) - 1000, SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20050301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20050301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE), SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE), SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE), SYSDATE + 1)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE), SYSDATE + 1) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE) - 1000, SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TRUNC(SYSDATE) - 1000, SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20000301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20000301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20010301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20010301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20020301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20020301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20000229'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20000229', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20030301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20030301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20040301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20040301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''20030228'', ''YYYYMMDD''), TO_DATE(''20050301'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('20030228', 'YYYYMMDD'), TO_DATE('20050301', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE(''19930228'', ''YYYYMMDD''), TO_DATE(''20010331'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversarystart(TO_DATE('19930228', 'YYYYMMDD'), TO_DATE('20010331', 'YYYYMMDD')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE(''19930228'', ''YYYYMMDD''), TO_DATE(''20010331'', ''YYYYMMDD''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.anniversaryend(TO_DATE('19930228', 'YYYYMMDD'), TO_DATE('20010331', 'YYYYMMDD')) callresult FROM dual;

-- Number utilities
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800601'', ''yyyymmdd''), TO_DATE(''19850615'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800601', 'yyyymmdd'), TO_DATE('19850615', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800614'', ''yyyymmdd''), TO_DATE(''19850615'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800614', 'yyyymmdd'), TO_DATE('19850615', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800615'', ''yyyymmdd''), TO_DATE(''19850615'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800615', 'yyyymmdd'), TO_DATE('19850615', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800616'', ''yyyymmdd''), TO_DATE(''19850615'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800616', 'yyyymmdd'), TO_DATE('19850615', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800630'', ''yyyymmdd''), TO_DATE(''19850615'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800630', 'yyyymmdd'), TO_DATE('19850615', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790227'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790227', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790228'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790228', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790301'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790301', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790227'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790227', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790228'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790228', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790301'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790301', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790227'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790227', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790228'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790228', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19790301'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19790301', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800228'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800228', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800229'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800229', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800301'', ''yyyymmdd''), TO_DATE(''19840228'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800301', 'yyyymmdd'), TO_DATE('19840228', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800228'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800228', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800229'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800229', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800301'', ''yyyymmdd''), TO_DATE(''19840229'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800301', 'yyyymmdd'), TO_DATE('19840229', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800228'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800228', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800229'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800229', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE(''19800301'', ''yyyymmdd''), TO_DATE(''19840301'', ''yyyymmdd''))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(TO_DATE('19800301', 'yyyymmdd'), TO_DATE('19840301', 'yyyymmdd')) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360), SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360), SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360) + 23, SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360) + 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360) - 23, SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -360) - 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -12) + 23, SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -12) + 23, SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -12), SYSDATE)' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(add_months(SYSDATE, -12), SYSDATE) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.ageyears(SYSDATE, add_months(SYSDATE, -36))' functioncall, ab_hzrd_rts_anlys.hazardutilities.ageyears(SYSDATE, add_months(SYSDATE, -36)) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninteger(''987654321'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninteger('987654321') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninteger(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninteger('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninteger(''12345a67890'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninteger('12345a67890') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(2)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn(2) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(1000000002)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn(1000000002) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(987654321)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn(987654321) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(''12345a67890'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn('12345a67890') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanphn(''a123b456c789'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanphn('a123b456c789d') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninpatient('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(''87000'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninpatient('87000') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(''80-700'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninpatient('80-700') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleaninpatient(''80700'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleaninpatient('80700') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanambulatory(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanambulatory('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanambulatory(''87000'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanambulatory('87000') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanambulatory(''88-700'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanambulatory('88-700') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanambulatory(''88700'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanambulatory('88700') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(2)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid(2) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(1000000002)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid(1000000002) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(987654321)' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid(987654321) callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(''12345a67890'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid('12345a67890') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleanprid(''a123b456c789'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleanprid('a123b456c789d') callresult FROM dual;

-- String utilities
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleansex(''abcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleansex('abcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleansex(''abmcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleansex('abmcd') callresult FROM dual UNION ALL
SELECT 'ab_hzrd_rts_anlys.hazardutilities.cleansex(''abfmcd'')' functioncall, ab_hzrd_rts_anlys.hazardutilities.cleansex('abfmcd') callresult FROM dual;

-- One day event census generation
WITH
	testdata AS
	(
		SELECT ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) -275 birthdate, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275, -360) birthdate, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE), -360) birthdate, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE), -360) - 315 birthdate, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT add_months(ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE), -360) - 182 birthdate, ab_hzrd_rts_anlys.hazardutilities.fiscalstart(SYSDATE) - 275 eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800229', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800228', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19800301', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810228', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19810301', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790228', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20030228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20030301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20040228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20040229', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20040301', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20050228', 'yyyymmdd') eventdate FROM dual UNION ALL
		SELECT TO_DATE('19790301', 'yyyymmdd') birthdate, TO_DATE('20050301', 'yyyymmdd') eventdate FROM dual
	)
SELECT
	a0.*,
	a1.*
FROM
	testdata a0
	CROSS JOIN
	TABLE(ab_hzrd_rts_anlys.hazardutilities.generatecensus(a0.eventdate, a0.birthdate)) a1;
	
-- A few conditions taken from problematic cases
WITH
	testdata AS
	(
		SELECT TO_DATE('20080228', 'yyyymmdd') eventstart, TO_DATE('20180331', 'yyyymmdd') eventend, TO_DATE('19920229', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('20070401', 'yyyymmdd') eventstart, TO_DATE('20180331', 'yyyymmdd') eventend, TO_DATE('19920229', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('20070401', 'yyyymmdd') eventstart, TO_DATE('20180331', 'yyyymmdd') eventend, TO_DATE('20070401', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('19990331', 'yyyymmdd') eventstart, TO_DATE('20190101', 'yyyymmdd') eventend, TO_DATE('19660401', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('19980401', 'yyyymmdd') eventstart, TO_DATE('20190101', 'yyyymmdd') eventend, TO_DATE('19660401', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('20091121', 'yyyymmdd') eventstart, TO_DATE('20181110', 'yyyymmdd') eventend, TO_DATE('20091121', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('19930401', 'yyyymmdd') eventstart, TO_DATE('20180519', 'yyyymmdd') eventend, TO_DATE('19691130', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('19940331', 'yyyymmdd') eventstart, TO_DATE('20190126', 'yyyymmdd') eventend, TO_DATE('19560924', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('19930401', 'yyyymmdd') eventstart, TO_DATE('20190126', 'yyyymmdd') eventend, TO_DATE('19570925', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('20170720', 'yyyymmdd') eventstart, TO_DATE('20170817', 'yyyymmdd') eventend, TO_DATE('19370315', 'yyyymmdd') birthdate FROM dual UNION ALL
		SELECT TO_DATE('20170720', 'yyyymmdd') eventstart, TO_DATE('20170817', 'yyyymmdd') eventend, TO_DATE('19370312', 'yyyymmdd') birthdate FROM dual
	)
SELECT
	a0.*,
	a1.*
FROM
	testdata a0
	CROSS JOIN
	TABLE(ab_hzrd_rts_anlys.hazardutilities.generatecensus(a0.eventstart, a0.eventend, a0.birthdate)) a1;