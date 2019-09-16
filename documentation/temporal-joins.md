Relational Database Temporal Joins in Three Pieces
==================================================

*Aaron Sheldon, 2018*

*For Kelly Blacklaws and Tania Dosenberger, whom I should have gotten back to much sooner.*

*Many thanks to Edwin Rogers for the conversations, motivating examples, and review.*

0: Introduction
---------------
The primary problem in the longitudinal analysis of large samples of event intervals is the
determination of the intersections of combinations of the event intervals in time, and the
bounds of those intersections. In the health system event interval records are commonly
stored in relational databases. While relational databases are intrinsically designed to
facilitate the structured querying of data in a consistent and integral manner they do not
natively support temporal querying of data, or sequential time series processing.

The tabular, indexing, and referential constraints of relational databases force health
system interval data to be structured as loosely coupled event stores, where a common
identifier for each patient exists across all tables, but no keys enforcing temporal
referential relationships between records of events exist. As such intersections of event
intervals must be deduced from the time boundaries of the events. Writing efficient and
consistent temporal queries takes some care and forethought, because naive approaches lead
to testing an exponential number of possible intersections, and case by case ad hoc 
formulations lead to results that have difficult to interpret temporal relationships.

Relational databases are principally designed to sequentially process records independently.
In contrast temporal joins are inherently self referential, as they query the temporal
relationships between records. While naive methods of constructing temporal joins will
result in exponential recursion, the intrinsic ordering of time can be exploited to
construct first order queries through the use of sorting and extremum aggregates. The chief
complicating factor is that every event interval contains two time points, that do not
necessarily sort records in the same order. Specifically, sorting the event interval records
by their lower bounds will not necessarily order the records the same as sorting by their
upper bounds.

Figure 1. Sorting event intervals by their lower bounds.
```
e1: |--------|
e2:    |------------|
e3:       |-----|
e4:                          |---------------------------|
e5:                             |--------------|
e6:                                                           |-----------|
e7:                                                                 |---------|
```

Figure 2. Sorting the same event intervals by their upper bounds.
```
e1: |--------|
e3:       |-----|
e2:    |------------|
e5:                             |--------------|
e4:                          |---------------------------|
e6:                                                           |-----------|
e7:                                                                 |---------|
```

The health system is composed of many processes which can generate event intervals. The
generating processes broadly fall into four categories:

1. Explicitly bounded utilizations of services, such as stays in hospitals or residential
care sites, administrative registrations and enrollments in panels.
2. Single point in time events, such as primary care visits, assessments, and specific
interventions.
3. An interval relative to an event, such as two weeks following discharge, or the month
before a visit for a therapeutic intervention.
4. Lists of census intervals, such as each quarter from the fiscal year 2012 to the fiscal
year 2018.

In the context of the health system, temporal joins search for intersections of events in
any of the four categories (simultaneous, coincident, overlapping, or co-occurring).
Examples of the type of queries that result in temporal joins include:

1. Primary care visits that occurred within one month of an opioid dispensing, and
report by the quarter of the dispensing date.
2. Laboratory results that occurred while registered in home care.
3. Hospital stays that occurred while residing in a long term care facility.

While it may seem relatively transparent what each of these examples is attempting to
detect, the complexity quickly grows when faced with the nuances of the operation of the
health system. Even for hospital stays, patients may have multiple hospital stays where the
records intersect non-trivially in time.

We will develop a methodology to generate temporal joins specifically for events with start
and end dates. To apply temporal joins to recurring events we have to materialize the events
into their individual occurrences, and then process the temporal joins. The goal of a
temporal join is to produce records that allow us to assert relational statements about
moments in time, a single record at a time, without the need for further joins. That is,
after computing a temporal join we should not need to compute any more temporal joins of the
same data to determine temporal relationships.

If the result of a temporal join was a set of records that intersected in time non-trivial
then we would be no better off then having not computed the temporal join. Thus, the result
of a temporal join should be a set of records such that any pair of records are either
disjoint in time (do not intersect) or intersect trivial (they have exactly equal start and
end times). This condition is equivalent to asserting that the result of a temporal join
should partition time into disjoint intervals, or equivalently that each interval is
uniquely identified by its start date.

Before we develop the theory behind temporal joins we need to introduce four interrelated 
mathematical concepts that will be heavily used in reasoning about temporal joins,
specifically, bounds, intervals, partitions, and their refinements. Briefly, an interval is
the period of time between an upper and lower bound; a partition of time is a set of
disjoint intervals (non-intersecting) whose union is the timeline, finally a refinement of
a partition is another partition where every interval in the second partition is a proper
subset of some interval in the first partition, that is a refinement provides greater
detail.

Figure 3. Mathematical concepts.
```
Time        : ------------------------------
Lower Bound :       |- >
Upper Bound :              < -|
An Interval :       |---------|
A Partition : |-----|---------|----|-------|
A Refinement: |--|--|---------|----|--|--|-|
```

To temporally join event interval records mathematically consistently we will construct a
partition by refining time by the individual event interval records. To complete the
temporal join we then assign information to each interval in the constructed partition. This
strategy becomes computationally efficient by recognizing that a partition is uniquely
defined by the boundary points of the intervals, because the upper boundary of the earlier
interval is shared with the lower boundary of the later interval. We will repeatedly use
this observation to find intersections by generating and sorting boundary points from
individual event interval records. To summarize, to temporally join events mathematically
consistently and computationally efficiently we work with the boundaries between intervals
rather than the intervals directly.

Now that we have the underlying mathematical concepts outlined lets take a look at how we
determine if two events intersect. To answer this question, in generality, we consider the 
contra-positive condition that two events do not intersect, that is they are disjoint. We
observe that for disjoint events, one event must always end before the other begins.

Figure 4. Disjoint events.
```
e1: |start-------------end|
e2:                              |start------------end|
```

We assert intersection by negating the disjoint condition, and then simplifying the negation
using the distributive property of negation, and finally generalizing using an implicit
comparison.

Example 1. Disjoint test.
```SQL
e1.start > e2.end OR e1.end < e2.start
```

Example 2. Negation of disjoint test.
```SQL
NOT (e1.start > e2.end OR e2.start > e2.end) 
```

Example 3. Intersection conjunction test.
```SQL
e1.start <= e2.end AND e2.start <= e1.end
```

Example 4. General intersection test.
```SQL
greatest(e1.start, e2.start, e3.start,...) <= least(e1.end, e2.end, e3.end,...)
```

Using the Continuing Care Application we can demonstrate the time intersection test with a
self intersection query that, for each patient, will find all pairs of overlapping stays in
supportive living. We start by selecting from the residency data twice.

Example 5. Join snippet.
```SQL
TABLE(continuing_care.home_care.get_residency) a0
INNER JOIN
TABLE(continuing_care.home_care.get_residency) a1
```

In the join clause we will require that these are only supportive living stays.

Example 6. Filter the delivery setting.
```SQL
a0.delivery_setting_affiliation IN
(
  'SUPPORTIVE LIVING LEVEL 3',
  'SUPPORTIVE LIVING LEVEL 4',
  'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
)
AND
a1.delivery_setting_affiliation IN
(
  'SUPPORTIVE LIVING LEVEL 3',
  'SUPPORTIVE LIVING LEVEL 4',
  'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
)
```

We have to link the patients stays with themselves, starting with the patient

Example 7. Patient linking.
```SQL
a0.uli_ab_phn = a1.uli_ab_phn
```

We also require the stays intersect by more than one day, thus we replace the greater than
or equals test with the strictly greater than test, note the symmetry of the table aliases.

Example 8. Intersection test.
```SQL
greatest(a0.entry_from_date, a1.entry_from_date) < least(a0.exit_to_date, a1.exit_to_date)
```

This will however report each pair of matches twice, so we will return only ordered pairs of
matches.

Example 9. Ordered pair test.
```SQL
a0.entry_from_date < a1.entry_from_date
OR
a0.stay_residence_id < a1.stay_residence_id
```

Putting these conditions together, along with the appropriate columns yields the final
query.

Example 10. Stays overlapping by more than a day.
```SQL
SELECT
  COUNT(*) OVER (PARTITION BY a0.uli_ab_phn) event_count,
  a0.uli_ab_phn,
  a0.former_region early_region,
  a0.information_system early_system,
  a0.client_id early_client,
  a0.stay_residence_id early_id,
  a0.delivery_setting_affiliation early_name,
  a0.facility_id early_code,
  a0.entry_from_date early_start,
  a0.exit_to_date early_end,
  a1.former_region later_region,
  a1.information_system later_system,
  a1.client_id later_client,
  a1.stay_residence_id later_id,
  a1.delivery_setting_affiliation later_name,
  a1.facility_id later_code,
  a1.entry_from_date later_start,
  a1.exit_to_date later_end
FROM
  TABLE(continuing_care.home_care.get_residency) a0
  INNER JOIN
  TABLE(continuing_care.home_care.get_residency) a1
  ON
    a0.delivery_setting_affiliation IN
    (
      'SUPPORTIVE LIVING LEVEL 3',
      'SUPPORTIVE LIVING LEVEL 4',
      'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
    )
    AND
    a1.delivery_setting_affiliation IN
    (
      'SUPPORTIVE LIVING LEVEL 3',
      'SUPPORTIVE LIVING LEVEL 4',
      'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
    )
    AND
    a0.uli_ab_phn = a1.uli_ab_phn
    AND
    (
      a0.entry_from_date < a1.entry_from_date
      OR
      a0.stay_residence_id < a1.stay_residence_id
    )
    AND
    greatest(a0.entry_from_date, a1.entry_from_date) < 
    least(a0.exit_to_date, a1.exit_to_date)
ORDER BY
  1 DESC NULLS FIRST,
  2 ASC NULLS FIRST,
  9 ASC NULLS FIRST,
  17 ASC NULLS FIRST;
```

While this is a temporal query it does not meet the criteria, as outlined above, of being a
temporal join because the records produced will have non-trivial intersections with each
other, and thus do not generate a partition of time. Furthermore, finding all triples
requires three inner joins, quartets four inner joins, quintets five, and so on. This
recursive method of finding all pair, triples, quartets, etc... by successive inner joins is
neither efficient to write, efficient to compute, as it runs in exponential time, nor easy
to maintain, or easy to reason about and interpret.

Instead we will compute intersections using the boundaries of the intervals, which in the
worst case runs in quadratic time, and in the best case in log-linear time. In the following
three pieces we will describe three partitions of time, each a refinement of the later. The
coarsest partition divides time into intervals indicating the presence or absence of any
records; the next partition implements a heuristic to return one record per interval,
without any loss of coverage; the final partition divides time into all intervals
containing distinct combinations of events. In all of the following pieces we will make
heavy use of SQL analytic clauses to arbitrage start and end dates against each other to
find the boundaries of the disjoint intervals that compose the partition.

1: Existentially Qualified Partitions
-------------------------------------
Existentially qualified partitions divide time into intervals that indicate either the
presence or absence of any event interval records. Remarkably even in the case of long
chains of event interval records that just overlap we can use SQL analytic functions to find
the boundaries of the presence intervals.

Figure 5. Constructing an existentially qualified partition.
```
e1: |--------|
e2:    |------------|
e3:       |-----|
e4:                          |---------------------------|
e5:                             |--------------|
e6:                                                           |-----------|
e7:                                                                 |---------|
EQ: |---------------|--------|---------------------------|----|---------------|
ST: ^               ^        ^                           ^    ^
```

We begin by considering an event interval record whose lower bound coincides with the start
of the presence interval. In this case all the event interval records that started before
the given event interval record also ended before the given record started. In succinct
terms the maximum of the end dates of all records that started before the current record
must be strictly less than the start date. The critical logical tests in the analytic clause
ensure all null start dates are sorted first, and that end dates of all preceding start
dates up to the previous date are searched.

Example 11. Analytic test for the start of a presence interval.
```SQL
e.start > MAX(e.end) OVER 
(
  ORDER BY
    e.start ASC NULLS FIRST
  RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
)
```

Conversely, an event interval record that coincides with the end of the presence interval
ends before all the subsequent ending events start. Thus the minimum of the start dates of
all the records that ended after the current must be strictly greater than the end date.

Example 12. Analytic test for the end of a presence interval.
```SQL
e.end < MIN(e.start) OVER 
(
  ORDER BY
    e.end ASC NULLS LAST
  RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
)
```

However, when implementing existentially qualified partitions we test the contra-positive 
condition so that the extremes of first and last events are handled without any additional
tests. The contra-positive conditions states that a record is not at the beginning of
presence interval if the start of the record is in the interior of any other record, and is
not at the end of a presence interval if the end of the record is in the interior of any
other record. We construct existentially qualified partitions in four steps:

1. Collect all the events together into a single columnar list of events by union, so that
only unique pairs of start and end dates are listed.
2. Flag the records that either have a start date on the start of the presence interval
or an end date on the end of the presence interval, by testing the contra-positive
condition.
3. Pivot the start and end dates into a single columnar list of boundary dates.
4. Construct the intervals by using the next boundary date as the current end date, flagging
last trailing absence record.
5. Return all the constructed intervals, excluding the last trailing absence record.

Returning to the Continuing Care Application we will partition each patient's timeline into
intervals representing the presence or absence of at least one record of stay in a 
residential delivery site, combining supportive living and long term care stays.

In the first step we begin by consolidating the records of stays in supportive living and
the records of stays in long term care into a single columnar data set. We ensure that all
start dates are in a single column, and all end dates are in a separate single column,
regardless of the source.

Example 13. Consolidate residential site stays.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.admit_date event_start,
  a0.discharge_date event_end
FROM
  TABLE(continuing_care.accis.get_adt) a0
UNION
SELECT
  a0.uli_ab_phn,
  a0.entry_from_date event_start,
  a0.exit_to_date event_end
FROM
  TABLE(continuing_care.home_care.get_residency) a0
WHERE
  a0.delivery_setting_affiliation IN
  (
    'SUPPORTIVE LIVING LEVEL 3',
    'SUPPORTIVE LIVING LEVEL 4',
    'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
  )
  AND
  a0.uli_ab_phn IS NOT NULL
```

As mentioned earlier we flag the start of presence intervals by testing for start dates that
are interior to the presence interval. Interior start dates are less than or equal to the
end dates of all records that started before the start date of the record, where we assume
null end dates are in the future. Furthermore, we have to ensure that of the records that
started on the same date only the longest record is flagged as the beginning of the presence
interval.

Example 14. Test for the start of presence.
```SQL
CASE
  WHEN a0.event_start <= MAX(COALESCE(a0.event_end, SYSDATE)) OVER
    (
      PARTITION BY
        a0.uli_ab_phn
      ORDER BY
        a0.event_start ASC NULLS FIRST,
        a0.event_end DESC NULLS FIRST
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) THEN
    0
  ELSE
    1
END
```

Dual to the test for the beginning of the presence interval, we test for end dates that are
interior to the presence interval. Interior end dates are greater than or equal to the start
dates of all the records that ended after the end date of the record, where we assume null
end dates are in the future. Furthermore, we have to ensure that of the records that ended
on the same date only the longest record is flagged as the end of the presence interval.

Example 15. Test for the end of presence.
```SQL
CASE
  WHEN COALESCE(a0.event_end, SYSDATE) >= MIN(a0.event_start) OVER
    (
      PARTITION BY
        a0.uli_ab_phn
      ORDER BY
        a0.event_end ASC NULLS LAST,
        a0.event_start DESC NULLS LAST
      ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ) THEN
    0
  ELSE
    1
END
```

In the second step we incorporate the previous two tests into two columns that identify the
upper and lower boundaries of the presence interval. As well we preserve the boundaries of
the original event intervals. This step has to occur after the first because we want the
analytic functions to operate over all the event interval records, regardless of source.

Example 16. Flag the start and end of disjoint partition intervals.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_start,
  a0.event_end,
  CASE
    WHEN a0.event_start <= MAX(COALESCE(a0.event_end, SYSDATE)) OVER
      (
        PARTITION BY
          a0.uli_ab_phn
        ORDER BY
          a0.event_start ASC NULLS FIRST,
          a0.event_end DESC NULLS FIRST
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) THEN
      0
    ELSE
      1
  END is_start,
  CASE
    WHEN COALESCE(a0.event_end, SYSDATE) >= MIN(a0.event_start) OVER
      (
        PARTITION BY
          a0.uli_ab_phn
        ORDER BY
          a0.event_end ASC NULLS LAST,
          a0.event_start DESC NULLS LAST
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) THEN
      0
    ELSE
      1
  END is_end
FROM
  event_intervals a0
```

In the third step we pivot the start and end dates of the presence interval into a single
column, so that we can order the dates to produce a partition of the time line. We select
records that were the start of presence records. We introduce a field that indicates this is
a record of the presence of residential delivery site stay, and a second field that
indicates that this is not the final absence record.

Example 17. Pivot the start dates.
```SQL
SELECT
  a0.uli_ab_phn,
  'Residential Site Stay' event_type,
  a0.event_start,
  0 is_last
FROM
  identify_boundaries a0
WHERE
  a0.is_start = 1
```

We select records that were the end of presence records, as one plus that date is the start
of absence records. We introduce a field that indicates this is a record of the absence of
any residential delivery site stays, and a second field to flag the final absence record,
so that the record can be excluded in the final list of intervals.

Example 18. Pivot the end dates.
```SQL
SELECT
  a0.uli_ab_phn,
  'No Events' event_type,
  a0.event_end + 1 event_start,
  CASE COALESCE(a0.event_end, SYSDATE)
    WHEN MAX(COALESCE(a0.event_end, SYSDATE)) OVER (PARTITION BY a0.uli_ab_phn) THEN
      1
    ELSE
      0
  END is_last
FROM
  identify_boundaries a0
WHERE
  a0.is_end = 1
```

To complete the pivot of step three we combine the two queries together to create a single
list of interval boundaries that define the partition of time for each patient.

Example 19. Pivot the start and end dates.
```SQL
SELECT
  a0.uli_ab_phn,
  'Residential Site Stay' event_type,
  a0.event_start,
  0 is_last
FROM
  identify_boundaries a0
WHERE
  a0.is_start = 1
UNION ALL
SELECT
  a0.uli_ab_phn,
  'No Events' event_type,
  a0.event_end + 1 event_start,
  CASE COALESCE(a0.event_end, SYSDATE)
    WHEN MAX(COALESCE(a0.event_end, SYSDATE)) OVER (PARTITION BY a0.uli_ab_phn) THEN
      1
    ELSE
      0
  END is_last
FROM
  identify_boundaries a0
WHERE
  a0.is_end = 1
```

In the fourth step, we end date each record by retrieving the next start date and
subtracting one, assuming nulls are in the future.

Example 20. Look ahead to the next start.
```SQL
LEAD(a0.event_start - 1, 1, NULL) OVER
(
  PARTITION BY
    a0.uli_ab_phn
  ORDER BY
    a0.event_start ASC NULLS LAST
)
```

The computed end dates are included as a column in the list of partition intervals.

Example 21. Disjoint partition intervals.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_type,
  a0.event_start,
  LEAD(a0.event_start - 1, 1, NULL) OVER
  (
    PARTITION BY
      a0.uli_ab_phn
    ORDER BY
      a0.event_start ASC NULLS LAST
  ) event_end,
  a0.is_last
FROM
  partition_boundaries a0
```

In the fifth step we we select all the partition intervals except for the trailing absence
interval.

Example 22. Exclude the last trailing absence interval.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_type,
  a0.event_start,
  a0.event_end
FROM
  partition_intervals a0
WHERE
  a0.is_last = 0
```

We combine the five previous steps into a single common table expression that generates
existentially qualified partitions of time for each patient.

Example 23. Presence and absence of residential site stays.
```SQL
WITH
  event_intervals AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.admit_date event_start,
      a0.discharge_date event_end
    FROM
      TABLE(continuing_care.accis.get_adt) a0
    UNION
    SELECT
      a0.uli_ab_phn,
      a0.entry_from_date event_start,
      a0.exit_to_date event_end
    FROM
      TABLE(continuing_care.home_care.get_residency) a0
    WHERE
      a0.delivery_setting_affiliation IN
      (
        'SUPPORTIVE LIVING LEVEL 3',
        'SUPPORTIVE LIVING LEVEL 4',
        'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
      )
      AND
      a0.uli_ab_phn IS NOT NULL
  ),
  identify_boundaries AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.event_start,
      a0.event_end,
      CASE
        WHEN a0.event_start <= MAX(COALESCE(a0.event_end, SYSDATE)) OVER
          (
            PARTITION BY
              a0.uli_ab_phn
            ORDER BY
              a0.event_start ASC NULLS FIRST,
              a0.event_end DESC NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) THEN
          0
        ELSE
          1
      END is_start,
      CASE
        WHEN COALESCE(a0.event_end, SYSDATE) >= MIN(a0.event_start) OVER
          (
            PARTITION BY
              a0.uli_ab_phn
            ORDER BY
              a0.event_end ASC NULLS LAST,
              a0.event_start DESC NULLS LAST
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
          ) THEN
          0
        ELSE
          1
      END is_end
    FROM
      event_intervals a0
  ),
  partition_boundaries AS
  (
    SELECT
      a0.uli_ab_phn,
      'Residential Site Stay' event_type,
      a0.event_start,
      0 is_last
    FROM
      identify_boundaries a0
    WHERE
      a0.is_start = 1
    UNION ALL
    SELECT
      a0.uli_ab_phn,
      'No Events' event_type,
      a0.event_end + 1 event_start,
      CASE COALESCE(a0.event_end, SYSDATE)
        WHEN MAX(COALESCE(a0.event_end, SYSDATE)) OVER (PARTITION BY a0.uli_ab_phn) THEN
          1
        ELSE
          0
      END is_last
    FROM
      identify_boundaries a0
    WHERE
      a0.is_end = 1
  ),
  partition_intervals AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.event_type,
      a0.event_start,
      LEAD(a0.event_start - 1, 1, NULL) OVER
      (
        PARTITION BY
          a0.uli_ab_phn
        ORDER BY
          a0.event_start ASC NULLS LAST
      ) event_end,
      a0.is_last
    FROM
      partition_boundaries a0
  )
SELECT
  COUNT(*) OVER (PARTITION BY a0.uli_ab_phn) event_count,
  a0.uli_ab_phn,
  a0.event_type,
  a0.event_start,
  a0.event_end
FROM
  partition_intervals a0
WHERE
  a0.is_last = 0
ORDER BY
  1 DESC NULLS FIRST,
  2 ASC NULLS FIRST,
  4 ASC NULLS FIRST;
```

2: Tail Rectified Partitions
----------------------------
Tail rectified partitions are a refinement of existentially qualified partitions, and
implement a simple heuristic for choosing a single event interval record for each disjoint
interval in the partition. The heuristic drops any event interval records that are interior
subsets of any other event interval record, and when the end of an event interval record
is overlapped by the next record the heuristic truncates the record on the start of the next
record. This heuristic ensures presence intervals are not truncated, selects the
dominate event interval record, in terms of length of time and recency, and ensures start
and end dates order the records equivalently. However, when two event interval records have
the exact same bounds a secondary heuristic ranking must be introduced so that only one
record is chosen.

Figure 6. Constructing a tail rectified partition.
```
e1: |--------|
e2:    |------------|
e3:       |-----|
e4:                          |---------------------------|
e5:                             |--------------|
e6:                                                           |-----------|
e7:                                                                 |---------|
TR: |--|------------|--------|---------------------------|----|-----|---------|
ST: ^  ^            ^        ^                           ^    ^     ^
```

The construction of tail rectified partitions hinges on finding, for each event interval
record, the smallest start date over all the records that end after the record; hence the
name tail rectified partitions. In the simplest circumstance where there are no two
event interval records with identical boundaries, finding the next start date requires
sorting by end dates then start dates, and finding the minimum of those record that end
after and then in the condition of duplicate end dates, started after.

Example 24. Analytic clause to find next start date
```SQL
MIN(e.start) OVER 
(
  ORDER BY
    e.end ASC NULLS LAST,
    e.start ASC NULLS FIRST
  ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
)
```

This heuristic stipulates that only records that are interior subsets of another record are
excluded. Records that align on their end dates will successively truncate each other, with
the earlier record ending on the start of the later record. The construction then proceeds
in three steps:

1. Collect all the events together into a single columnar list of events, introducing a
heuristic ranking for any events that have exactly equal boundaries.
2. Find the next start date, ranking first by the end dates, then by the start dates and
finally, if necessary, by the introduced heuristic ranking.
3. Filter the records returning only those where the start date is strictly less than the
next start date, and union that with the list of end dates that are at least two days before
the next start date, these are the same absence intervals as in the existentially qualified
partitions.

We continue with our working example of supportive living stays and long term care stays,
this time assigning a single stay record to each disjoint interval using the tail
rectification heuristic. The heuristic requires ranking site stays in the case of equal
intervals. We will rank supportive living stays by the level of care of the site.

Example 25. Supportive Living Rankings
```SQL
CASE a0.delivery_setting_affiliation
  WHEN 'SUPPORTIVE LIVING LEVEL 3' THEN
    3
  WHEN 'SUPPORTIVE LIVING LEVEL 4' THEN
    4
  ELSE
    5
END
```

We will sandwich the supportive living ranking with the long term care ranking, placing
funded stays higher, and unfunded stays lower.

Example 26. Long Term Care Rankings
```SQL
CASE a0.current_client_type
  WHEN 'Private pay resident' THEN
    1
  WHEN 'Short stay resident' THEN
    2
  ELSE
    6
END
```

In the first step we collect all the supportive living stays and long term care stays into a
single columnar list of events, and instantiate the ranking column.

Example 27. Consolidate residential site stays.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  CAST(a0.resident_stay_id AS VARCHAR2(16)) event_id,
  'Residential Site Stay' event_type,
  a0.current_client_type event_name,
  a0.facility_id event_code,
  CASE a0.current_client_type
    WHEN 'Private pay resident' THEN
      1
    WHEN 'Short stay resident' THEN
      2
    ELSE
      6
  END event_rank,
  a0.admit_date event_start,
  a0.discharge_date event_end
FROM
  TABLE(continuing_care.accis.get_adt) a0
UNION ALL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.stay_residence_id event_id,
  'Residential Site Stay' event_type,
  a0.delivery_setting_affiliation event_name,
  CAST(a0.facility_id AS VARCHAR2(16)) event_code,
  CASE a0.delivery_setting_affiliation
    WHEN 'SUPPORTIVE LIVING LEVEL 3' THEN
      3
    WHEN 'SUPPORTIVE LIVING LEVEL 4' THEN
      4
    ELSE
      5
  END event_rank,
  a0.entry_from_date event_start,
  a0.exit_to_date event_end
FROM
  TABLE(continuing_care.home_care.get_residency) a0
WHERE
  a0.delivery_setting_affiliation IN
  (
    'SUPPORTIVE LIVING LEVEL 3',
    'SUPPORTIVE LIVING LEVEL 4',
    'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
  )
  AND
  a0.uli_ab_phn IS NOT NULL
```

In the second step, we generate the partition boundaries by looking forward to the following
ending records and retrieving the next start date over all following ending records.

Example 28. Next start dates.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.event_id,
  a0.event_type,
  a0.event_name,
  a0.event_code,
  a0.event_start,
  a0.event_end,
  MIN(a0.event_start) OVER 
  (
    PARTITION BY
      a0.uli_ab_phn
    ORDER BY 
      a0.event_end ASC NULLS LAST,
      a0.event_start ASC NULLS FIRST,
      a0.event_rank ASC NULLS FIRST
    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
  ) next_start
FROM
  event_intervals a0
```

The third step selects all the disjoint partition intervals that start strictly before the
next computed start, or have no following events ending after itself; that is they are not
an interior subset of another event. We then rectify the end dates by the next start date.

Example 29. Tail rectified events.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.event_id,
  a0.event_type,
  a0.event_name,
  a0.event_code,
  a0.event_start,
  CASE
    WHEN a0.event_end IS NULL THEN
      a0.next_start - 1
    WHEN a0.next_start IS NULL THEN
      a0.event_end
    ELSE
      least(a0.event_end, a0.next_start - 1)
  END event_end
FROM
  partition_boundaries a0
WHERE
  a0.next_start IS NULL
  OR
  a0.event_start < a0.next_start
```

We also need to generate all the disjoint partition intervals for the absence of events.
These occur for the unique records that end two days before the next record starts. We
select these events and set the start to the day after the end, and the end to the day
before the next start.

Example 30. Absence intervals.
```SQL
SELECT
  a0.uli_ab_phn,
  NULL former_region,
  NULL information_system,
  NULL client_id,
  NULL event_id,
  'No Events' event_type,
  'Inferred Private Home' event_name,
  NULL event_code,
  a0.event_end + 1 event_start,
  a0.next_start - 1 event_end
FROM
  partition_boundaries a0
WHERE
  a0.event_end < a0.next_start - 1
ORDER BY
  1 DESC NULLS FIRST,
  2 ASC NULLS FIRST,
  10 ASC NULLS FIRST;
```

We combine the three previous steps into a single common table expression that generates
tail rectified partitions of time for each patient.

Example 31. Generating tail rectified partitions.
```SQL
WITH
  event_intervals AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      CAST(a0.resident_stay_id AS VARCHAR2(16)) event_id,
      'Residential Site Stay' event_type,
      a0.current_client_type event_name,
      a0.facility_id event_code,
      CASE a0.current_client_type
        WHEN 'Private pay resident' THEN
          1
        WHEN 'Short stay resident' THEN
          2
        ELSE
          6
      END event_rank,
      a0.admit_date event_start,
      a0.discharge_date event_end
    FROM
      TABLE(continuing_care.accis.get_adt) a0
    UNION ALL
    SELECT
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      a0.stay_residence_id event_id,
      'Residential Site Stay' event_type,
      a0.delivery_setting_affiliation event_name,
      CAST(a0.facility_id AS VARCHAR2(16)) event_code,
      CASE a0.delivery_setting_affiliation
        WHEN 'SUPPORTIVE LIVING LEVEL 3' THEN
          3
        WHEN 'SUPPORTIVE LIVING LEVEL 4' THEN
          4
        ELSE
          5
      END event_rank,
      a0.entry_from_date event_start,
      a0.exit_to_date event_end
    FROM
      TABLE(continuing_care.home_care.get_residency) a0
    WHERE
      a0.delivery_setting_affiliation IN
      (
        'SUPPORTIVE LIVING LEVEL 3',
        'SUPPORTIVE LIVING LEVEL 4',
        'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
      )
      AND
      a0.uli_ab_phn IS NOT NULL
  ),
  partition_boundaries AS
  (
    SELECT
      COUNT(*) OVER (PARTITION BY a0.uli_ab_phn) event_count,
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      a0.event_id,
      a0.event_type,
      a0.event_name,
      a0.event_code,
      a0.event_start,
      a0.event_end,
      MIN(a0.event_start) OVER 
      (
        PARTITION BY
          a0.uli_ab_phn
        ORDER BY 
          a0.event_end ASC NULLS LAST,
          a0.event_start ASC NULLS FIRST,
          a0.event_rank ASC NULLS FIRST
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) next_start
    FROM
      event_intervals a0
  )
SELECT
  a0.event_count,
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.event_id,
  a0.event_type,
  a0.event_name,
  a0.event_code,
  a0.event_start,
  CASE
    WHEN a0.event_end IS NULL THEN
      a0.next_start - 1
    WHEN a0.next_start IS NULL THEN
      a0.event_end
    ELSE
      least(a0.event_end, a0.next_start - 1)
  END event_end
FROM
  partition_boundaries a0
WHERE
  a0.next_start IS NULL
  OR
  a0.event_start < a0.next_start
UNION ALL
SELECT
  a0.event_count,
  a0.uli_ab_phn,
  NULL former_region,
  NULL information_system,
  NULL client_id,
  NULL event_id,
  'No Events' event_type,
  'Inferred Private Home' event_name,
  NULL event_code,
  a0.event_end + 1 event_start,
  a0.next_start - 1 event_end
FROM
  partition_boundaries a0
WHERE
  a0.event_end < a0.next_start - 1
ORDER BY
  1 DESC NULLS FIRST,
  2 ASC NULLS FIRST,
  10 ASC NULLS FIRST;
```

3: Universally Qualified Partitions
-----------------------------------
Universally qualified partitions are a refinement of tail rectified partition, and are the
deepest possible refinement of time from a list of event intervals, in the sense that it is
the coarsest partition of time that has boundary points for every change in events. While
universally qualified partitions are the most conceptually challenging, they are both the
easiest to implement, and the most flexible for analysis. The conceptual difficult with
universally qualified partitions is that each disjoint interval in the partition will be
represented by one or more records, each record representing an event that intersects with
the disjoint interval. In contrast both existentially qualified partitions and tail
rectified partitions contain just one record per disjoint interval in the partition. However
this multiplicity gives a great deal of choice in dimensional reduction when ascribing
observations to disjoint intervals.

Figure 7. Constructing an universally qualified partition.
```
e1: |--------|
e2:    |------------|
e3:       |-----|
e4:                          |---------------------------|
e5:                             |--------------|
e6:                                                           |-----------|
e7:                                                                 |---------|
UQ: |--|--|--|--|---|--------|--|--------------|---------|----|-----|-----|---|
ST: ^  ^  ^  ^  ^   ^        ^  ^              ^         ^    ^     ^     ^
```

The overall strategy to universally qualified partitions is to divide time by the boundaries
of every change in events, thus generating boundaries for every start and end point of every
event. The simplicity of generating universally qualified partitions hinges on observing
that for each interval of unique combinations of events we need only know which events
intersected with the beginning of the interval, the only nuance is assuming that end dates
are included in an event, and thus the events do not change until the day after the end
date. Universally qualified partitions are generated in three steps:

1. Collect all the events together into a single columnar list of events.
2. Pivot the start and end dates into a single columnar list of boundary dates, adding one
to the end dates.
3. Join the columnar list of boundary dates back to the columnar list of events.

To demonstrate the power and flexibility of universally qualified partitions in generating
intervals that compare events we will find all find all simultaneous stays in residential
delivery sites and assignments of home care client clinical group classifications.

In the first step we begin by consolidating the records of stays in supportive living, the
records of stays in long term care, and records of home care clinical client group
assignments into a single columnar data set. We ensure that all start dates are in a single
column, and all end dates are in a separate single column, regardless of the source.

Example 32. Consolidate residential site stays and client groups.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  CAST(a0.resident_stay_id AS VARCHAR2(16)) event_id,
  'Residential Site Stay' event_type,
  a0.current_client_type event_name,
  a0.facility_id event_code,
  a0.admit_date event_start,
  a0.discharge_date event_end
FROM
  TABLE(continuing_care.accis.get_adt) a0
UNION ALL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.stay_residence_id event_id,
  'Residential Site Stay' event_type,
  a0.delivery_setting_affiliation event_name,
  CAST(a0.facility_id AS VARCHAR2(16)) event_code,
  a0.entry_from_date event_start,
  a0.exit_to_date event_end
FROM
  TABLE(continuing_care.home_care.get_residency) a0
WHERE
  a0.delivery_setting_affiliation IN
  (
    'SUPPORTIVE LIVING LEVEL 3',
    'SUPPORTIVE LIVING LEVEL 4',
    'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
  )
  AND
  a0.uli_ab_phn IS NOT NULL
UNION ALL
SELECT
  a0.uli_ab_phn,
  a0.former_region,
  a0.information_system,
  a0.client_id,
  a0.group_cache_id event_id,
  'CIHI Client Group' event_type,
  a0.group_affiliation event_name,
  NULL event_code,
  a0.start_from_date event_start,
  a0.end_to_date event_end
FROM
  TABLE(continuing_care.home_care.get_group) a0
WHERE
  a0.group_affiliation IN
  (
    'ACUTE',
    'AWAITING BED OF CHOICE',
    'END OF LIFE',
    'LONG TERM SUPPORT',
    'MAINTENANCE',
    'REHABILITATION',
    'SHORT STAY',
    'WELLNESS'
  )
  AND
  a0.uli_ab_phn IS NOT NULL
```

In the second step we pivot the start and end dates into a single column of unique dates, so
that we can order the dates to produce a partition of the time line. As well we need to be
able to exclude the very last end date, as we do not want the last record to be an absence
record. We start by selecting all the start dates in the boundary column, and creating a
column that records the maximum end date for each patient.

Example 33. Pivot the start dates.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_start boundary_date,
  MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn)  last_date
FROM
  event_intervals a0
```

We then select all the end dates into the same column and add one to the end dates to
reflect the beginning of a change of events strictly after the end of an event. Like the
start date pivot we include the same column with the exact same maximum end date for each
patient.

Example 34. Pivot the end dates.
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_end + 1 boundary_date,
  MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn)  last_date
FROM
  event_intervals a0
WHERE
  a0.event_end IS NOT NULL
```

To complete the pivot of step two we combine the two queries together to create a single
list of unique interval boundaries that define the partition of time for each patient. Note
that we also exclude null end dates.

Example 35. Pivot start and end dates
```SQL
SELECT
  a0.uli_ab_phn,
  a0.event_start boundary_date,
  MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn)  last_date
FROM
  event_intervals a0
UNION
SELECT
  a0.uli_ab_phn,
  a0.event_end + 1 boundary_date,
  MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn)  last_date
FROM
  event_intervals a0
WHERE
  a0.event_end IS NOT NULL
```

In the third step we intersect the unique boundaries with the list of events, by simply
testing if the boundary falls within the event.

Example 36. Test for intersection.
```SQL
a0.boundary_date BETWEEN a1.event_start AND COALESCE(a1.event_end, TRUNC(SYSDATE))
```

We further want to exclude the very last end date, provided there are no events with an null
end date. We accomplish this by including any boundary date that is interior to an event, or
any boundary date that is less than the maximum end date for the patient.

Example 37. Exclude last boundary date
```SQL
a1.event_start IS NOT NULL
OR
a0.boundary_date < a0.last_date
```

A left join between the list of unique boundaries and the list of events creates a record
for each event that intersects with a disjoint partition interval, and ensures that if no
events intersect with the disjoint partition interval the absence interval is still
produced. The end date of the disjoint partition intervals is found by looking to the next
start date and subtracting one, assuming null dates are in the future, except for the last
disjoint interval, which is end dated by the end of the event, if it exists.

Example 38. Look ahead to the next start.
```SQL
COALESCE
(
  MIN(a0.boundary_date - 1) OVER
  (
    PARTITION BY
      a0.uli_ab_phn
    ORDER BY
      a0.boundary_date ASC NULLS LAST
    RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
  ),
  a1.event_end
)
```

The computed end dates are included as a column in the final data set.

Example 39. Disjoint partition intervals.
```SQL
SELECT
  a0.boundary_date interval_start,
  COALESCE
  (
    MIN(a0.boundary_date - 1) OVER
    (
      PARTITION BY
        a0.uli_ab_phn
      ORDER BY
        a0.boundary_date ASC NULLS LAST
      RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ),
    a1.event_end
  ) interval_end,
  a0.uli_ab_phn,
  a1.former_region,
  a1.information_system,
  a1.client_id,
  a1.event_id,
  COALESCE(a1.event_type, 'No Events') event_type,
  COALESCE(a1.event_name, 'No Client Group - Inferred Private Home') event_name,
  a1.event_code,
  a1.event_start,
  a1.event_end
FROM
  partition_boundaries a0
  LEFT JOIN
  event_intervals a1
  ON
    a0.uli_ab_phn = a1.uli_ab_phn
    AND
    a0.boundary_date BETWEEN a1.event_start AND COALESCE(a1.event_end, TRUNC(SYSDATE))
WHERE
  a1.event_start IS NOT NULL
  OR
  a0.boundary_date < a0.last_date
```

We combine the three previous steps into a single common table expression that generates
universally qualified partitions of time for each patient.

Example 40. Intersections of residential stays and client groups.
```SQL
WITH
  event_intervals AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      CAST(a0.resident_stay_id AS VARCHAR2(16)) event_id,
      'Residential Site Stay' event_type,
      a0.current_client_type event_name,
      a0.facility_id event_code,
      a0.admit_date event_start,
      a0.discharge_date event_end
    FROM
      TABLE(continuing_care.accis.get_adt) a0
    UNION ALL
    SELECT
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      a0.stay_residence_id event_id,
      'Residential Site Stay' event_type,
      a0.delivery_setting_affiliation event_name,
      CAST(a0.facility_id AS VARCHAR2(16)) event_code,
      a0.entry_from_date event_start,
      a0.exit_to_date event_end
    FROM
      TABLE(continuing_care.home_care.get_residency) a0
    WHERE
      a0.delivery_setting_affiliation IN
      (
        'SUPPORTIVE LIVING LEVEL 3',
        'SUPPORTIVE LIVING LEVEL 4',
        'SUPPORTIVE LIVING LEVEL 4 DEMENTIA'
      )
      AND
      a0.uli_ab_phn IS NOT NULL
    UNION ALL
    SELECT
      a0.uli_ab_phn,
      a0.former_region,
      a0.information_system,
      a0.client_id,
      a0.group_cache_id event_id,
      'CIHI Client Group' event_type,
      a0.group_affiliation event_name,
      NULL event_code,
      a0.start_from_date event_start,
      a0.end_to_date event_end
    FROM
      TABLE(continuing_care.home_care.get_group) a0
    WHERE
      a0.group_affiliation IN
      (
        'ACUTE',
        'AWAITING BED OF CHOICE',
        'END OF LIFE',
        'LONG TERM SUPPORT',
        'MAINTENANCE',
        'REHABILITATION',
        'SHORT STAY',
        'WELLNESS'
      )
      AND
      a0.uli_ab_phn IS NOT NULL
  ),
  partition_boundaries AS
  (
    SELECT
      a0.uli_ab_phn,
      a0.event_start boundary_date,
      MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn)  last_date
    FROM
      event_intervals a0
    UNION
    SELECT
      a0.uli_ab_phn,
      a0.event_end + 1 boundary_date,
      MAX(a0.event_end + 1) OVER (PARTITION BY a0.uli_ab_phn) last_date
    FROM
      event_intervals a0
    WHERE
      a0.event_end IS NOT NULL
  )
SELECT
  COUNT(*) OVER (PARTITION BY a0.uli_ab_phn) event_count,
  a0.boundary_date interval_start,
  COALESCE
  (
    MIN(a0.boundary_date - 1) OVER
    (
      PARTITION BY
        a0.uli_ab_phn
      ORDER BY
        a0.boundary_date ASC NULLS LAST
      RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ),
    a1.event_end
  ) interval_end,
  a0.uli_ab_phn,
  a1.former_region,
  a1.information_system,
  a1.client_id,
  a1.event_id,
  COALESCE(a1.event_type, 'No Events') event_type,
  COALESCE(a1.event_name, 'No Client Group - Inferred Private Home') event_name,
  a1.event_code,
  a1.event_start,
  a1.event_end
FROM
  partition_boundaries a0
  LEFT JOIN
  event_intervals a1
  ON
    a0.uli_ab_phn = a1.uli_ab_phn
    AND
    a0.boundary_date BETWEEN a1.event_start AND COALESCE(a1.event_end, TRUNC(SYSDATE))
WHERE
  a1.event_start IS NOT NULL
  OR
  a0.boundary_date < a0.last_date
ORDER BY
  1 DESC NULLS FIRST,
  4 ASC NULLS FIRST,
  2 ASC NULLS FIRST,
  12 ASC NULLS FIRST,
  13 ASC NULLS FIRST;
```