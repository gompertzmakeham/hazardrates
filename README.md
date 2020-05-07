Hazard Rates
============

Declaration and Land Acknowledgement
------------------------------------

This project, its materials, resources, and manpower are wholly funded by Alberta Health Services for the purpose of informing health system performance improvement, and health care quality improvement. [Alberta Health Services](https://www.albertahealthservices.ca/) is the single public health authority of the province of Alberta, within the boundaries of the traditional lands of the nations of Treaties [6](https://en.wikipedia.org/wiki/Treaty_6), [7](https://en.wikipedia.org/wiki/Treaty_7), [8](https://en.wikipedia.org/wiki/Treaty_8), and [10](https://en.wikipedia.org/wiki/Treaty_10), and the peoples of Metis Regions 1, 2, 4, 5, and 6, including the Cree, Dene, Inuit, Kainai, Metis, Nakota Sioux, Piikani, Saulteaux, Siksika, Tsek’ehne, Tsuut’ina. The author is best reached through the project [issue log](https://github.com/gompertzmakeham/hazardrates/issues).

Introduction
------------

Proactive methodological disclosure of a high resolution precision calibrated estimate of the Gompertz-Makeham Law of Mortality and general utilization hazard rates through lifespan interferometry against annual census data consolidated from the administrative data of all publicly funded healthcare provided in a single geopolitical jurisdiction. This repository only contains the source code, and only for the purpose of peer review, validation, and replication. This repository does not contain any data, results, findings, figures, conclusions, or discussions.

This code base is under active development, and is currently being tested against a data store compiled from 19 distinct administrative data sets containing nearly 3 billion healthcare utilization events, each with couple hundred features, covering more than 6 million individual persons, and two decades of surveillance. The application generates approximately 175 million time intervals, each with four dozen features, by Measure Theory consistent Temporal Joins and dimensional reduction, implemented in ad hoc map-reduce steps.

High resolution estimation of mortality and utilization hazard rates is accomplished by measuring the person-time denominator of the hazard rates to the single person-day, without any rounding or truncation to coarser time scales. The precision of the hazard rate estimators are calibrated against the main source of epistemic uncertainty: clerical equivocation in the measurement, recording, and retention of the life events of birth, death, immigration, and emigration. The aleatory uncertainty is estimated using standard errors formally derived by applying the Delta Method to the formal representation of the hazard rate estimators as equations of random variables. The existence, uniqueness, and consistency of the standard errors are left unproven, although a straightforward application of the usual asymptotic Maximum Likelihood theory should suffice.

Overview
--------

The construction of the denominators and numerators of the hazard rate analysis broadly proceeds in 11 steps of ad hoc map-reduce and dynamic reconstitution, to produce records of person census intervals:

1. Ingest independently and in parallel the external administrative data sources, mapping the clerically records of life events and demographic information.
2. Digest independently and in parallel the mapped data sources from step 1, reducing each source to one record per person. The files in the [survey](https://github.com/gompertzmakeham/hazardrates/tree/master/source/structures/survey) folder contain steps 1 and 2.
3. Ingest sequentially the reduced data sources from step 2, mapping into a common structure.
4. Digest sequentially the mapped common structure from step 3, reducing to one master record per person, containing the extremums of life event dates. The file [persondemographic.sql](source/structures/survey/persondemographic.sql) contains steps 3 and 4.
5. Dynamically reconstitute the pair of surveillance extremums for each person from step 4. This process is contained in the file [personsurveillance.sql](source/structures/survey/personsurveillance.sql).
6. Dynamically reconstitute the census intervals for each surveillance extremum from step 5. This process is contained in the file [personcensus.sql](source/structures/census/personcensus.sql).
7. Ingest independently and in parallel the external administrative data sources, mapping the transactional records of utilization events and the event details.
8. Digest independently and in parallel the mapped data sources from step 7, reducing each source to one record per person per census interval, using the dynamically generated census intervals from step 6. The files in the [census](https://github.com/gompertzmakeham/hazardrates/tree/master/source/structures/census) folder contains steps 7 and 8.
9. Ingest sequentially the reduced records per person per census interval from step 8, mapping to a common data structure.
10. Digest sequentially the mapped common data structure from step 9, reducing by Temporal Joins to one record per person per census interval, containing the utilization in that census interval. The file [personutilization.sql](source/structures/census/personutilization.sql) contains steps 9 and 10.
11. Dynamically reconstitute a columnar list of utilization measures, eliding trivial measures. This is contained in
the file [personmeasure.sql](source/structures/census/personmeasure.sql).

Currently the build process is contained in [refresh.sql](source/refresh.sql); which for the time being will remain partly manual because of idiosyncratic crashes that occur during table builds, possibly due to locking of the underlying table sources. An example of querying the terminal assets of this analysis is contained in the files [dense.sql](documentation/examples/dense.sql) and [columnar.sql](documentation/examples/columnar.sql).

Temporal Joins
--------------

In keeping with declarative languages, a [Measure Theory](https://en.wikipedia.org/wiki/Measure_(mathematics)) consistent Temporal Join on a longitudinal data set is defined by the global characteristics of the resulting data set. Specifically, a [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra) join is a Measure Theory consistent Temporal Join if the resulting data set represents a [totally ordered](https://en.wikipedia.org/wiki/Total_order) [partition](https://en.wikipedia.org/wiki/Partition_of_a_set) of a bounded time span under the absolute set ordering. Furthermore, the time intervals represented by any two records produced by a Temporal Join must intersect trivial, either being disjoint, or equal. Put more simply, a Temporal Join takes a time span,

    |--------------------------------------------------------------------------------------|

 and partitions it into compact contiguous intervals, of possibly unequal lengths,

    |-------|--------|-------------|-----|--------|--|--|--------------|-----------|-------|

such that the produced data set contains at least one, and possibly arbitrarily more, records for each interval. A Temporal Join unambiguously ascribes a definite set of features, from one or more records, to each moment in a time span, because there are neither gaps in the representation of time, nor non-trivial intersections between intervals. Temporal Joins are [Category Theory](https://en.wikipedia.org/wiki/Category_theory) closed, in that the [composition](https://en.wikipedia.org/wiki/Composition_operator) of Temporal Joins is a Temporal Join, because successive Temporal Joins are Measure Theory (finite) [refinements](https://en.wikipedia.org/wiki/Sigma-algebra#Combining_%CF%83-algebras) of the (finite) minimal [sigma algebra](https://en.wikipedia.org/wiki/Sigma-algebra) to which the partition belongs.

Given two intervals a Temporal Join will generate one, two, or three records. If the intervals have identical boundaries the result will be a single record containing the characteristics of the two source intervals:

    |--------------------|
               a                 

    |--------------------|
               b

    |--------------------|
             a & b

If the boundaries of the intervals are exactly contiguous the result will be two records:

    |-------|
        a

            |------------|
                   b

    |-------|------------|
        a          b

If the intervals intersect non-trivial the result will be three records:

    |-----------------|
           a

              |-----------------|
                    b

    |---------|-------|---------|
         a      a & b      b

Finally if the intervals are fully disjoint and not contiguous the result will also be three records:

    |---------|
         a

                         |-------------------|
                                  b

    |---------|----------|-------------------|                       
          a     ~(a & b)           b

This construction can be composed by iteration on any (finite) number of intervals, because the Category of Temporal Joins is Measure Theory closed with respect to refinement. Fortunately much faster techniques can be found than the naive iteration, exploiting either sorting on the boundary dates and then back searching, or in the case of the methods in this analysis, by explicitly constructing the intervals based on the bounds of the events.

Concretely, in the context of this project, for each surveillance time span during which a person's healthcare utilization was observed, we divide the time span into fiscal years, starting on April 1, and further subdivide each fiscal year on the person's birthday in the fiscal year; where if the birthday falls on April 1 the fiscal year is not subdivided. This is precisely what the function [hazardutilities.generatecensus](source/packages/hazardutilities/interface.sql) implements, taking three dates, a start date, an end date, and a date of birth.

Events
------

The administrative data sources we work has three modalities of recording information about events: existential, clerical, and transactional. Transactional recording of events occurs at the time of the event, is general completed by the person delivering the service recorded in the transaction, and may record information about prior events during the process of collecting information about the current event, examples include: visits to inpatient care, dispensing of prescribed pharmaceuticals at community pharmacy, and delivery of home care. Clerical recording of events occurs after the event has occurred, requiring recall on the part of the participants of the event, usually in the form of self-report by the recipient of services, examples include: symptom onset, birth dates, and provincial migration dates. Finally existential records record a broad time interval during which an event was known to have occurred, these are usual recorded in the contexts of administrative registrations in programs, and time intervals of data capture by administrative information systems, examples include: year of coverage start, quarter of inpatient record capture, and registration for home care.

Differentiation between:

* transactionally recorded events
* clerically recorded events
* Existential bounds on events

The impact of:

* *Censoring* is what you do not know about the patients you have been observing because you cannot see into the future.
* *Survivorship bias* is what you do not know about the patients you never observed because they did not live long enough to be included.
* *Immortal time bias* is what you do not know about observed patients because you cannot see into the past

Epistemic Uncertainty
---------------------

We measure epistemic uncertainty using Clerical Equivocation Interferometry against the clerical recording of the lifespan events of birth, death, immigration, and emigration. This technique begins by identifying, for each person, the shortest and longest lifespan possible given all the clerical events. Given entry events `O`, and exit events `X`, we generate two lifespans, the longest and the shortest:

    O-----O--O---O----O----------------------------------X--X------X-X-X----------X

    |-----------------------------------------------------------------------------|

                      |----------------------------------|

Within each lifespan, we then identify the shortest possible surveillance interval within the shortest lifespan, and the longest possible surveillance interval within in the longest
lifespan.

* Transactional dates are fixed, but age may change due to clerical uncertainty, moving events to different age buckets.
* Shortest is not the upper bound, longest is not the lower bound, they can even cross
* Not the uniform norm bound either
* The envelope is a mini-maxi estimator, minimum covariance, maximum variance.

It is a *reasonable* estimate of epistemic uncertainty. It is not the maximum variance possible due to clerical equivocation, but it is a reasonable amount. Combinatoric methods could provide broader bounds, but the computational trade-offs in terms of expediency of the analysis were not worth it at this time.

Aleatory Uncertainty
--------------------

We measure aleatory uncertainty using a non-parametric standard error of the hazard rate estimator. *embed Codecog LaTeX images*

Data Sources
------------

Of the 19 data sources that currently feed into this hazard rates analysis, a number either partially or completely publish their data collection methodology, definitions, and standards:

- *Ambulatory care* Canadian Institute of Health Information [National Ambulatory Care Reporting System](https://www.cihi.ca/en/national-ambulatory-care-reporting-system-metadata).
  - 2 data sources from 2002 onward; currently approximately 126 000 000 events.
- *Inpatient care* Canadian Institute of Health Information [Discharge Abstract Database](https://www.cihi.ca/en/discharge-abstract-database-metadata).
  - 1 data source from 2002 onward; current approximately 7 000 000 events.
- *Long Term Care* Canadian Institute of Health Information [Resident Assessment Instrument](https://www.cihi.ca/en/residential-care).
  - 1 data source from 2010 onward; currently approximately 560 000 events.
- *Primary Care* Alberta Health [Schedule of Medical Benefits](https://www.alberta.ca/fees-health-professionals.aspx).
  - 1 data source from 2001 onward; currently approximately 656 000 000 events.
- *Community Pharmacy Dispensing* Alberta Health [Pharmacy Information Network](http://www.albertanetcare.ca/learningcentre/Pharmaceutical-Information-Network.htm).
  - 1 data source from 2008 onward; currently approximately 605 000 000 events.
- *Annual Registry* Alberta Health [Alberta Health Care Insurance Plan](https://www.alberta.ca/ahcip.aspx)
  - 1 data source from 1993 onward; currently approximately 90 000 000 events.
- *Continuing Care Registrations* proprietary direct access (Civica, Meditech, StrataHealth).
  - 3 data sources, phased adoption 2008, 2010, and 2012 onward; currently approximately 520 000 events.
- *Community Laboratory Collections* proprietary direct access (Fusion, Meditech, Millennium, SunQuest).
  - 4 data sources, phased adoption 2008, 2009, 2012, and 2014 onward; currently approximately 1 500 000 000 events.
- *Care Management* proprietary direct access (Civica, Meditech).
  - 2 data sources phased adoption 2008, 2010, and 2012 onward; currently approximately 2 800 000 events.
- *Home Care Activity* proprietary direct access (Civica, Meditech, StrataHealth).
  - 3 data sources, phased adoption 2008, 2010, and 2012 onward; currently approximately 70 000 000 events.
- *Diagnostic Imaging* proprietary direct access (in staging).
  - Not calibrated yet.
- *Emergency Medical Services* proprietary direct access (in staging).
  - Not calibrated yet.
- *Health Link* proprietary direct access (in staging).
  - Not calibrated yet.

Disclaimer
----------
The aggregate provincial data presented in this project are compiled in accordance with the [Health Information Act of Alberta](http://canlii.ca/t/81pf) under the provision of [Part 4, Section 27, Sub-section 1(g)](http://canlii.ca/t/53fss#sec27subsec1) for the purpose of health system quality improvement, monitoring, and evaluation. Further to this the aggregate provincial data are released under all the provisions of [Part 4, Section 27, Subsection 2 of the Health Information Act of Alberat](<http://canlii.ca/t/53fss#sec27subsec2>).

This material is intended for general information only, and is provided on an *"as is"*, or *"where is"* basis. Although reasonable efforts were made to confirm the accuracy of the information, Alberta Health Services does not make any representation or warranty, express, implied, or statutory, as to the accuracy, reliability, completeness, applicability, or fitness for a particular purpose of such information. This material is not a substitute for the advice of a qualified health professional. Alberta Health Services expressly disclaims all liability for the use of these materials, and for any claims, actions, demands, or suits arising from such use.
