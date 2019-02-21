Hazard Rates
============

Introduction
------------

Proactive methodological disclosure of a high resolution precision calibrated estimation of the Gompertz-Makeham Law of Mortality in annual census data consolidated from the healthcare administration data of all publicly funded services provided in a single geopolitical jurisdiction. This repository only contains the source code, and only for the purpose of peer review, validation, and replication. This repository does not contain any data, results, findings, figures, conclusions, or discussions.

This code base is under active development, and is currently being tested against a data store of approximately 3 billion health utilization events, each containing roughly a couple hundred features, across 19 distinct data sets, covering approximately 6 million individual persons from the year 1993 to present day. The source events are dimensionally reduced using measure theoretically consistent temporal joins implemented in ad hoc map-reduce steps to generate approximately 300 million time intervals, each with roughly four dozen features.

High resolution estimation of mortality and utilization hazard rates is accomplished by measuring the person-time denominator of the hazard rates to the single person-day, without any rounding or truncation to larger time scales. The precision of the hazard rate estimators are calibrated against the main source of epistemic uncertainty: clerical equivocation in the measurement, recording, and retention of the life events of birth, death, immigration, and emigration. The aleatoric uncertainty is estimated using standard errors formally derived by applying the Delta Method to the formal representation of the hazard rate estimators as equations of random variables. The existence, uniqueness, and consistency of the standard errors are left unproven, although a straightforward application of the usual asymptotic Maximum Likelihood theory should suffice.

Overview
--------

Outline of ingest-digest-reconstitute map-reduce steps.

Events
------

Definite observations, versus known to exist.

Temporal Joins
--------------

Partitions total ordered under absolute set ordering
