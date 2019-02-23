/*
 *  Reverse dependency order releasing.
 */

-- Utilization support
DROP MATERIALIZED VIEW personutilization;
DROP MATERIALIZED VIEW personcensus;

-- Census structures
DROP MATERIALIZED VIEW censusambulatorycare;
DROP MATERIALIZED VIEW censusinpatientcare;
DROP MATERIALIZED VIEW censuslongtermcare;
DROP MATERIALIZED VIEW censuslaboratorycollection;
DROP MATERIALIZED VIEW censuspharmacydispense;
DROP MATERIALIZED VIEW censusprimarycare;
DROP MATERIALIZED VIEW censussupportiveliving;

-- Surveillance support
DROP MATERIALIZED VIEW personsurveillance;
DROP MATERIALIZED VIEW persondemographic;

-- Surveillance structures
DROP MATERIALIZED VIEW surveyambulatorycare;
DROP MATERIALIZED VIEW surveyannualregistry;
DROP MATERIALIZED VIEW surveycontinuingcare;
DROP MATERIALIZED VIEW surveyinpatientcare;
DROP MATERIALIZED VIEW surveylaboratorycollection;
DROP MATERIALIZED VIEW surveypharmacydispense;
DROP MATERIALIZED VIEW surveyprimarycare;
DROP MATERIALIZED VIEW surveyvitalstatistics;

-- Base support
DROP PACKAGE maintenanceutilities;
DROP PACKAGE hazardutilities;