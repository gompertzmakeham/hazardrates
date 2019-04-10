SELECT
CASE [hazardrate]
    WHEN 'INTERVALDECEASED' THEN
        1000 
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INTERVALDECEASED])
            / SUM([DURATIONDAYS]) 
    WHEN 'INTERVALEMIGRATE' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INTERVALEMIGRATE])
            / SUM([DURATIONDAYS])
    WHEN 'LIVENEWBORNS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LIVENEWBORNS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYMINUTES])
            / (60 * SUM([DURATIONDAYS]))
    WHEN 'AMBULATORYVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYLENGTHOFSTAY' THEN
        SUM([AMBULATORYMINUTES])
            / (60 * SUM([AMBULATORYVISITS]))
    WHEN 'AMBULATORYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([AMBULATORYVISITS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYPRIVATEMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYPRIVATEMINUTES])
            / (60 * SUM([DURATIONDAYS]))
    WHEN 'AMBULATORYPRIVATEVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYPRIVATEVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYPRIVATELENGTHOFSTAY' THEN
        SUM([AMBULATORYPRIVATEMINUTES])
            / (60 * SUM([AMBULATORYPRIVATEVISITS]))
    WHEN 'AMBULATORYPRIVATESITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYPRIVATESITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYPRIVATEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYPRIVATEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYPRIVATEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([AMBULATORYPRIVATEVISITS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYWORKMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYWORKMINUTES])
            / (60 * SUM([DURATIONDAYS]))
    WHEN 'AMBULATORYWORKVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYWORKVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYWORKLENGTHOFSTAY' THEN
        SUM([AMBULATORYWORKMINUTES])
            / (60 * SUM([AMBULATORYWORKVISITS]))
    WHEN 'AMBULATORYWORKSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYWORKSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYWORKDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([AMBULATORYWORKDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'AMBULATORYWORKPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([AMBULATORYWORKVISITS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTADMISSIONS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTDISCHARGES])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTLENGTHOFSTAY' THEN
        SUM([INPATIENTDAYS])
            / SUM([INPATIENTDISCHARGES])
    WHEN 'INPATIENTSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTSTAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([INPATIENTSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPRIVATEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTPRIVATEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPRIVATEADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTPRIVATEADMISSIONS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPRIVATEDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTPRIVATEDISCHARGES])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPRIVATELENGTHOFSTAY' THEN
        SUM([INPATIENTPRIVATEDAYS])
            / SUM([INPATIENTPRIVATEDISCHARGES])
    WHEN 'INPATIENTPRIVATESTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTPRIVATESTAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTPRIVATEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([INPATIENTPRIVATESTAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTWORKDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTWORKDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTWORKADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTWORKADMISSIONS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTWORKDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTWORKDISCHARGES])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTWORKLENGTHOFSTAY' THEN
        SUM([INPATIENTWORKDAYS])
            / SUM([INPATIENTWORKDISCHARGES])
    WHEN 'INPATIENTWORKSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([INPATIENTWORKSTAYS])
            / SUM([DURATIONDAYS])
    WHEN 'INPATIENTWORKPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([INPATIENTWORKSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'CAREMANAGERDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CAREMANAGERDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'CAREMANAGERALLOCATIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CAREMANAGERALLOCATIONS])
            / SUM([DURATIONDAYS])
    WHEN 'CAREMANAGERRELEASES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CAREMANAGERRELEASES])
            / SUM([DURATIONDAYS])
    WHEN 'CAREMANAGERLENGTH' THEN
        SUM([CAREMANAGERDAYS])
            / (ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CAREMANAGERRELEASES]))
    WHEN 'CAREMANAGERS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CAREMANAGERS])
            / SUM([DURATIONDAYS])
    WHEN 'CAREMANAGERPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CAREMANAGERS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'HOMECARESERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECARESERVICES])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECAREVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECAREVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'SERVICESPERVISIT' THEN
        SUM([HOMECARESERVICES])
            / SUM([HOMECAREVISITS])
    WHEN 'HOMECAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECAREDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([HOMECARESERVICES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'HOMECAREPROFESSIONALSERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECAREPROFESSIONALSERVICES])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECAREPROFESSIONALVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECAREPROFESSIONALVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'PROFESSIONALSERVICESPERVISIT' THEN
        SUM([HOMECAREPROFESSIONALSERVICES])
            / SUM([HOMECAREPROFESSIONALVISITS])
    WHEN 'HOMECAREPROFESSIONALDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECAREPROFESSIONALDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECAREPROFESSIONALPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([HOMECAREPROFESSIONALSERVICES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'HOMECARETRANSITIONSERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECARETRANSITIONSERVICES])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECARETRANSITIONVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECARETRANSITIONVISITS])
            / SUM([DURATIONDAYS])
    WHEN 'TRANSITIONSERVICESPERVISIT' THEN
        SUM([HOMECARETRANSITIONSERVICES])
            / SUM([HOMECARETRANSITIONVISITS])
    WHEN 'HOMECARETRANSITIONDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([HOMECARETRANSITIONDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'HOMECARETRANSITIONPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([HOMECARETRANSITIONSERVICES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'LABORATORYASSAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LABORATORYASSAYS])
            / SUM([DURATIONDAYS])
    WHEN 'LABORATORYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LABORATORYSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'ASSAYSPERCOLLECTION' THEN
        SUM([LABORATORYASSAYS])
            / SUM([LABORATORYSITEDAYS])
    WHEN 'LABORATORYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LABORATORYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'LABORATORYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([LABORATORYASSAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'LONGTERMCAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LONGTERMCAREDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'LONGTERMCAREADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LONGTERMCAREADMISSIONS])
            / SUM([DURATIONDAYS])
    WHEN 'LONGTERMCAREDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LONGTERMCAREDISCHARGES])
            / SUM([DURATIONDAYS])
    WHEN 'LONGTERMCARELENGTHOFSTAY' THEN
        SUM([LONGTERMCAREDAYS])
            / (ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LONGTERMCAREDISCHARGES]))
    WHEN 'LONGTERMCARESTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([LONGTERMCARESTAYS])
            / SUM([DURATIONDAYS])
    WHEN 'LONGTERMCAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([LONGTERMCARESTAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYDAILYDOSES' THEN
        SUM([PHARMACYDAILYDOSES])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYTHERAPEUTICS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYTHERAPEUTICS])
            / SUM([DURATIONDAYS])
    WHEN 'DOSESPERTHERAPY' THEN
        SUM([PHARMACYDAILYDOSES])
            / SUM([PHARMACYTHERAPEUTICS])
    WHEN 'ALLDOSESPERDISPENSEDPERSON' THEN
        SUM([PHARMACYDAILYDOSES])
            / SUM(IIF([PHARMACYTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYPERCENTDISPENSED' THEN
        100
            * SUM(IIF([PHARMACYTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYSTANDARDDAILYDOSES' THEN
        SUM([PHARMACYSTANDARDDAILYDOSES])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYSTANDARDTHERAPEUTICS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYSTANDARDTHERAPEUTICS])
            / SUM([DURATIONDAYS])
    WHEN 'DOSESPERSTANDARDTHERAPY' THEN
        SUM([PHARMACYSTANDARDDAILYDOSES])
            / SUM([PHARMACYSTANDARDTHERAPEUTICS])
    WHEN 'STANDARDDOSESPERDISPENSEDPERSON' THEN
        SUM([PHARMACYSTANDARDDAILYDOSES])
            / SUM(IIF([PHARMACYSTANDARDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYSTANDARDSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYSTANDARDSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYSTANDARDDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYSTANDARDDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYPERCENTDISPENSEDSTANDARD' THEN
        100
            * SUM(IIF([PHARMACYSTANDARDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYCONTROLLEDDAILYDOSES' THEN
        1000
            * SUM([PHARMACYCONTROLLEDDAILYDOSES])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYCONTROLLEDTHERAPEUTICS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYCONTROLLEDTHERAPEUTICS])
            / SUM([DURATIONDAYS])
    WHEN 'DOSESPERCONTROLLEDTHERAPY' THEN
        SUM([PHARMACYCONTROLLEDDAILYDOSES])
            / SUM([PHARMACYCONTROLLEDTHERAPEUTICS])
    WHEN 'CONTROLLEDDOSESPERDISPENSEDPERSON' THEN
        1000
            * SUM([PHARMACYCONTROLLEDDAILYDOSES])
            / SUM(IIF([PHARMACYCONTROLLEDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYCONTROLLEDSITEDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYCONTROLLEDSITEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYCONTROLLEDDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PHARMACYCONTROLLEDDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PHARMACYPERCENTDISPENSEDCONTROLLED' THEN
        100
            * SUM(IIF([PHARMACYCONTROLLEDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'PRIMARYCAREPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PRIMARYCAREPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'PRIMARYCAREPROVIDERDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PRIMARYCAREPROVIDERDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERPROVIDER' THEN
        SUM([PRIMARYCAREPROCEDURES])
            / SUM([PRIMARYCAREPROVIDERDAYS])
    WHEN 'PRIMARYCAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PRIMARYCAREDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PRIMARYCAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([PRIMARYCAREPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'ANESTHESIOLOGYPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([ANESTHESIOLOGYPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'ANESTHESIOLOGISTSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([ANESTHESIOLOGISTSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERANESTHESIOLOGIST' THEN
        SUM([ANESTHESIOLOGYPROCEDURES])
            / SUM([ANESTHESIOLOGISTSDAYS])
    WHEN 'ANESTHESIOLOGYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([ANESTHESIOLOGYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'ANESTHESIOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([ANESTHESIOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'CONSULTPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CONSULTPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'CONSULTPROVIDERDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CONSULTPROVIDERSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERCONSULT' THEN
        SUM([CONSULTPROCEDURES])
            / SUM([CONSULTPROVIDERSDAYS])
    WHEN 'CONSULTDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([CONSULTDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'CONSULTPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CONSULTPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'GENERALPRACTICEPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([GENERALPRACTICEPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'GENERALPRACTITIONERSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([GENERALPRACTITIONERSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERGENERALPRACTITIONER' THEN
        SUM([GENERALPRACTICEPROCEDURES])
            / SUM([GENERALPRACTITIONERSDAYS])
    WHEN 'GENERALPRACTICEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([GENERALPRACTICEDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'GENERALPRACTICEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([GENERALPRACTICEPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'OBSTETRICPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([OBSTETRICPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'OBSTETRICIANSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([OBSTETRICIANSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPEROBSTETRICIAN' THEN
        SUM([OBSTETRICPROCEDURES])
            / SUM([OBSTETRICIANSDAYS])
    WHEN 'OBSTETRICDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([OBSTETRICDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'OBSTETRICPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([OBSTETRICPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'PATHOLOGYPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PATHOLOGYPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'PATHOLOGISTSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PATHOLOGISTSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERPATHOLOGIST' THEN
        SUM([PATHOLOGYPROCEDURES])
            / SUM([PATHOLOGISTSDAYS])
    WHEN 'PATHOLOGYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([PATHOLOGYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PATHOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([PATHOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'RADIOLOGYPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([RADIOLOGYPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'RADIOLOGISTSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([RADIOLOGISTSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERRADIOLOGIST' THEN
        SUM([RADIOLOGYPROCEDURES])
            / SUM([RADIOLOGISTSDAYS])
    WHEN 'RADIOLOGYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([RADIOLOGYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'RADIOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([RADIOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'SPECIALTYPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SPECIALTYPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'SPECIALISTSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SPECIALISTSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERSPECIALIST' THEN
        SUM([SPECIALTYPROCEDURES])
            / SUM([SPECIALISTSDAYS])
    WHEN 'SPECIALTYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SPECIALTYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'SPECIALTYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([SPECIALTYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'SURGICALPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SURGICALPROCEDURES])
            / SUM([DURATIONDAYS])
    WHEN 'SURGEONSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SURGEONSDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'PROCEDURESPERSURGEON' THEN
        SUM([SURGICALPROCEDURES])
            / SUM([SURGEONSDAYS])
    WHEN 'SURGERYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SURGERYDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'SURGERYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([SURGICALPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    WHEN 'SUPPORTIVELIVINGDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SUPPORTIVELIVINGDAYS])
            / SUM([DURATIONDAYS])
    WHEN 'SUPPORTIVELIVINGADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SUPPORTIVELIVINGADMISSIONS])
            / SUM([DURATIONDAYS])
    WHEN 'SUPPORTIVELIVINGDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SUPPORTIVELIVINGDISCHARGES])
            / SUM([DURATIONDAYS])
    WHEN 'SUPPORTIVELIVINGLENGTHOFSTAY' THEN
        SUM([SUPPORTIVELIVINGDAYS])
            / (ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SUPPORTIVELIVINGDISCHARGES]))
    WHEN 'SUPPORTIVELIVINGSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM([SUPPORTIVELIVINGSTAYS])
            / SUM([DURATIONDAYS])
    WHEN 'SUPPORTIVELIVINGPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([SUPPORTIVELIVINGSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM([DURATIONDAYS])
    ELSE
        0.0
END