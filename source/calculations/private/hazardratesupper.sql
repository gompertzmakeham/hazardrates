SELECT
CASE [hazardrate]
    WHEN 'INTERVALDECEASED' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INTERVALDECEASED], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INTERVALEMIGRATE' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INTERVALEMIGRATE], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0)))
    WHEN 'AMBULATORYVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYLENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [AMBULATORYMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYVISITS], 0)))
    WHEN 'AMBULATORYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [AMBULATORYVISITS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYPRIVATEMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATEMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0)))
    WHEN 'AMBULATORYPRIVATEVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATEVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYPRIVATELENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATEMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATEVISITS], 0)))
    WHEN 'AMBULATORYPRIVATESITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATESITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYPRIVATEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYPRIVATEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYPRIVATEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [AMBULATORYPRIVATEVISITS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYWORKMINUTES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0)))
    WHEN 'AMBULATORYWORKVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYWORKLENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKMINUTES], 0))
            / (60 * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKVISITS], 0)))
    WHEN 'AMBULATORYWORKSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYWORKDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [AMBULATORYWORKDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'AMBULATORYWORKPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [AMBULATORYWORKVISITS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTADMISSIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTDISCHARGES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTLENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [INPATIENTDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [INPATIENTDISCHARGES], 0))
    WHEN 'INPATIENTSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTSTAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [INPATIENTSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPRIVATEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPRIVATEADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATEADMISSIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPRIVATEDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATEDISCHARGES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPRIVATELENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATEDISCHARGES], 0))
    WHEN 'INPATIENTPRIVATESTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTPRIVATESTAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTPRIVATEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [INPATIENTPRIVATESTAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTWORKDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTWORKADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKADMISSIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTWORKDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKDISCHARGES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTWORKLENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKDISCHARGES], 0))
    WHEN 'INPATIENTWORKSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [INPATIENTWORKSTAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'INPATIENTWORKPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [INPATIENTWORKSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CAREMANAGERDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CAREMANAGERALLOCATIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERALLOCATIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CAREMANAGERRELEASES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERRELEASES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CAREMANAGERLENGTH' THEN
        SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERRELEASES], 0))
    WHEN 'CAREMANAGERS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CAREMANAGERS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CAREMANAGERPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [CAREMANAGERS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECARESERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECARESERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECAREVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECAREVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SERVICESPERVISIT' THEN
        SUM(IIF([CORNERCASE] = 'U', [HOMECARESERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [HOMECAREVISITS], 0))
    WHEN 'HOMECAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECAREDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [HOMECARESERVICES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECAREPROFESSIONALSERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECAREPROFESSIONALSERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECAREPROFESSIONALVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECAREPROFESSIONALVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROFESSIONALSERVICESPERVISIT' THEN
        SUM(IIF([CORNERCASE] = 'U', [HOMECAREPROFESSIONALSERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [HOMECAREPROFESSIONALVISITS], 0))
    WHEN 'HOMECAREPROFESSIONALDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECAREPROFESSIONALDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECAREPROFESSIONALPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [HOMECAREPROFESSIONALSERVICES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECARETRANSITIONSERVICES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECARETRANSITIONSERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECARETRANSITIONVISITS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECARETRANSITIONVISITS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'TRANSITIONSERVICESPERVISIT' THEN
        SUM(IIF([CORNERCASE] = 'U', [HOMECARETRANSITIONSERVICES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [HOMECARETRANSITIONVISITS], 0))
    WHEN 'HOMECARETRANSITIONDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [HOMECARETRANSITIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'HOMECARETRANSITIONPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [HOMECARETRANSITIONSERVICES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LABORATORYASSAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LABORATORYASSAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LABORATORYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LABORATORYSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'ASSAYSPERCOLLECTION' THEN
        SUM(IIF([CORNERCASE] = 'U', [LABORATORYASSAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [LABORATORYSITEDAYS], 0))
    WHEN 'LABORATORYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LABORATORYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LABORATORYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [LABORATORYASSAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LONGTERMCAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LONGTERMCAREDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LONGTERMCAREADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LONGTERMCAREADMISSIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LONGTERMCAREDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LONGTERMCAREDISCHARGES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LONGTERMCARELENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [LONGTERMCAREDAYS], 0))
            / (ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LONGTERMCAREDISCHARGES], 0)))
    WHEN 'LONGTERMCARESTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [LONGTERMCARESTAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'LONGTERMCAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [LONGTERMCARESTAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYDAILYDOSES' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYTHERAPEUTICS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYTHERAPEUTICS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'DOSESPERTHERAPY' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [PHARMACYTHERAPEUTICS], 0))
    WHEN 'ALLDOSESPERDISPENSEDPERSON' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYDAILYDOSES], 0))
            / SUM(IIF([PHARMACYTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYPERCENTDISPENSED' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [PHARMACYTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYSTANDARDDAILYDOSES' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYSTANDARDTHERAPEUTICS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDTHERAPEUTICS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'DOSESPERSTANDARDTHERAPY' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDTHERAPEUTICS], 0))
    WHEN 'STANDARDDOSESPERDISPENSEDPERSON' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDDAILYDOSES], 0))
            / SUM(IIF([PHARMACYSTANDARDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYSTANDARDSITEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYSTANDARDDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYSTANDARDDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYPERCENTDISPENSEDSTANDARD' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [PHARMACYSTANDARDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYCONTROLLEDDAILYDOSES' THEN
        1000
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYCONTROLLEDTHERAPEUTICS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDTHERAPEUTICS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'DOSESPERCONTROLLEDTHERAPY' THEN
        SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDDAILYDOSES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDTHERAPEUTICS], 0))
    WHEN 'CONTROLLEDDOSESPERDISPENSEDPERSON' THEN
        1000
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDDAILYDOSES], 0))
            / SUM(IIF([PHARMACYCONTROLLEDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
    WHEN 'PHARMACYCONTROLLEDSITEDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDSITEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYCONTROLLEDDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PHARMACYCONTROLLEDDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PHARMACYPERCENTDISPENSEDCONTROLLED' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [PHARMACYCONTROLLEDTHERAPEUTICS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PRIMARYCAREPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PRIMARYCAREPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PRIMARYCAREPROVIDERDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PRIMARYCAREPROVIDERDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERPROVIDER' THEN
        SUM(IIF([CORNERCASE] = 'U', [PRIMARYCAREPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [PRIMARYCAREPROVIDERDAYS], 0))
    WHEN 'PRIMARYCAREDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PRIMARYCAREDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PRIMARYCAREPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [PRIMARYCAREPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'ANESTHESIOLOGYPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [ANESTHESIOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'ANESTHESIOLOGISTSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [ANESTHESIOLOGISTSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERANESTHESIOLOGIST' THEN
        SUM(IIF([CORNERCASE] = 'U', [ANESTHESIOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [ANESTHESIOLOGISTSDAYS], 0))
    WHEN 'ANESTHESIOLOGYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [ANESTHESIOLOGYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'ANESTHESIOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [ANESTHESIOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CONSULTPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CONSULTPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CONSULTPROVIDERDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CONSULTPROVIDERSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERCONSULT' THEN
        SUM(IIF([CORNERCASE] = 'U', [CONSULTPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [CONSULTPROVIDERSDAYS], 0))
    WHEN 'CONSULTDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [CONSULTDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'CONSULTPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [CONSULTPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'GENERALPRACTICEPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [GENERALPRACTICEPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'GENERALPRACTITIONERSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [GENERALPRACTITIONERSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERGENERALPRACTITIONER' THEN
        SUM(IIF([CORNERCASE] = 'U', [GENERALPRACTICEPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [GENERALPRACTITIONERSDAYS], 0))
    WHEN 'GENERALPRACTICEDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [GENERALPRACTICEDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'GENERALPRACTICEPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [GENERALPRACTICEPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'OBSTETRICPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [OBSTETRICPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'OBSTETRICIANSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [OBSTETRICIANSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPEROBSTETRICIAN' THEN
        SUM(IIF([CORNERCASE] = 'U', [OBSTETRICPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [OBSTETRICIANSDAYS], 0))
    WHEN 'OBSTETRICDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [OBSTETRICDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'OBSTETRICPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [OBSTETRICPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PATHOLOGYPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PATHOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PATHOLOGISTSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PATHOLOGISTSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERPATHOLOGIST' THEN
        SUM(IIF([CORNERCASE] = 'U', [PATHOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [PATHOLOGISTSDAYS], 0))
    WHEN 'PATHOLOGYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [PATHOLOGYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PATHOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [PATHOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'RADIOLOGYPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [RADIOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'RADIOLOGISTSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [RADIOLOGISTSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERRADIOLOGIST' THEN
        SUM(IIF([CORNERCASE] = 'U', [RADIOLOGYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [RADIOLOGISTSDAYS], 0))
    WHEN 'RADIOLOGYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [RADIOLOGYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'RADIOLOGYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [RADIOLOGYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SPECIALTYPROCEDURES' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SPECIALTYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SPECIALISTSDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SPECIALISTSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERSPECIALIST' THEN
        SUM(IIF([CORNERCASE] = 'U', [SPECIALTYPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [SPECIALISTSDAYS], 0))
    WHEN 'SPECIALTYDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SPECIALTYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SPECIALTYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [SPECIALTYPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SURGICALPROCEDURES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SURGICALPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SURGEONSDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SURGEONSDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'PROCEDURESPERSURGEON' THEN
        SUM(IIF([CORNERCASE] = 'U', [SURGICALPROCEDURES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [SURGEONSDAYS], 0))
    WHEN 'SURGERYDAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SURGERYDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SURGERYPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [SURGICALPROCEDURES] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SUPPORTIVELIVINGDAYS' THEN
        ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SUPPORTIVELIVINGADMISSIONS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGADMISSIONS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SUPPORTIVELIVINGDISCHARGES' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGDISCHARGES], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SUPPORTIVELIVINGLENGTHOFSTAY' THEN
        SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGDAYS], 0))
            / (ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGDISCHARGES], 0)))
    WHEN 'SUPPORTIVELIVINGSTAYS' THEN
        1000
            * ATTR(1 + DATEDIFF('day', [CENSUSSTART], [CENSUSEND]))
            * SUM(IIF([CORNERCASE] = 'U', [SUPPORTIVELIVINGSTAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    WHEN 'SUPPORTIVELIVINGPERCENTUTILIZATION' THEN
        100
            * SUM(IIF([CORNERCASE] = 'U' AND [SUPPORTIVELIVINGSTAYS] > 0, [DURATIONDAYS], 0))
            / SUM(IIF([CORNERCASE] = 'U', [DURATIONDAYS], 0))
    ELSE
        0.0
END