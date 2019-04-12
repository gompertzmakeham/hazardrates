SELECT
CASE [hazardrate]
    WHEN 'INTERVALDECEASED' THEN
        1000.0
    WHEN 'INTERVALEMIGRATE' THEN
        30.0
    WHEN 'LIVENEWBORNS' THEN
        200.0
    WHEN 'AMBULATORYMINUTES' THEN
        24.0
    WHEN 'AMBULATORYVISITS' THEN
        2.0
    WHEN 'AMBULATORYLENGTHOFSTAY' THEN
        12.0
    WHEN 'AMBULATORYSITEDAYS' THEN
        2.0
    WHEN 'AMBULATORYDAYS' THEN
        2.0
    WHEN 'AMBULATORYPERCENTUTILIZATION' THEN
        100.0
    WHEN 'AMBULATORYPRIVATEMINUTES' THEN
        24.0
    WHEN 'AMBULATORYPRIVATEVISITS' THEN
        2.0
    WHEN 'AMBULATORYPRIVATELENGTHOFSTAY' THEN
        12.0
    WHEN 'AMBULATORYPRIVATESITEDAYS' THEN
        2.0
    WHEN 'AMBULATORYPRIVATEDAYS' THEN
        2.0
    WHEN 'AMBULATORYPRIVATEPERCENTUTILIZATION' THEN
        100.0
    WHEN 'AMBULATORYWORKMINUTES' THEN
        250.0
    WHEN 'AMBULATORYWORKVISITS' THEN
        100.0
    WHEN 'AMBULATORYWORKLENGTHOFSTAY' THEN
        12.0
    WHEN 'AMBULATORYWORKSITEDAYS' THEN
        100.0
    WHEN 'AMBULATORYWORKDAYS' THEN
        100.0
    WHEN 'AMBULATORYWORKPERCENTUTILIZATION' THEN
        5.0
    WHEN 'INPATIENTDAYS' THEN
        30.0
    WHEN 'INPATIENTADMISSIONS' THEN
        1000.0
    WHEN 'INPATIENTDISCHARGES' THEN
        1000.0
    WHEN 'INPATIENTLENGTHOFSTAY' THEN
        60.0
    WHEN 'INPATIENTSTAYS' THEN
        1000.0
    WHEN 'INPATIENTPERCENTUTILIZATION' THEN
        100.0
    WHEN 'INPATIENTPRIVATEDAYS' THEN
        30.0
    WHEN 'INPATIENTPRIVATEADMISSIONS' THEN
        1000.0
    WHEN 'INPATIENTPRIVATEDISCHARGES' THEN
        1000.0
    WHEN 'INPATIENTPRIVATELENGTHOFSTAY' THEN
        60.0
    WHEN 'INPATIENTPRIVATESTAYS' THEN
        1000.0
    WHEN 'INPATIENTPRIVATEPERCENTUTILIZATION' THEN
        100.0
    WHEN 'INPATIENTWORKDAYS' THEN
        0.1
    WHEN 'INPATIENTWORKADMISSIONS' THEN
        5.0
    WHEN 'INPATIENTWORKDISCHARGES' THEN
        5.0
    WHEN 'INPATIENTWORKLENGTHOFSTAY' THEN
        60.0
    WHEN 'INPATIENTWORKSTAYS' THEN
        5.0
    WHEN 'INPATIENTWORKPERCENTUTILIZATION' THEN
        0.5
    WHEN 'CAREMANAGERDAYS' THEN
        365.0
    WHEN 'CAREMANAGERALLOCATIONS' THEN
        1000.0
    WHEN 'CAREMANAGERRELEASES' THEN
        1000.0
    WHEN 'CAREMANAGERLENGTH' THEN
        5.0
    WHEN 'CAREMANAGERS' THEN
        5.0
    WHEN 'CAREMANAGERPERCENTUTILIZATION' THEN
        100.0
    WHEN 'HOMECARESERVICES' THEN
        25.0
    WHEN 'HOMECAREVISITS' THEN
        25.0
    WHEN 'SERVICESPERVISIT' THEN
        5.0
    WHEN 'HOMECAREDAYS' THEN
        25.0
    WHEN 'HOMECAREPERCENTUTILIZATION' THEN
        100.0
    WHEN 'HOMECAREPROFESSIONALSERVICES' THEN
        25.0
    WHEN 'HOMECAREPROFESSIONALVISITS' THEN
        25.0
    WHEN 'PROFESSIONALSERVICESPERVISIT' THEN
        5.0
    WHEN 'HOMECAREPROFESSIONALDAYS' THEN
        25.0
    WHEN 'HOMECAREPROFESSIONALPERCENTUTILIZATION' THEN
        100.0
    WHEN 'HOMECARETRANSITIONSERVICES' THEN
        5.0
    WHEN 'HOMECARETRANSITIONVISITS' THEN
        2.0
    WHEN 'TRANSITIONSERVICESPERVISIT' THEN
        5.0
    WHEN 'HOMECARETRANSITIONDAYS' THEN
        2.0
    WHEN 'HOMECARETRANSITIONPERCENTUTILIZATION' THEN
        100.0
    WHEN 'LABORATORYASSAYS' THEN
        100.0
    WHEN 'LABORATORYSITEDAYS' THEN
        5.0
    WHEN 'ASSAYSPERCOLLECTION' THEN
        50.0
    WHEN 'LABORATORYDAYS' THEN
        5.0
    WHEN 'LABORATORYPERCENTUTILIZATION' THEN
        100.0
    WHEN 'LONGTERMCAREDAYS' THEN
        365.0
    WHEN 'LONGTERMCAREADMISSIONS' THEN
        250.0
    WHEN 'LONGTERMCAREDISCHARGES' THEN
        250.0
    WHEN 'LONGTERMCARELENGTHOFSTAY' THEN
        25.0
    WHEN 'LONGTERMCARESTAYS' THEN
        1000.0
    WHEN 'LONGTERMCAREPERCENTUTILIZATION' THEN
        100.0
    WHEN 'PHARMACYDAILYDOSES' THEN
        10.0
    WHEN 'PHARMACYTHERAPEUTICS' THEN
        200.0
    WHEN 'DOSESPERTHERAPY' THEN
        90.0
    WHEN 'ALLDOSESPERDISPENSEDPERSON' THEN
        10.0
    WHEN 'PHARMACYSITEDAYS' THEN
        30.0
    WHEN 'PHARMACYDAYS' THEN
        30.0
    WHEN 'PHARMACYPERCENTDISPENSED' THEN
        100.0
    WHEN 'PHARMACYSTANDARDDAILYDOSES' THEN
        10.0
    WHEN 'PHARMACYSTANDARDTHERAPEUTICS' THEN
        200.0
    WHEN 'DOSESPERSTANDARDTHERAPY' THEN
        90.0
    WHEN 'STANDARDDOSESPERDISPENSEDPERSON' THEN
        10.0
    WHEN 'PHARMACYSTANDARDSITEDAYS' THEN
        30.0
    WHEN 'PHARMACYSTANDARDDAYS' THEN
        30.0
    WHEN 'PHARMACYPERCENTDISPENSEDSTANDARD' THEN
        100.0
    WHEN 'PHARMACYCONTROLLEDDAILYDOSES' THEN
        100.0
    WHEN 'PHARMACYCONTROLLEDTHERAPEUTICS' THEN
        2500.0
    WHEN 'DOSESPERCONTROLLEDTHERAPY' THEN
        90.0
    WHEN 'CONTROLLEDDOSESPERDISPENSEDPERSON' THEN
        2500.0
    WHEN 'PHARMACYCONTROLLEDSITEDAYS' THEN
        2500.0
    WHEN 'PHARMACYCONTROLLEDDAYS' THEN
        2500.0
    WHEN 'PHARMACYPERCENTDISPENSEDCONTROLLED' THEN
        20.0
    WHEN 'PRIMARYCAREPROCEDURES' THEN
        20.0
    WHEN 'PRIMARYCAREPROVIDERDAYS' THEN
        20.0
    WHEN 'PROCEDURESPERPROVIDER' THEN
        2.0
    WHEN 'PRIMARYCAREDAYS' THEN
        20.0
    WHEN 'PRIMARYCAREPERCENTUTILIZATION' THEN
        100.0
    WHEN 'ANESTHESIOLOGYPROCEDURES' THEN
        250.0
    WHEN 'ANESTHESIOLOGISTSDAYS' THEN
        250.0
    WHEN 'PROCEDURESPERANESTHESIOLOGIST' THEN
        5.0
    WHEN 'ANESTHESIOLOGYDAYS' THEN
        250.0
    WHEN 'ANESTHESIOLOGYPERCENTUTILIZATION' THEN
        5.0
    WHEN 'CONSULTPROCEDURES' THEN
        5.0
    WHEN 'CONSULTPROVIDERDAYS' THEN
        1000.0
    WHEN 'PROCEDURESPERCONSULT' THEN
        5.0
    WHEN 'CONSULTDAYS' THEN
        1000.0
    WHEN 'CONSULTPERCENTUTILIZATION' THEN
        50.0
    WHEN 'GENERALPRACTICEPROCEDURES' THEN
        10.0
    WHEN 'GENERALPRACTITIONERSDAYS' THEN
        10.0
    WHEN 'PROCEDURESPERGENERALPRACTITIONER' THEN
        1.5
    WHEN 'GENERALPRACTICEDAYS' THEN
        10.0
    WHEN 'GENERALPRACTICEPERCENTUTILIZATION' THEN
        100.0
    WHEN 'OBSTETRICPROCEDURES' THEN
        1500.0
    WHEN 'OBSTETRICIANSDAYS' THEN
        1000.0
    WHEN 'PROCEDURESPEROBSTETRICIAN' THEN
        5.0
    WHEN 'OBSTETRICDAYS' THEN
        1000.0
    WHEN 'OBSTETRICPERCENTUTILIZATION' THEN
        20
    WHEN 'PATHOLOGYPROCEDURES' THEN
        500.0
    WHEN 'PATHOLOGISTSDAYS' THEN
        500.0
    WHEN 'PROCEDURESPERPATHOLOGIST' THEN
        2.0
    WHEN 'PATHOLOGYDAYS' THEN
        500.0
    WHEN 'PATHOLOGYPERCENTUTILIZATION' THEN
        20.0
    WHEN 'RADIOLOGYPROCEDURES' THEN
        2.0
    WHEN 'RADIOLOGISTSDAYS' THEN
        2.0
    WHEN 'PROCEDURESPERRADIOLOGIST' THEN
        2.0
    WHEN 'RADIOLOGYDAYS' THEN
        2.0
    WHEN 'RADIOLOGYPERCENTUTILIZATION' THEN
        50.0
    WHEN 'SPECIALTYPROCEDURES' THEN
        10.0
    WHEN 'SPECIALISTSDAYS' THEN
        5.0
    WHEN 'PROCEDURESPERSPECIALIST' THEN
        5.0
    WHEN 'SPECIALTYDAYS' THEN
        5.0
    WHEN 'SPECIALTYPERCENTUTILIZATION' THEN
        100.0
    WHEN 'SURGICALPROCEDURES' THEN
        500.0
    WHEN 'SURGEONSDAYS' THEN
        500.0
    WHEN 'PROCEDURESPERSURGEON' THEN
        1.5
    WHEN 'SURGERYDAYS' THEN
        500.0
    WHEN 'SURGERYPERCENTUTILIZATION' THEN
        20.0
    WHEN 'SUPPORTIVELIVINGDAYS' THEN
        90.0
    WHEN 'SUPPORTIVELIVINGADMISSIONS' THEN
        200.0
    WHEN 'SUPPORTIVELIVINGDISCHARGES' THEN
        200.0
    WHEN 'SUPPORTIVELIVINGLENGTHOFSTAY' THEN
        25.0
    WHEN 'SUPPORTIVELIVINGSTAYS' THEN
        500.0
    WHEN 'SUPPORTIVELIVINGPERCENTUTILIZATION' THEN
        100.0
    ELSE
        [upperlimit]
END