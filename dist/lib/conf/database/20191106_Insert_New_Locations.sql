
INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SMVN4',
    '01400360',
    'MARFC',
    'NJ',
    NULL,
    'PETERS BROOK AT MERCER ST AT SOMERVILLE NJ',
    40.575,
    -74.615833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SMVN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LSMN6',
    '01350480',
    'NERFC',
    'NY',
    NULL,
    'LITTLE SCHOHARIE CREEK NEAR MIDDLEBURGH NY',
    42.5797,
    -74.3067
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSMN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NBHN6',
    '01350212',
    'NERFC',
    'NY',
    NULL,
    'SCHOHARIE CREEK NEAR NORTH BLENHEIM NY',
    42.4725,
    -74.4411
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NBHN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EVCN6',
    '04214060',
    'NERFC',
    'NY',
    NULL,
    'BIG SISTER CREEK AT EVANS CENTER NY',
    42.656667,
    -79.035556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EVCN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HMBN6',
    '0421422210',
    'NERFC',
    'NY',
    NULL,
    'EIGHTEENMILE CREEK AT HAMBURG NY',
    42.706667,
    -78.849167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMBN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12211232,
    'NW806',
    '04127885',
    NULL,
    'MI',
    '4070001',
    'ST. MARYS RIVER AT SAULT STE. MARIE~ ONTARIO',
    46.49138,
    -84.305
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW806'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SVRN6',
    '04213394',
    'OHRFC',
    'NY',
    NULL,
    'SILVER CREEK AT US ROUTE 20 AT SILVER CREEK NY',
    42.543889,
    -79.164722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SVRN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WFLN6',
    '04213319',
    'OHRFC',
    'NY',
    NULL,
    'CHAUTAUQUA CREEK BELOW WESTFIELD NY',
    42.330833,
    -79.59
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WFLN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WNCN6',
    '04213401',
    'OHRFC',
    'NY',
    NULL,
    'WALNUT CREEK AT US ROUTE 20 AT SILVER CREEK NY',
    42.538333,
    -79.169444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WNCN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UNVN6',
    '01368050',
    'NERFC',
    'NY',
    NULL,
    'WALLKILL RIVER AT OIL CITY ROAD NEAR UNIONVILLE NY',
    41.2878,
    -74.5343
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UNVN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRBO1',
    '03115917',
    'OHRFC',
    'OH',
    NULL,
    'TUSCARAWAS RIVER ABOVE BARBERTON OH',
    41.02,
    -81.5602
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRBO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CEDO1',
    '03131965',
    'OHRFC',
    'OH',
    NULL,
    'CEDAR FORK ABOVE BELLVILLE OH',
    40.6177,
    -82.5897
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CEDO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ECAO1',
    '410433081312500',
    'OHRFC',
    'OH',
    NULL,
    'LOCK 1 OUTLET O&E CANAL OH',
    41.075833,
    -81.523611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ECAO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GMLO1',
    '04208347',
    'OHRFC',
    'OH',
    NULL,
    'MILL CREEK AT GARFIELD PKWY AT GARFIELD HEIGHTS OH',
    41.423889,
    -81.637778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GMLO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OLMO1',
    '04201429',
    'OHRFC',
    'OH',
    NULL,
    'UNNAMED TRIB TO W B ROCKY R NEAR OLMSTED FALLS OH',
    41.3833,
    -81.9094
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OLMO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PLUO1',
    '04201423',
    'OHRFC',
    'OH',
    NULL,
    'PLUM CREEK NEAR OLMSTED FALLS OH',
    41.3586,
    -81.9213
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PLUO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PRLO1',
    '04201484',
    'OHRFC',
    'OH',
    NULL,
    'EAST BRANCH ROCKY RIVER NEAR STRONGSVILLE OH',
    41.3344,
    -81.8347
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRLO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPOO1',
    '04201409',
    'OHRFC',
    'OH',
    NULL,
    'UNNAMED TRIBUTARY TO W B ROCKY R NEAR BEREA OH',
    41.3508,
    -81.8861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPOO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SUSO3',
    '14158798',
    'NWRFC',
    'OR',
    NULL,
    'SMITH RIVER ABV TRAIL BRDG RESV NR BELKNAP SPRINGS',
    44.29,
    -122.0486
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SUSO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BLIS1',
    '02135501',
    'SERFC',
    'SC',
    NULL,
    'BLACK RIVER AT I-95 NEAR MANNINGSC',
    33.8339,
    -80.1315
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLIS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAYS1',
    '021695075',
    'SERFC',
    'SC',
    NULL,
    'CONGAREE RIVER BELOW CAYCESC',
    33.932222,
    -81.017222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAYS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KELS1',
    '02131455',
    'SERFC',
    'SC',
    NULL,
    'LITTLE LYNCHES RIVER NEAR KERSHAW SC',
    34.5511,
    -80.5466
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KELS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LYOS1',
    '02131510',
    'SERFC',
    'SC',
    NULL,
    'LYNCHES RIVER AT I-95 ABOVE OLANTASC',
    34.038,
    -79.9861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LYOS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RBGS1',
    '021695045',
    'SERFC',
    'SC',
    NULL,
    'TRIB TO ROCKY BRANCH AB GERVAIS ST AT COLUMBIA SC',
    34.006667,
    -81.021667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RBGS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17636636,
    'SCXC1',
    '11136600',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    34.8166465759277,
    -119.562072753906
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCXC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDAT1',
    '03469251',
    'LMRFC',
    'TN',
    NULL,
    'WEST PRONG LITTLE PIGEON R NR GATLINBURG TN',
    35.699444,
    -83.527222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDAT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GUBT2',
    '08167200',
    'WGRFC',
    'TX',
    NULL,
    'GUADALUPE RV AT FM 474 NR BERGHEIM TX',
    29.893333,
    -98.669722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GUBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LRTT2',
    '08151100',
    'WGRFC',
    'TX',
    NULL,
    'LLANO RV AT CR 102 NR LLANO TX',
    30.727222,
    -98.814444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRTT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MEPT2',
    '08178980',
    'WGRFC',
    'TX',
    NULL,
    'MEDINA RV ABV ENGLISH CRSG NR PIPE CREEK TX',
    29.694444,
    -98.979444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MEPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NPBT2',
    '08178861',
    'WGRFC',
    'TX',
    NULL,
    'N PRONG MEDINA RV AT BREWINGTON CK NR MEDINA TX',
    29.875278,
    -99.348611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NPBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PEWT2',
    '08174550',
    'WGRFC',
    'TX',
    NULL,
    'PEACH CREEK AT HWY 90 NR WAELDER TX',
    29.685556,
    -97.230556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PEWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRMT2',
    '08197936',
    'WGRFC',
    'TX',
    NULL,
    'SABINAL RV BL MILL CK NR VANDERPOOL TX',
    29.718889,
    -99.548611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WART2',
    '08157540',
    'WGRFC',
    'TX',
    NULL,
    'WALLER CK AT RED RIVER ST AUSTIN TX',
    30.271389,
    -97.735556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WART2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WEET2',
    '08178638',
    'WGRFC',
    'TX',
    NULL,
    'W ELM CK AT ENCINO RIO AT SAN ANTONIO TX',
    29.629167,
    -98.448611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WPMT2',
    '08178871',
    'WGRFC',
    'TX',
    NULL,
    'W PRONG MEDINA RV AT CARPENTER CK RD NR MEDINA TX',
    29.78,
    -99.379167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10351594,
    'NW760',
    '10147100',
    'CBRFC',
    'UT',
    '16020201',
    'SUMMIT CREEK ABV SUMMIT CR CANAL NR SANTAQUIN UT',
    39.94888,
    -111.77944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW760'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'STDC3',
    '01209788',
    'NERFC',
    'CT',
    NULL,
    'STAMFORD HURRICANE BARRIER AT STAMFORD CT',
    41.037222,
    -73.534722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STDC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889650,
    'NW607',
    '11337190',
    'CNRFC',
    'CA',
    '18040003',
    'SAN JOAQUIN R A JERSEY POINT CA',
    38.05083,
    -121.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW607'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBSP1',
    '03021890',
    'OHRFC',
    'PA',
    NULL,
    'FRENCH CREEK AT CAMBRIDGE SPRINGS PA',
    41.807167,
    -80.063278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBSP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLLK2',
    NULL,
    'OHRFC',
    'KY',
    NULL,
    'PRECIPITATION SITE AT WILLARD KY',
    38.18333,
    -82.89333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLLK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WHLR1',
    NULL,
    'NERFC',
    'RI',
    NULL,
    'WATCH HILL COVE TIDE GAGE WESTERLY RI',
    41.31056,
    -71.85833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WHLR1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CORP4',
    '50038300',
    NULL,
    'PR',
    NULL,
    'RIO COROZAL AT COROZAL PR',
    18.34458,
    -66.32233
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CORP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SNAI2',
    '05543010',
    'NCRFC',
    'IL',
    NULL,
    'ILLINOIS RIVER AT SENECA IL',
    41.299722,
    -88.614167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNAI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LAKI3',
    '04093250',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET RIVER NEAR LAKE STATION IN',
    41.5725,
    -87.2994
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LAKI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WEBH1',
    '16068000',
    'AKRFC',
    'HI',
    NULL,
    'EB OF NF WAILUA RIVER NR LIHUE KAUAI HI',
    22.068778,
    -159.415167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEBH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WNIH1',
    '16108000',
    'AKRFC',
    'HI',
    NULL,
    'WAINIHA RIVER NR HANALEI KAUAI HI',
    22.135889,
    -159.557861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WNIH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCOI4',
    '05489490',
    'NCRFC',
    'IA',
    NULL,
    'BEAR CREEK AT OTTUMWA IA',
    41.014458,
    -92.462409
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCOI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SFTG1',
    '02207200',
    'SERFC',
    'GA',
    NULL,
    'SWIFT CREEK NEAR LITHONIA GA',
    33.748056,
    -84.083889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFTG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WWKH1',
    '16518000',
    'AKRFC',
    'HI',
    NULL,
    'WEST WAILUAIKI STREAM NEAR KEANAE MAUI HI',
    20.814361,
    -156.142972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WWKH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CNPM5',
    NULL,
    'NCRFC',
    'MN',
    NULL,
    'PRAIRIE CREEK NEAR CANNON FALLS MN',
    44.47167,
    -93.00361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNPM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MRGA2',
    '15515060',
    'AKRFC',
    'AK',
    NULL,
    'MARGUERITE C AB EMMA C NR HEALY AK',
    64.008888,
    -148.725833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MRGA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MTKM5',
    NULL,
    'NCRFC',
    'MN',
    NULL,
    'LAKE MINNETONKA ABV GRAYS BAY OTLT IN MINNETONKA',
    44.95333,
    -93.48722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTKM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NRCA2',
    '15580095',
    'AKRFC',
    'AK',
    NULL,
    'NIUKLUK R AB MELSING C AT COUNCIL AK',
    64.891944,
    -163.67
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NRCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PHLA2',
    '15478040',
    'AKRFC',
    'AK',
    NULL,
    'PHELAN C NR PAXSON AK',
    63.240833,
    -145.4675
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHLA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SADA2',
    '15742980',
    'AKRFC',
    'AK',
    NULL,
    'SADIE C 1.3 MI AB MOUTH NR KOTZEBUE AK',
    66.817222,
    -162.5125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SADA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SALA2',
    '15484000',
    'AKRFC',
    'AK',
    NULL,
    'SALCHA R NR SALCHAKET AK',
    64.471528,
    -146.928056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SALA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SGRA2',
    '15908000',
    'AKRFC',
    'AK',
    NULL,
    'SAGAVANIRKTOK R NR PUMP STA 3 AK',
    69.015,
    -148.817222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ALEA2',
    '15129120',
    'AKRFC',
    'AK',
    NULL,
    'ALSEK R AT DRY BAY NR YAKUTAT AK',
    59.192806,
    -138.333917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALEA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CKTA2',
    '15056500',
    'AKRFC',
    'AK',
    NULL,
    'CHILKAT R NR KLUKWAN AK',
    59.415,
    -135.933056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CKTA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCKA2',
    '15072000',
    'AKRFC',
    'AK',
    NULL,
    'FISH C NR KETCHIKAN AK',
    55.391944,
    -131.193889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GOVA2',
    '15058700',
    'AKRFC',
    'AK',
    NULL,
    'GOVERNMENT C NR KETCHIKAN AK',
    55.342778,
    -131.698889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GOVA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCCA2',
    '15086225',
    'AKRFC',
    'AK',
    NULL,
    'HATCHERY C AT FOREST SVC RD 23 NR COFFMAN COVE AK',
    55.9086,
    -132.9308
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'IRVA2',
    '15087700',
    'AKRFC',
    'AK',
    NULL,
    'INDIAN R AT SITKA AK',
    57.053333,
    -135.314444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IRVA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VEDP4',
    '50063800',
    NULL,
    'PR',
    NULL,
    'RIO ESPIRITU SANTO NR RIO GRANDE PR',
    18.35991,
    -65.814
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VEDP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YBUP4',
    '50083500',
    NULL,
    'PR',
    NULL,
    'RIO GUAYANES NR YABUCOA PR',
    18.05889,
    -65.90111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YBUP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UTXP4',
    '50024950',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE ARECIBO BLW UTUADO PR',
    18.30209,
    -66.7041
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UTXP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9147710,
    'HRKS1',
    '02131472',
    'SERFC',
    'SC',
    NULL,
    'HANGING ROCK CREEK NR KERSHAW SC',
    34.51611,
    -80.58306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRKS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TOAP4',
    '50046000',
    NULL,
    'PR',
    NULL,
    'RIO DE LA PLATA AT HWY 2 NR TOA ALTA PR',
    18.41159,
    -66.26096
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TOAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ECLO1',
    '410121081330300',
    'OHRFC',
    'OH',
    NULL,
    'LONG LAKE FEEDER O&E CANAL OH',
    41.022556,
    -81.55067
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ECLO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ITAA2',
    '15129280',
    'AKRFC',
    'AK',
    NULL,
    'ITALIO R AB MOUTH NR YAKUTAT AK',
    59.302202,
    -139.04569
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ITAA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LSCA2',
    '15068040',
    'AKRFC',
    'AK',
    NULL,
    'LEASK CREEK AT SHELTER COVE NR KETCHIKAN AK',
    55.51944,
    -131.52277
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MNDA2',
    '15052500',
    'AKRFC',
    'AK',
    NULL,
    'MENDENHALL R NR AUKE BAY AK',
    58.429722,
    -134.572778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCCC1',
    '11169860',
    'CNRFC',
    'CA',
    NULL,
    'COYOTE C BL COYOTE RES NR SAN MARTIN CA',
    37.123056,
    -121.551667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCCC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RROC1',
    '11467270',
    'CNRFC',
    'CA',
    NULL,
    'RUSSIAN R A HIGHWAY 1 BRIDGE NR JENNER CA',
    38.433889,
    -123.101111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RROC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLWH1',
    '16400000',
    'AKRFC',
    'HI',
    NULL,
    'HALAWA STREAM NEAR HALAWA MOLOKAI HI',
    21.1555,
    -156.761972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLWH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLEH1',
    '16103000',
    'AKRFC',
    'HI',
    NULL,
    'HANALEI RIVER NR HANALEI KAUAI HI',
    22.179583,
    -159.466389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLEH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLSH1',
    '16343100',
    'AKRFC',
    'HI',
    NULL,
    'HELEMANO STR AT JOSEPH LEONG HWY HALEIWA OAHUHI',
    21.578611,
    -158.102778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KNFH1',
    '16200000',
    'AKRFC',
    'HI',
    NULL,
    'NF KAUKONAHUA STR ABV RB NR WAHIAWA OAHU HI',
    21.516278,
    -157.945306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNFH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KLSH1',
    '16304200',
    'AKRFC',
    'HI',
    NULL,
    'KALUANUI STREAM NR PUNALUU OAHU HI',
    21.586111,
    -157.908056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KLSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VCPL1',
    '07387040',
    'LMRFC',
    'LA',
    NULL,
    'VERMILION BAY NEAR CYPREMORT POINT LA',
    29.713056,
    -91.880278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VCPL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MUDM3',
    '01104683',
    'NERFC',
    'MA',
    NULL,
    'MUDDY RIVER AT BROOKLINE MA',
    42.337222,
    -71.111667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MUDM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4518742,
    'FABP1',
    '01553990',
    'MARFC',
    'PA',
    NULL,
    'SUSQUEHANNA RIVER ABOVE DAM AT SUNBURY PA',
    40.85278,
    -76.80278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FABP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRCN4',
    '01460440',
    'MARFC',
    'NJ',
    NULL,
    'DELAWARE AND RARITAN CANAL AT PORT MERCER NJ',
    40.30444,
    -74.685
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRCN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22724061,
    'NSLG1',
    '02196999',
    'SERFC',
    'GA',
    NULL,
    'SAVANNAH RV ABOVE NEW SAV. LOCK AND DAM',
    33.37306,
    -81.94222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSLG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20107197,
    'FWDS1',
    '021989791',
    'SERFC',
    'SC',
    NULL,
    'LITTLE BACK RIVER AT F&W DOCK NEAR LIMEHOUSE SC',
    32.17056,
    -81.11833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FWDS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9735208,
    'FCHS1',
    '02147403',
    'SERFC',
    'SC',
    NULL,
    'FISHING CREEK BELOW FORT LAWN SC',
    34.63667,
    -80.9275
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCHS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9731398,
    'COXS1',
    '0214676115',
    'SERFC',
    'SC',
    NULL,
    'MCALPINE CREEK AT SR2964 NR CAMP COX SC',
    35.04083,
    -80.89167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COXS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TENF1',
    '02291673',
    'SERFC',
    'FL',
    NULL,
    'TENMILE CANAL AT CONTROL NEAR ESTERO FL',
    26.50689,
    -81.85442
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TENF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SIXF1',
    '02291669',
    'SERFC',
    'FL',
    NULL,
    'SIXMILE CYPRESS CREEK NEAR FORT MYERS  FL',
    26.52203,
    -81.85442
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SIXF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLOF1',
    '02286400',
    'SERFC',
    'FL',
    NULL,
    'MIAMI CANAL AT S-354 AND S-3 AT LAKE HARBOR FLA',
    26.695,
    -80.806944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCKF1',
    '02257000',
    'SERFC',
    'FL',
    NULL,
    'FISHEATING CREEK AT LAKEPORT FL',
    26.962222,
    -81.118056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCIF1',
    '02313272',
    'SERFC',
    'FL',
    NULL,
    'WITHLACOOCHEE R AT CHAMBERS IS NEAR YANKEETOWN FL',
    29.000833,
    -82.762222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCIF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10243117,
    'TLKF1',
    '022929176',
    'SERFC',
    'FL',
    NULL,
    'TELEGRAPH CREEK AT STATE HIGHWAY AT OLGA FL',
    26.72736,
    -81.70345
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TLKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WOLF1',
    '02274505',
    'SERFC',
    'FL',
    NULL,
    'WOLFF CREEK NR OKEECHOBEEFL',
    27.28056,
    -80.82417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WOLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WSKF1',
    '02293230',
    'SERFC',
    'FL',
    NULL,
    'WHISKEY CREEK AT FT. MYERS FL',
    26.57489,
    -81.89189
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SDAF1',
    '02290769',
    'SERFC',
    'FL',
    NULL,
    'CANAL 111 AT S-18-C NEAR FLORIDA CITY FL',
    25.33028,
    -80.52528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SDAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14762345,
    'WBOI2',
    '05551545',
    'NCRFC',
    'IL',
    '7120007',
    'WAUBONSEE CREEK NEAR OSWEGO~ IL',
    41.70139,
    -88.32722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBOI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16918174,
    'BLFF1',
    '02300703',
    'SERFC',
    'FL',
    NULL,
    'BULLFROG CREEK NEAR RIVERVIEW FL',
    27.83472,
    -82.34639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FVJF1',
    '02310286',
    'SERFC',
    'FL',
    NULL,
    'FIVEMILE CR BL SUNCOAST PKWY NR FIVAY JUNCTION FL',
    28.29414,
    -82.55372
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FVJF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21477644,
    'WCVF1',
    '02266205',
    'SERFC',
    'FL',
    NULL,
    'WHITTENHORSE CREEK AT S-411 NEAR VINELAND FL',
    28.39278,
    -81.61111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2232713,
    'FANF1',
    '02323502',
    'SERFC',
    'FL',
    NULL,
    'FANNING SPRINGS NR WILCOX FLA',
    29.58889,
    -82.93333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FANF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6333364,
    'LAWG1',
    '02205522',
    'SERFC',
    'GA',
    NULL,
    'PEW CREEK AT PATTERSON RD NEAR LAWRENCEVILLE GA',
    33.92583,
    -84.03778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LAWG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16906283,
    'CRZF1',
    '02302000',
    'SERFC',
    'FL',
    NULL,
    'CRYSTAL SPRINGS NEAR ZEPHYRHILLS FL',
    28.18194,
    -82.18528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6330544,
    'LAVG1',
    '02208130',
    'SERFC',
    'GA',
    NULL,
    'SHOAL CREEK AT PAPER MILL RD NR LAWRENCEVILLE GA',
    33.94972,
    -83.94833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LAVG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3286312,
    'SNKG1',
    '02337500',
    'SERFC',
    'GA',
    NULL,
    'SNAKE CREEK NEAR WHITESBURG GA',
    33.52944,
    -84.92833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNKG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2045337,
    'BDTG1',
    '02334401',
    'SERFC',
    'GA',
    NULL,
    'CHATTAHOOCHEE RIVER (BUFORD DAM) NR BUFORD GA',
    34.16139,
    -84.07611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BDTG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9840096,
    'FDAN6',
    '04213376',
    'OHRFC',
    'NY',
    NULL,
    'CANADAWAY CREEK AT FREDONIA NY',
    42.45064,
    -79.35044
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FDAN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21976279,
    'MCDN6',
    '04219000',
    'NERFC',
    'NY',
    NULL,
    'ERIE (BARGE) CANAL AT LOCK 30 AT MACEDON NY',
    43.07178,
    -77.29697
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCDN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10176975,
    'HILT1',
    '03418000',
    'OHRFC',
    'TN',
    NULL,
    'ROARING RIVER NEAR HILHAM TN',
    36.34083,
    -85.42639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HILT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2087849,
    'VISK2',
    '03254520',
    'OHRFC',
    'KY',
    NULL,
    'LICKING RIVER AT HWY 536 NEAR ALEXANDRIA KY',
    38.92028,
    -84.44806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VISK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18476455,
    'FCSI3',
    '03352875',
    'OHRFC',
    'IN',
    NULL,
    'FALL CREEK AT 16TH STREET AT INDIANAPOLIS IN',
    39.78878,
    -86.17747
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCSI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PDLM7',
    '07010035',
    'NCRFC',
    'MO',
    NULL,
    'ENGELHOLM CREEK NEAR WELLSTON MO',
    38.68264,
    -90.30289
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PDLM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WBRM7',
    '07010094',
    'NCRFC',
    'MO',
    NULL,
    'GRAMMOND CREEK NEAR WILBUR PARK MO',
    38.56456,
    -90.30944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBRM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PGLM7',
    '07010030',
    'NCRFC',
    'MO',
    NULL,
    'RIVER DES PERES TRIBUTARY AT PAGEDALE MO',
    38.67689,
    -90.31483
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PGLM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14762875,
    'BBMI2',
    '05551675',
    'NCRFC',
    'IL',
    NULL,
    'BLACKBERRY CREEK NEAR MONTGOMERY IL',
    41.74083,
    -88.38333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBMI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12992931,
    'SFFW3',
    '05359500',
    'NCRFC',
    'WI',
    NULL,
    'SOUTH FORK FLAMBEAU RIVER NEAR PHILLIPS WI',
    45.70333,
    -90.61556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFFW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14769044,
    'MGOW3',
    '05544385',
    'NCRFC',
    'WI',
    NULL,
    'MUSKEGO (BIG MUSKEGO) LAKE OUTLET NR WIND LAKE WI',
    42.8525,
    -88.13056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MGOW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19474395,
    'BBJL1',
    '07364203',
    'LMRFC',
    'LA',
    NULL,
    'BAYOU BARTHOLOMEW NW OF JONES LA',
    32.98472,
    -91.69861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBJL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SFRI1',
    '13106500',
    'NWRFC',
    'ID',
    NULL,
    'SALMON RIVER CANAL CO RES NR ROGERSON ID',
    42.212222,
    -114.733333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFRI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GLBI1',
    '13057300',
    'NWRFC',
    'ID',
    NULL,
    'GRAYS LAKE DIV TO BLACKFOOT R BASIN NR WAYAN ID',
    43.005672,
    -111.493758
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLBI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCIM8',
    '05018000',
    'MBRFC',
    'MT',
    NULL,
    'ST. MARY CANAL AT INTAKE NEAR BABB MT',
    48.852778,
    -113.416944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCIM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MLNI1',
    '13087900',
    'NWRFC',
    'ID',
    NULL,
    'MILNER LAKE AT MILNER DAM ID',
    42.52306,
    -114.01333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MLNI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GWSI1',
    '13057132',
    'NWRFC',
    'ID',
    NULL,
    'GREAT WESTERN SPILLBACK NR IDAHO FALLS ID',
    43.60083,
    -112.06194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GWSI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4392783,
    'ICSM7',
    '06893400',
    'MBRFC',
    'MO',
    NULL,
    'INDIAN CREEK AT 103RD ST IN KANSAS CITY MO',
    38.94197,
    -94.6045
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ICSM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4169038,
    'LRRM8',
    '06012500',
    'MBRFC',
    'MT',
    NULL,
    'RED ROCK R BL LIMA RESERVOIR NR MONIDA MT',
    44.65588,
    -112.37122
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRRM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'INLS2',
    '06434505',
    'MBRFC',
    'SD',
    NULL,
    'INLET CANAL ABOVE BELLE FOURCHE RESERVOIRSD',
    44.70094,
    -103.73231
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INLS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2965566,
    'BRMW4',
    '06190540',
    'MBRFC',
    'WY',
    '10070001',
    'Boiling River at Mammoth YNP',
    44.98513,
    -110.68934
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRMW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11628941,
    'WDHK2',
    '03315500',
    'OHRFC',
    'KY',
    NULL,
    'GREEN RIVER AT LOCK 4 AT WOODBURY KY',
    37.18222,
    -86.63
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WDHK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10836370,
    'MRMT2',
    '08180500',
    'WGRFC',
    'TX',
    NULL,
    'MEDINA RV NR RIOMEDINA TX',
    29.498,
    -98.9055
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MRMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8588002,
    'LSNO2',
    '07049000',
    'LMRFC',
    'OK',
    NULL,
    'WAR EAGLE CREEK NEAR HINDSVILLE AR',
    36.2,
    -93.855
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSNO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1439303,
    'MSTT2',
    '08074598',
    'WGRFC',
    'TX',
    NULL,
    'WHITEOAK BAYOU AT MAIN ST HOUSTON TX',
    29.76639,
    -95.35833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MSTT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 210083,
    'ACHT2',
    '08470500',
    'WGRFC',
    'TX',
    NULL,
    'ARROYO COLORADO AT FM 106 RIO HONDO TX',
    26.23528,
    -97.58472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACHT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1440233,
    'LCAT2',
    '08072800',
    'WGRFC',
    'TX',
    NULL,
    'LANGHAM CK NR ADDICKS TX',
    29.83556,
    -95.62556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1653097,
    'ACET2',
    '08211800',
    'WGRFC',
    'TX',
    NULL,
    'SAN DIEGO CK AT ALICE TX',
    27.76664,
    -98.07556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1515443,
    'WSVT2',
    '08067252',
    'WGRFC',
    'TX',
    NULL,
    'TRINITY RV AT WALLISVILLE TX',
    29.81222,
    -94.73111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1562342,
    'PRLT2',
    '08076997',
    'WGRFC',
    'TX',
    NULL,
    'CLEAR CK AT MYKAWA ST NR PEARLAND TX',
    29.59667,
    -95.29722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10836382,
    'LACT2',
    '08180640',
    'WGRFC',
    'TX',
    NULL,
    'MEDINA RV AT LA COSTE TX',
    29.32389,
    -98.81278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LACT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1576378,
    'OBRT2',
    '08079120',
    'WGRFC',
    'TX',
    NULL,
    'OLD BRAZOS RV NR FREEPORT TX',
    28.95083,
    -95.33861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OBRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1468106,
    'POET2',
    '08068090',
    'WGRFC',
    'TX',
    NULL,
    'W FK SAN JACINTO RV ABV LK HOUSTON NR PORTER TX',
    30.14117,
    -95.33822
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'POET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5781319,
    'BCWT2',
    '08158035',
    'WGRFC',
    'TX',
    NULL,
    'BOGGY CK AT WEBBERVILLE RD AUSTIN TX',
    30.26306,
    -97.7125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5522448,
    'ELVT2',
    '08087300',
    'WGRFC',
    'TX',
    NULL,
    'CLEAR FK BRAZOS RV AT ELIASVILLE TX',
    32.96068,
    -98.76677
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10836104,
    'SMMT2',
    '08180800',
    'WGRFC',
    'TX',
    NULL,
    'MEDINA RV NR SOMERSET TX',
    29.26194,
    -98.58111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SMMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5489503,
    'AECT2',
    '08083430',
    'WGRFC',
    'TX',
    NULL,
    'ELM CK AT ABILENE TX',
    32.50731,
    -99.74103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AECT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1515401,
    'WCVT2',
    '08067118',
    'WGRFC',
    'TX',
    NULL,
    'LK CHARLOTTE NR ANAHUAC TX',
    29.86722,
    -94.71472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1509333,
    'HOCT2',
    '08068700',
    'WGRFC',
    'TX',
    NULL,
    'CYPRESS CK AT SHARP RD NR HOCKLEY TX',
    29.92083,
    -95.84
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1279586,
    'CAHT2',
    '08053090',
    'WGRFC',
    'TX',
    NULL,
    'HUTTON BR AT N DENTON DR AT CARROLLTON TX',
    32.95716,
    -96.90712
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAHT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ETEU1',
    '09319000',
    'CBRFC',
    'UT',
    NULL,
    'EPHRAIM TUNNEL NEAR EPHRAIM UT',
    39.32972,
    -111.43083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ETEU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCSU1',
    '10170500',
    'CBRFC',
    'UT',
    NULL,
    'SURPLUS CANAL @ SALT LAKE CITY UT',
    40.72933,
    -111.9295
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCSU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCKW2',
    '03190307',
    'OHRFC',
    'WV',
    NULL,
    'HEDRICKS CREEK TRIBUTARY ABOVE US-19 NEAR HICO WV',
    38.121229,
    -80.996696
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCKW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24092756,
    'WRKO3',
    '11504115',
    'CNRFC',
    'OR',
    NULL,
    NULL,
    42.5815544128418,
    -121.941703796387
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRKO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23957266,
    'JNCW1',
    '12210220',
    'NWRFC',
    'WA',
    NULL,
    'JONES CREEK AT ACME WA',
    48.71861,
    -122.21306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JNCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WADA2',
    '15320100',
    'AKRFC',
    'AK',
    NULL,
    'WADE C TRIB NR CHICKEN AK',
    64.118333,
    -141.553611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WADA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCYP4',
    '50114000',
    NULL,
    'PR',
    NULL,
    'RIO CERRILLOS BLW LAGO CERRILLOS NR PONCE PR',
    18.07285,
    -66.58139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCYP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CNAP4',
    '50061800',
    NULL,
    'PR',
    NULL,
    'RIO CANOVANAS NR CAMPO RICO PR',
    18.31848,
    -65.88891
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CJDP4',
    '50111320',
    NULL,
    'PR',
    NULL,
    'CANAL DE JUANA DIAZ AT PIEDRA AGUZADA NR JUANA DIA',
    18.03995,
    -66.48876
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CJDP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LOEP4',
    '50128905',
    NULL,
    'PR',
    NULL,
    'CANAL DE RIEGO DE LAJAS BLW LAGO LOCO DAM YAUCO PR',
    18.04304,
    -66.88856
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOEP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8273343,
    'CGNC1',
    '11465700',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    38.3735237121582,
    -122.768325805664
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CGNC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9643431,
    'PTRS1',
    '02172001',
    'SERFC',
    'SC',
    NULL,
    'LAKE MOULTRIE NEAR PINOPOLIS SC (TAILRACE)',
    33.24444,
    -79.99167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'QMAP4',
    '50049620',
    NULL,
    'PR',
    NULL,
    'QDA. MARGARITA AT CAPARRA INTER. NR GUAYNABO PR',
    18.41553,
    -66.10336
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QMAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAMP4',
    '50055225',
    NULL,
    'PR',
    NULL,
    'RIO CAGUITAS AT VILLA BLANCA AT CAGUAS PR',
    18.2487,
    -66.02763
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAMP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CIAP4',
    '50035000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE MANATI AT CIALES PR',
    18.32443,
    -66.45995
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CIAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LOGP4',
    '50129254',
    NULL,
    'PR',
    NULL,
    'RIO LOCO AT LAS LATAS NR LA JOYA NR GUANICA PR',
    18.00916,
    -66.8767
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOGP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PANP4',
    '50093000',
    NULL,
    'PR',
    NULL,
    'RIO MARIN NR PATILLAS PR',
    18.03778,
    -66.00972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PANP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16770152,
    'FKUF1',
    '255432081303900',
    'SERFC',
    'FL',
    NULL,
    'FAKA-UNION RIVER NEAR THE MOUTH',
    25.90881,
    -81.51092
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FKUF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PIEP4',
    '50081000',
    NULL,
    'PR',
    NULL,
    'RIO HUMACAO AT LAS PIEDRAS PR',
    18.17406,
    -65.86944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PIEP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RPOP4',
    '50049100',
    NULL,
    'PR',
    NULL,
    'RIO PIEDRAS AT HATO REY PR',
    18.40955,
    -66.06933
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RPOP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAGP4',
    '50055000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE LOIZA AT CAGUAS PR',
    18.24269,
    -66.00959
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAGP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JDZP4',
    '50111330',
    NULL,
    'PR',
    NULL,
    'CANAL DE JUANA DIAZ AT PASO SECO NR SANTA ISABEL',
    18.01433,
    -66.39032
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JDZP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLNP4',
    '50051800',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE LOIZA AT HWY 183 SAN LORENZO PR',
    18.18571,
    -65.96145
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLNP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NAMP4',
    '50044810',
    NULL,
    'PR',
    NULL,
    'RIO GUADIANA NR GUADIANA NARANJITO PR',
    18.3014,
    -66.22858
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NAMP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CABP4',
    '50055380',
    NULL,
    'PR',
    NULL,
    'RIO BAIROA ABV BAIROA CAGUAS PR',
    18.25829,
    -66.04429
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CABP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JAXP4',
    '50026025',
    NULL,
    'PR',
    NULL,
    'RIO CAONILLAS AT PASO PALMA PR',
    18.23142,
    -66.63709
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JAXP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSAP4',
    '50067000',
    NULL,
    'PR',
    NULL,
    'RIO SABANA AT SABANA PR',
    18.33096,
    -65.73101
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BAUP4',
    '50034000',
    NULL,
    'PR',
    NULL,
    'RIO BAUTA NR OROCOVIS PR',
    18.23596,
    -66.45483
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BAUP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COAP4',
    '50106100',
    NULL,
    'PR',
    NULL,
    'RIO COAMO AT HWY 14 AT COAMO PR',
    18.08351,
    -66.35482
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GUAP4',
    '50011200',
    NULL,
    'PR',
    NULL,
    'RIO GUAJATACA BLW LAGO GUAJATACA PR',
    18.40028,
    -66.92778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GUAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MJAP4',
    '50128920',
    NULL,
    'PR',
    NULL,
    'CANAL DE RIEGO DE LAJAS ABV MAJINAS FILT. PLANT PR',
    18.04587,
    -66.94914
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MJAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCAP4',
    '50048565',
    NULL,
    'PR',
    NULL,
    'QUEBRADA SANTA CATALINA NEAR GUAYNABO PR',
    18.40903,
    -66.11847
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MJBP4',
    '50128925',
    NULL,
    'PR',
    NULL,
    'CANAL DE RIEGO DE LAJAS BLW MAJINAS FILT. PLANT PR',
    18.04457,
    -66.95009
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MJBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAPP4',
    '50058350',
    NULL,
    'PR',
    NULL,
    'RIO CANAS AT RIO CANAS PR',
    18.2945,
    -66.04509
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAPP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NGIP4',
    '50075000',
    NULL,
    'PR',
    NULL,
    'RIO ICACOS NR NAGUABO PR',
    18.27722,
    -65.78583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NGIP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCEP4',
    '50113800',
    NULL,
    'PR',
    NULL,
    'RIO CERRILLOS ABV LAGO CERRILLOS NR PONCE PR',
    18.11679,
    -66.60489
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCEP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PRTP4',
    '50114900',
    NULL,
    'PR',
    NULL,
    'RIO PORTUGUES NR TIBES PR',
    18.09988,
    -66.64287
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRTP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDCF1',
    '302341086305600',
    'SERFC',
    'FL',
    NULL,
    'EAST PASS TO CHOCTAWHATCHEE BAY AT DESTIN FL',
    30.39485,
    -86.51557
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDAF1',
    '301116085443000',
    'SERFC',
    'FL',
    NULL,
    'ST ANDREWS BAY NEAR PANAMA CITY FL',
    30.18783,
    -85.74167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SVLI2',
    '05586745',
    'NCRFC',
    'IL',
    NULL,
    'MACOUPIN CREEK AT HWY 111 NR SUMMERVILLE IL',
    39.20444,
    -90.10056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SVLI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23970783,
    'TNFW1',
    '12147470',
    'NWRFC',
    'WA',
    NULL,
    NULL,
    47.7197227478027,
    -121.743194580078
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TNFW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23773365,
    'MUSO3',
    '14158740',
    'NWRFC',
    'OR',
    NULL,
    NULL,
    44.2863883972168,
    -122.036804199219
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MUSO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FBGM5',
    '05387030',
    'NCRFC',
    'MN',
    NULL,
    'CROOKED CREEK AT FREEBURG MN',
    43.61028,
    -91.36083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FBGM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'INCW2',
    '03177480',
    'OHRFC',
    'WV',
    NULL,
    'INDIAN CREEK AT RED SULPHUR SPRINGS WV',
    37.529167,
    -80.770278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INCW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8566741,
    'SORV2',
    '02032515',
    'MARFC',
    'VA',
    NULL,
    'S F RIVANNA RIVER NEAR CHARLOTTESVILLE VA',
    38.10167,
    -78.46083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SORV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ADRS1',
    '02136030',
    'SERFC',
    'SC',
    NULL,
    'BLACK RIVER NEAR ANDREWSSC',
    33.490278,
    -79.544833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ADRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FHGT2',
    '08101310',
    'WGRFC',
    'TX',
    NULL,
    'HOUSE CK AT OLD GEORGETOWN RD NR FT HOOD TX',
    31.16494,
    -97.87819
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FHGT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ARFP4',
    '50029000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE ARECIBO AT CENTRAL CAMBALACHE PR',
    18.45553,
    -66.70245
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ARFP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ARDP4',
    '50028400',
    NULL,
    'PR',
    NULL,
    'RIO TANAMA AT CHARCO HONDO PR',
    18.4141,
    -66.71471
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ARDP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AIBP4',
    '50043000',
    NULL,
    'PR',
    NULL,
    'RIO DE LA PLATA AT PROYECTO LA PLATA PR',
    18.16,
    -66.22889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AIBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AREP4',
    '50027600',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE ARECIBO NR SAN PEDRO PR',
    18.39882,
    -66.69156
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AREP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PODO2',
    '07178645',
    'ABRFC',
    'OK',
    NULL,
    'VERDIGRIS RIVER NEAR WAGONER OK',
    35.95556,
    -95.49417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PODO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NPTG1',
    '02336093',
    'SERFC',
    'GA',
    NULL,
    'N FK PEACHTREE CR TRB AT DRESDEN DR NR ATLANTAGA',
    33.87,
    -84.29472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NPTG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PDCG1',
    '02204010',
    'SERFC',
    'GA',
    NULL,
    'POLE BRIDGE CRK AT FAIRINGTON DR NR LITHONIA GA',
    33.69556,
    -84.14611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PDCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBHI4',
    '06610505',
    'MBRFC',
    'IA',
    NULL,
    'MISSOURI RIVER NEAR COUNCIL BLUFFS IA',
    41.18917,
    -95.8625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBHI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RWLT2',
    '08061480',
    'WGRFC',
    'TX',
    NULL,
    'SQUABBLE CK AT SHORES BLVD NR ROCKWALL TX',
    32.95225,
    -96.47364
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RWLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCDG1',
    '02394682',
    'SERFC',
    'GA',
    NULL,
    'RICHLAND CREEK AT OLD DALLAS RD NEAR DALLAS GA',
    34.078972,
    -84.854444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCDG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SKYI4',
    '06811875',
    'MBRFC',
    'IA',
    NULL,
    'SNAKE CREEK NEAR YORKTOWN IA',
    40.742437,
    -95.129611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SKYI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24537962,
    'MORW1',
    '12115900',
    'NWRFC',
    'WA',
    NULL,
    'CHESTER MORSE LAKE AT CEDAR FALLS WA',
    47.40944,
    -121.72278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MORW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LMXI3',
    '03352690',
    'OHRFC',
    'IN',
    NULL,
    'LAKE MAXINHALL AT INDIANAPOLIS IN',
    39.85333,
    -86.11222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LMXI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BREI3',
    '05517010',
    'NCRFC',
    'IN',
    NULL,
    'YELLOW RIVER NEAR BREMS IN',
    41.30667,
    -86.7375
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BREI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WASM4',
    '04001000',
    'NCRFC',
    'MI',
    NULL,
    'WASHINGTON CREEK AT WINDIGO MI',
    47.92139,
    -89.14583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WASM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CIXM7',
    '06921760',
    'MBRFC',
    'MO',
    NULL,
    'SOUTH GRAND RIVER NEAR CLINTON MO',
    38.37014,
    -93.85811
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CIXM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPLM7',
    '06936530',
    'NCRFC',
    'MO',
    NULL,
    'SPANISH LAKE TRIB. NR BLACK JACK MO',
    38.80086,
    -90.21625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPLM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SBAM6',
    '07288847',
    'LMRFC',
    'MS',
    NULL,
    'STEELE BAYOU NR GLEN ALLAN MS',
    33.03139,
    -90.99806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBAM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TLCM6',
    '02473850',
    'LMRFC',
    'MS',
    NULL,
    'TALLAHOMA CREEK TRIBUTARY AT LAKE COMO MS',
    31.96194,
    -89.20528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TLCM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11959926,
    'GREM4',
    '04057811',
    'NCRFC',
    'MI',
    NULL,
    'GREENWOOD RESERVOIR NEAR GREENWOOD MI',
    46.44222,
    -87.80056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GREM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18471310,
    'HZNI3',
    '03374100',
    'OHRFC',
    'IN',
    NULL,
    'WHITE RIVER AT HAZLETON IN',
    38.48972,
    -87.55
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HZNI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDAM6',
    '02481400',
    'LMRFC',
    'MS',
    NULL,
    'WOLF RIVER NR POPLARVILLE MS',
    30.84722,
    -89.47222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDAM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RIDI3',
    '03325519',
    'OHRFC',
    'IN',
    NULL,
    'MISSISSINEWA RIVER AT RIDGEVILLE IN',
    40.283056,
    -85.028333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RIDI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRHN7',
    '0214196125',
    'SERFC',
    'NC',
    NULL,
    'CATAWBA RIVER AT NC HIGHWAY 16 NR MILLERSVILLE NC',
    35.82247,
    -81.19054
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRHN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRNN7',
    '0214264790',
    'SERFC',
    'NC',
    NULL,
    'CATAWBA R AT RR BRIDGE AB NC 73 AT COWANS FORD NC',
    35.42773,
    -80.95734
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRNN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRRN7',
    '02141500',
    'SERFC',
    'NC',
    NULL,
    'CATAWBA RIVER AT RHODHISS NC',
    35.77263,
    -81.43066
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRRN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SNYM8',
    '12357800',
    'NWRFC',
    'MT',
    NULL,
    'SNYDER CREEK NR MOUTH NR WEST GLACIER MT',
    48.616389,
    -113.876389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNYM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19285865,
    'NW917',
    '05438030',
    'NCRFC',
    'IL',
    '7090006',
    'FRANKLINVILLE CREEK AT FRANKLINVILLE~ IL',
    42.27111,
    -88.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW917'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WSSN7',
    '0208735460',
    'SERFC',
    'NC',
    NULL,
    'WALNUT CREEK AT SOUTH STATE STREET AT RALEIGH NC',
    35.757912,
    -78.623826
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WAAH1',
    '16759600',
    'AKRFC',
    'HI',
    NULL,
    'WAIAHA STREAM AT HOLUALOA HI',
    19.63419,
    -155.94969
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WAAH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCWN1',
    '06466470',
    'MBRFC',
    'NE',
    NULL,
    'HOWE CREEK BELOW WALKER DRAW NEAR CENTER NEBR.',
    42.6733,
    -97.8538
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCWN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DCLN7',
    '02145268',
    'SERFC',
    'NC',
    NULL,
    'DUHARTS CREEK AT SR 2439 NEAR CRAMERTON NC',
    35.2475,
    -81.09767
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCLN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPTN1',
    '410333095530101',
    'MBRFC',
    'NE',
    NULL,
    'MISSOURI RIVER NEAR LA PLATTE NEBR.',
    41.05935,
    -95.88367
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPTN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NPHN1',
    '412126095565201',
    'MBRFC',
    'NE',
    NULL,
    'MISSOURI RIVER AT NP DODGE PARK AT OMAHA NEBR.',
    41.35747,
    -95.94778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NPHN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EBRN4',
    '01393895',
    'MARFC',
    'NJ',
    NULL,
    'EAST BR RAHWAY RIV AT MILLBURN AVE AT MILLBURN NJ',
    40.72278,
    -74.285
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EBRN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MPLN4',
    '01393890',
    'MARFC',
    'NJ',
    NULL,
    'EAST BRANCH RAHWAY RIVER AT MAPLEWOOD NJ',
    40.73472,
    -74.27056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MPLN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6249966,
    'PEKN4',
    '01389550',
    'MARFC',
    'NJ',
    '2030103',
    'Peckman River at Little Falls NJ',
    40.87194,
    -74.22222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PEKN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AADN5',
    '08329880',
    'WGRFC',
    'NM',
    NULL,
    'ACADEMY ACRES DRAIN IN ALBUQUERQUE NM',
    35.15111,
    -106.57306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AADN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BUKN5',
    '08313150',
    'WGRFC',
    'NM',
    NULL,
    'RIO GRANDE ABV BUCKMAN DIVERSION NR WHITE ROCKNM',
    35.83842,
    -106.15908
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BUKN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LVDN2',
    '09419698',
    'CBRFC',
    'NV',
    NULL,
    'LV WASH BLW DUCK CK CONF NR HENDERSON NV',
    36.09155,
    -114.99922
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LVDN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'THRN4',
    '01392170',
    'MARFC',
    'NJ',
    NULL,
    'THIRD RIVER AT BLOOMFIELD NJ',
    40.799722,
    -74.188333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'THRN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AVLN4',
    '01411355',
    'MARFC',
    'NJ',
    NULL,
    'INGRAM THOROFARE AT AVALON NJ',
    39.110833,
    -74.734167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AVLN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11236489,
    'NW777',
    NULL,
    'SERFC',
    'NC',
    '3020202',
    'BEAR CREEK AT MAYS STORE~ NC',
    35.27111,
    -77.79638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW777'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VRMN2',
    '09415090',
    'CBRFC',
    'NV',
    NULL,
    'VIRGIN RV AT MESQUITE NV',
    36.7902,
    -114.0936
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VRMN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PESN2',
    '09415908',
    'CBRFC',
    'NV',
    NULL,
    'PEDERSON E SPGS NR MOAPA NV',
    36.70936,
    -114.71571
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PESN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PSGN2',
    '09415910',
    'CBRFC',
    'NV',
    NULL,
    'PEDERSON SPGS NR MOAPA NV',
    36.70958,
    -114.71597
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PSGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RSGN2',
    '360310115303201',
    'CNRFC',
    'NV',
    NULL,
    '163  S22 E58 07ADDA1    RAINBOW SPRING',
    36.05273,
    -115.50901
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AMDN6',
    '01354083',
    'NERFC',
    'NY',
    NULL,
    'MOHAWK RIVER AT AMSTERDAM NY',
    42.934,
    -74.19186
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AMDN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FODN6',
    '01349527',
    'NERFC',
    'NY',
    NULL,
    'MOHAWK R ABOVE STATE HIGHWAY 30A AT FONDA NY',
    42.95028,
    -74.3725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FODN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HTLN6',
    '01362342',
    'NERFC',
    'NY',
    NULL,
    'HOLLOW TREE BROOK AT LANESVILLE NY',
    42.14222,
    -74.265
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HTLN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KFDN6',
    '01359525',
    'NERFC',
    'NY',
    NULL,
    'NORMANS KILL AT KARLSFELD NY',
    42.64792,
    -73.846
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KFDN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MTBN6',
    '01362487',
    'NERFC',
    'NY',
    NULL,
    'BEAVER KILL AT MOUNT TREMPER NY',
    42.04667,
    -74.27722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTBN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WWAN6',
    '01422779',
    'MARFC',
    'NY',
    NULL,
    'WEST BROOK AT AUSTIN LINCOLN PARK AT WALTON NY',
    42.17739,
    -75.12892
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WWAN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DEXN6',
    '04260535',
    'NERFC',
    'NY',
    NULL,
    'PERCH RIVER NEAR DEXTER NY',
    44.00403,
    -76.07825
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DEXN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBSN6',
    '01351298',
    'NERFC',
    'NY',
    NULL,
    'COBLESKILL CREEK AT S GRAND ST AT COBLESKILL NY',
    42.6725,
    -74.481667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBSN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MSHN6',
    '0422018605',
    'NERFC',
    'NY',
    NULL,
    'MARSH CREEK AT MOUTH AT THE BRIDGES NY',
    43.35311,
    -78.1912
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MSHN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPON6',
    '04250516',
    'NERFC',
    'NY',
    NULL,
    'SOUTH POND OUTLET AT SANDY POND NY',
    43.63089,
    -76.19411
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPON6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10312290,
    'LCRN6',
    '04279000',
    'NERFC',
    'NY',
    NULL,
    'LA CHUTE AT TICONDEROGA NY',
    43.84403,
    -73.43189
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCRN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15594783,
    'BAKO1',
    '04201404',
    'OHRFC',
    'OH',
    NULL,
    'BAKER CREEK AT OLMSTED FALLS OH',
    41.35083,
    -81.90028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BAKO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DNBO1',
    '04208598',
    'OHRFC',
    'OH',
    NULL,
    'DOAN BROOK AT MLK JR DR AT CLEVELAND OH',
    41.51417,
    -81.61833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DNBO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EUCO1',
    '04208677',
    'OHRFC',
    'OH',
    NULL,
    'EUCLID CREEK AT SOUTH EUCLID OH',
    41.52444,
    -81.51417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EUCO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MYFO1',
    '04208943',
    'OHRFC',
    'OH',
    NULL,
    'UNNAMED TRIB TO CHAGRIN R AT MAYFIELD VILLAGE OH',
    41.54278,
    -81.44472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MYFO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PPKO1',
    '04208923',
    'OHRFC',
    'OH',
    NULL,
    'UNNAMED TRIBUTARY TO CHAGRIN R AT PEPPER PIKE OH',
    41.47111,
    -81.43833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PPKO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCMO1',
    '04208684',
    'OHRFC',
    'OH',
    NULL,
    'EAST BRANCH EUCLID CREEK AT RICHMOND HEIGHTS OH',
    41.57417,
    -81.49472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCMO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RKFO1',
    '03131122',
    'OHRFC',
    'OH',
    NULL,
    'ROCKY FORK AT LUCAS OH',
    40.70361,
    -82.41444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RKFO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'QUVO2',
    '07188005',
    'ABRFC',
    'OK',
    NULL,
    'BEAVER CREEK NEAR QUAPAW OK',
    36.93389,
    -94.75389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QUVO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WBFO2',
    '07194555',
    'ABRFC',
    'OK',
    NULL,
    'ARKANSAS RIVER AT GORE OK',
    35.51861,
    -95.12722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBFO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NUTO3',
    '14316460',
    'NWRFC',
    'OR',
    NULL,
    'NORTH UMPQUA R AT SODA SPGS NR TOKETEE FALLS OR',
    43.30611,
    -122.51167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NUTO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RRHO3',
    '14378430',
    'NWRFC',
    'OR',
    NULL,
    'ROGUE RIVER AT HWY 101 BRIDGE AT WEDDERBURN OR',
    42.42889,
    -124.41222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RRHO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FNFO3',
    '14150290',
    'NWRFC',
    'OR',
    NULL,
    'FALL CREEK ABOVE NORTH FORK NEAR LOWELL OR',
    43.96722,
    -122.62972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FNFO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JEFO3',
    '14189050',
    'NWRFC',
    'OR',
    NULL,
    'SANTIAM RIVER NEAR JEFFERSON OR',
    44.73889,
    -123.04861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JEFO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WREO3',
    '14158100',
    'NWRFC',
    'OR',
    NULL,
    'WILLAMETTE RIVER AT OWOSSO BRIDGE AT EUGENE OR',
    44.09167,
    -123.11611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WREO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRNO3',
    '453630122021400',
    'NWRFC',
    'OR',
    NULL,
    'COLUMBIA RIVER LEFT BANK NEAR DODSON OR',
    45.60833,
    -122.03722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRNO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 188633,
    'BCLC2',
    '06710605',
    'MBRFC',
    'CO',
    NULL,
    'BEAR CREEK ABOVE BEAR CREEK LAKE NEAR MORRISON CO',
    39.65203,
    -105.17325
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCLC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1234505,
    'CBLC2',
    '09019000',
    'CBRFC',
    'CO',
    NULL,
    'COLORADO RIVER BELOW LAKE GRANBY CO.',
    40.14417,
    -105.86667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBLC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TUCC1',
    '11289500',
    'CNRFC',
    'CA',
    NULL,
    'TURLOCK CN NR LA GRANGE CA',
    37.66583,
    -120.44
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TUCC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AMXP1',
    '03049641',
    'OHRFC',
    'PA',
    NULL,
    'ALLEGHENY R AT CW BILL YOUNG (LOWER) ACMETONIA PA',
    40.53609,
    -79.8171
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AMXP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PHDP1',
    '01474501',
    'MARFC',
    'PA',
    NULL,
    'SCHUYLKILL RIVER NEAR 30TH ST AT PHILADELPHIA PA',
    39.95664,
    -75.18031
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHDP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCPS1',
    '021313485',
    'SERFC',
    'SC',
    NULL,
    'BUFFALO CREEK AT MT PISGAH SC',
    34.58222,
    -80.45694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCPS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KEAS1',
    '02131452',
    'SERFC',
    'SC',
    NULL,
    'LITTLE LYNCHES RIVER ABOVE KERSHAW SC',
    34.58583,
    -80.58056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KEAS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SAFS1',
    '02172558',
    'SERFC',
    'SC',
    NULL,
    'SOUTH FORK EDISTO RIVER ABOVE SPRINGFIELDSC',
    33.52028,
    -81.41028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAFS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SAPS1',
    '02167716',
    'SERFC',
    'SC',
    NULL,
    'LITTLE SALUDA R NEAR PROSPERITY SC',
    34.07944,
    -81.56194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAPS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRIS1',
    '02168900',
    'SERFC',
    'SC',
    NULL,
    'SALUDA RIVER AT I-20 NEAR COLUMBIASC',
    34.02583,
    -81.12806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRIS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SUNS1',
    '02173495',
    'SERFC',
    'SC',
    NULL,
    'SUNNYSIDE CANAL AT ORANGEBURG SC',
    33.49194,
    -80.87583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SUNS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CERS1',
    '02174045',
    'SERFC',
    'SC',
    NULL,
    'EDISTO RIVER AT I-95 ABOVE CANADYSSC',
    33.0902,
    -80.6497
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CERS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MAYP4',
    '50136400',
    NULL,
    'PR',
    NULL,
    'RIO ROSARIO NR HORMIGUEROS PR',
    18.16009,
    -67.08576
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MAYP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MODP4',
    '50148890',
    NULL,
    'PR',
    NULL,
    'RIO CULEBRINAS AT MARGARITA DAMSITE NR AGUADA PR',
    18.3945,
    -67.1512
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MODP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UTUP4',
    '50021700',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE ARECIBO ABV UTUADO PR',
    18.2442,
    -66.72229
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UTUP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GUBP4',
    '50011000',
    NULL,
    'PR',
    NULL,
    'CANAL DE DERIVACION AT LAGO DE GUAJATACA PR',
    18.39952,
    -66.92751
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GUBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HORP4',
    '50138000',
    NULL,
    'PR',
    NULL,
    'RIO GUANAJIBO NR HORMIGUEROS PR',
    18.14268,
    -67.14875
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HORP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TBGS1',
    '021355015',
    'SERFC',
    'SC',
    NULL,
    'TEARCOAT BRANCH AT I-95 NEAR MANNINGSC',
    33.81303,
    -80.15161
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBGS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRGS1',
    '021720508',
    'SERFC',
    'SC',
    NULL,
    'COOPER RIVER ABOVE GOOSE CREEKSC',
    33.01578,
    -79.90675
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRGS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ELBS1',
    '0219897993',
    'SERFC',
    'SC',
    NULL,
    'SAVANNAH RIVER AT ELBA ISLAND NEAR SAVANNAH GA',
    32.10306,
    -81.00694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELBS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PAKS1',
    '0217206935',
    'SERFC',
    'SC',
    NULL,
    'COOPER RVR @ PORTS AUTHORITY PIER K CHARLESTON SC',
    32.85569,
    -79.95267
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAKS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CFCS1',
    '02147310',
    'SERFC',
    'SC',
    NULL,
    'CATAWBA RIVER AT GREAT FALLSSC',
    34.59806,
    -80.89056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CFCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CGFS1',
    '021474095',
    'SERFC',
    'SC',
    NULL,
    'GREAT FALLS RESERVOIR TAILRACE AT GREAT FALLSSC',
    34.55783,
    -80.89336
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CGFS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCCS1',
    '021623957',
    'SERFC',
    'SC',
    NULL,
    'BIG FALLS CREEK NEAR TIGERVILLESC',
    35.17,
    -82.40278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NSHS1',
    '021564493',
    'SERFC',
    'SC',
    NULL,
    'BROAD RIVER BELOW NEAL SHOALS RES. NR CARLISLESC',
    34.66306,
    -81.44722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSHS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FQCS1',
    '021720368',
    'SERFC',
    'SC',
    NULL,
    'FRENCH QUARTER CREEK NEAR HUGERSC',
    33.02842,
    -79.85409
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FQCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRCS1',
    '0217206962',
    'SERFC',
    'SC',
    NULL,
    'WANDO RIVER AT CAINHOY BELOW WANDOSC',
    32.924278,
    -79.832694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PDGS1',
    '02136350',
    'SERFC',
    'SC',
    NULL,
    'PEEDEE RIVER AT GEORGETOWN S C',
    33.366944,
    -79.267778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PDGS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HIBT1',
    '034991109',
    'LMRFC',
    'TN',
    NULL,
    'STOCK CREEK AT MARTIN MILL RD NR KNOXVILLE',
    35.884167,
    -83.838333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HIBT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NSLS1',
    '021623950',
    'SERFC',
    'SC',
    NULL,
    'NORTH SALUDA RIVER NEAR HIGHLANDSC',
    35.16722,
    -82.36472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSLS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SOSS2',
    '450628097060800',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP NEAR SOUTH SHORE SD',
    45.10778,
    -97.10222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SOSS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WVYS2',
    '445959096582600',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP AT WAVERLY SD',
    44.99972,
    -96.97389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WVYS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HMDS2',
    '433047096391700',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP AT HARMODON PARK AT SIOUX FALLS SD',
    43.51306,
    -96.65472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMDS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHPT1',
    '03434870',
    'OHRFC',
    'TN',
    NULL,
    'CUMBERLAND RIVER AT RIVER MILE 142.9 TN',
    36.31472,
    -87.21611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHPT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JNCT1',
    '03418224',
    'OHRFC',
    'TN',
    NULL,
    'JENNINGS CREEK AT RILEY CR RD AT WHITLEYVILLE TN',
    36.44028,
    -85.67472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JNCT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NSST1',
    '034315005',
    'OHRFC',
    'TN',
    NULL,
    'CUMBERLAND RIVER AT WOODLAND ST AT NASHVILLE TN',
    36.16722,
    -86.77639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSST1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MSAP4',
    '50065500',
    NULL,
    'PR',
    NULL,
    'RIO MAMEYES NR SABANA PR',
    18.32881,
    -65.75081
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MSAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPFP4',
    '50093053',
    NULL,
    'PR',
    NULL,
    'CANAL DE PATILLAS AT FOREBAY PR',
    18.02083,
    -66.02306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPFP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCRP4',
    '50085100',
    NULL,
    'PR',
    NULL,
    'RIO GUAYANES AT CENTRAL ROIG PR',
    18.06639,
    -65.87417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCRP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SNKT1',
    '03596075',
    'LMRFC',
    'TN',
    NULL,
    'SINKING POND AT AEDC TN',
    35.41,
    -86.06972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNKT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHYT2',
    '07227460',
    'ABRFC',
    'TX',
    NULL,
    'E FK CHEYENNE CK TRIB NR CHANNING TX',
    35.6751,
    -102.28072
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHYT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSRT2',
    '07299825',
    'ABRFC',
    'TX',
    NULL,
    'SALT FK RED RV TRIB AT FM 294 NR GOODNIGHT TX',
    35.11318,
    -101.18676
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HTBT2',
    '07295450',
    'ABRFC',
    'TX',
    NULL,
    'TIERRA BLANCA CK NR FM 1259 AT HEREFORD TX',
    34.81294,
    -102.3899
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HTBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UDCT2',
    '08407580',
    'WGRFC',
    'TX',
    NULL,
    'UNM TRIB NO. 1 UNIV DRAW NR CORNUDAS TX',
    31.79846,
    -105.58166
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UDCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BLVT2',
    '08170905',
    'WGRFC',
    'TX',
    NULL,
    'BLANCO RV AT VALLEY VIEW RD NR FISCHER TX',
    30.0368,
    -98.2229
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EWAT2',
    '08178627',
    'WGRFC',
    'TX',
    NULL,
    'ELM WATERHOLE CK TRIB AT EVANS RD NR SAN ANTONIOTX',
    29.64667,
    -98.40639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EWAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FSBT2',
    '08152710',
    'WGRFC',
    'TX',
    NULL,
    'FELPS SPGS NR BURNET TX',
    30.71944,
    -98.22806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FSBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCCT2',
    '08180941',
    'WGRFC',
    'TX',
    NULL,
    'CULEBRA CK IN GCSNA NR HELOTES TX',
    29.53917,
    -98.75139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HOST2',
    '08168000',
    'WGRFC',
    'TX',
    NULL,
    'HUECO SPGS NR NEW BRAUNFELS TX',
    29.75917,
    -98.13972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JWMT2',
    '08170990',
    'WGRFC',
    'TX',
    NULL,
    'JACOBS WELL SPG NR WIMBERLEY TX',
    30.03444,
    -98.12611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JWMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KIVT2',
    '08212300',
    'WGRFC',
    'TX',
    NULL,
    'TRANQUITAS CK AT KINGSVILLE TX',
    27.525833,
    -97.867222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KIVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LMKT2',
    '08458975',
    'WGRFC',
    'TX',
    NULL,
    'MANADAS CK AT LAREDO TX',
    27.575833,
    -99.506111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LMKT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BKTT2',
    '08106050',
    'WGRFC',
    'TX',
    NULL,
    'BRUSHY CK AT FM 619 NR TAYLOR TX',
    30.519444,
    -97.338333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKTT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FILT2',
    '08194840',
    'WGRFC',
    'TX',
    NULL,
    'FRIO RV AT LEAKEY TX',
    29.722778,
    -99.753333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FILT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCOT2',
    '0810588650',
    'WGRFC',
    'TX',
    NULL,
    'LAKE CK AT O''CONNOR DR NR ROUND ROCK TX',
    30.48414,
    -97.72004
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCOT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SVBT2',
    '08181725',
    'WGRFC',
    'TX',
    NULL,
    'SAN ANTONIO RV NR VICTOR BRAUNIG LK NR ELMENDORFTX',
    29.23541,
    -98.40261
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SVBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCST2',
    '08168770',
    'WGRFC',
    'TX',
    NULL,
    'WFK DRY COMAL CK AT SCHUETZ DAM NEW BRAUNFELS TX',
    29.67589,
    -98.25109
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ACMT2',
    '08063030',
    'WGRFC',
    'TX',
    NULL,
    'ASH CK AT HWY 171 NR MALONE TX',
    31.90956,
    -96.88146
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AQAT2',
    '08093360',
    'WGRFC',
    'TX',
    NULL,
    'AQUILLA CK ABV AQUILLA TX',
    31.89528,
    -97.20278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AQAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BNLT2',
    '08102595',
    'WGRFC',
    'TX',
    NULL,
    'NOLAN CK AT S PENELOPE BELTON TX',
    31.05397,
    -97.46253
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BNLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCCT2',
    '08101300',
    'WGRFC',
    'TX',
    NULL,
    'COWHOUSE CK AT W RANGE RD NR FORT HOOD TX',
    31.21499,
    -97.79419
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCJT2',
    '08101200',
    'WGRFC',
    'TX',
    NULL,
    'COWHOUSE CK AT OLD GEORGETOWN RD NR FT HOOD TX',
    31.24879,
    -97.83785
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCJT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FHCT2',
    '08100630',
    'WGRFC',
    'TX',
    NULL,
    'HENSON CK AT W RANGE RD NR GATESVILLE TX',
    31.32323,
    -97.75766
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FHCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FHHT2',
    '08101340',
    'WGRFC',
    'TX',
    NULL,
    'HOUSE CK AT W RANGE RD NR FT HOOD TX',
    31.18553,
    -97.80872
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FHHT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCST2',
    '08169780',
    'WGRFC',
    'TX',
    NULL,
    'GERONIMO CK NR SEGUIN TX',
    29.5905,
    -97.9347
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CIFP4',
    '50047535',
    NULL,
    'PR',
    NULL,
    'RIO DE BAYAMON AT ARENAS PR',
    18.16917,
    -66.12194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CIFP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JAYP4',
    '50025155',
    NULL,
    'PR',
    NULL,
    'RIO SALIENTE AT COABEY NR JAYUYA PR',
    18.21282,
    -66.56319
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JAYP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRPP4',
    '50093120',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE PATILLAS BLW LAGO PATILLAS PR',
    18.01639,
    -66.02361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRPP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LAGP4',
    '50128940',
    NULL,
    'PR',
    NULL,
    'CANAL DE RIEGO DE LAJAS BLW LAJAS FILT PLANT LAJAS',
    18.04534,
    -67.05529
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LAGP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LARP4',
    '50010500',
    NULL,
    'PR',
    NULL,
    'RIO GUAJATACA AT LARES PR',
    18.2992,
    -66.87386
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LARP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'IANP4',
    '50112500',
    NULL,
    'PR',
    NULL,
    'RIO INABON AT REAL ABAJO PR',
    18.08648,
    -66.56308
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IANP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UTHP4',
    '50028000',
    NULL,
    'PR',
    NULL,
    'RIO TANAMA NR UTUADO PR',
    18.30073,
    -66.78301
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UTHP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GUTP4',
    '50055750',
    NULL,
    'PR',
    NULL,
    'RIO GURABO BLW EL MANGO PR',
    18.23415,
    -65.88533
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GUTP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BAYP4',
    '50047850',
    NULL,
    'PR',
    NULL,
    'RIO DE BAYAMON NR BAYAMON PR',
    18.33428,
    -66.13948
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BAYP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NGKP4',
    '50075500',
    NULL,
    'PR',
    NULL,
    'RIO BLANCO AT FLORIDA PR',
    18.24083,
    -65.785
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NGKP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GYAP4',
    '50124200',
    NULL,
    'PR',
    NULL,
    'RIO GUAYANILLA NEAR GUAYANILLA PR',
    18.04424,
    -66.79816
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GYAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PAXP4',
    '50093075',
    NULL,
    'PR',
    NULL,
    'CANAL DE PATILLAS ABV GUAYAMA FILTRATION PLANT PR',
    17.9825,
    -66.10111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAXP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRSP4',
    '50128907',
    NULL,
    'PR',
    NULL,
    'LATERAL M5L CANAL DE RIEGO DE LAJAS SABANA GRANDE',
    18.03503,
    -66.89812
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRSP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LLYP4',
    '50126150',
    NULL,
    'PR',
    NULL,
    'RIO YAUCO ABV DIVERSION MONSERRATE NR YAUCO PR',
    18.04919,
    -66.84186
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLYP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SALP4',
    '50100200',
    NULL,
    'PR',
    NULL,
    'RIO LAPA NR RABO DEL BUEY PR',
    18.05972,
    -66.24111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SALP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLLP4',
    '50050900',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE LOIZA AT QUEBRADA ARENAS PR',
    18.12,
    -65.98861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLLP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GLNT2',
    '08017900',
    'WGRFC',
    'TX',
    NULL,
    'SABINE RV AT FM 17 NR GOLDEN TX',
    32.72022,
    -95.63509
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLNT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HBRT2',
    '08098450',
    'WGRFC',
    'TX',
    NULL,
    'BRAZOS RV AT FM 485 NEAR HEARNE TX',
    30.865,
    -96.695
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HBRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'APTA2',
    '15239900',
    'AKRFC',
    'AK',
    NULL,
    'ANCHOR R NR ANCHOR POINT AK',
    59.747222,
    -151.753056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'APTA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BATA2',
    '15238986',
    'AKRFC',
    'AK',
    NULL,
    'BATTLE C 1.0 MI AB MOUTH NR HOMER AK',
    59.762222,
    -150.953056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BATA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRTA2',
    '15239070',
    'AKRFC',
    'AK',
    NULL,
    'BRADLEY R NR TIDEWATER NR HOMER AK',
    59.801667,
    -150.882778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRTA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRUA2',
    '15238990',
    'AKRFC',
    'AK',
    NULL,
    'UPPER BRADLEY R NR NUKA GLACIER NR HOMER AK',
    59.700556,
    -150.7025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRUA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHLA2',
    '15511000',
    'AKRFC',
    'AK',
    NULL,
    'L CHENA R NR FAIRBANKS AK',
    64.886111,
    -147.247222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHLA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHRA2',
    '15275100',
    'AKRFC',
    'AK',
    NULL,
    'CHESTER C AT ARCTIC BOULEVARD AT ANCHORAGE AK',
    61.205278,
    -149.895278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CJXA2',
    '15304000',
    'AKRFC',
    'AK',
    NULL,
    'KUSKOKWIM R AT CROOKED CREEK AK',
    61.869444,
    -158.111389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CJXA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CKDA2',
    '15304010',
    'AKRFC',
    'AK',
    NULL,
    'CROOKED C AB AIRPORT RD NR CROOKED CREEK AK',
    61.89,
    -158.154444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CKDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EKLA2',
    '15278000',
    'AKRFC',
    'AK',
    NULL,
    'EKLUTNA LK NR PALMER AK',
    61.410833,
    -149.122222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EKLA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLBT2',
    '08108780',
    'WGRFC',
    'TX',
    NULL,
    'LITTLE BRAZOS RV AT FM 485 NR HEARNE TX',
    30.87944,
    -96.64028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MYRT2',
    '08050350',
    'WGRFC',
    'TX',
    NULL,
    'ELM FK TRINITY RV AT FM 1198 NR MYRA TX',
    33.601,
    -97.32583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MYRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PMCT2',
    '08061548',
    'WGRFC',
    'TX',
    NULL,
    'MUDDY CK AT CREEK CROSSING LN NR SACHSE TX',
    32.98586,
    -96.55642
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PMCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PPWT2',
    '08090550',
    'WGRFC',
    'TX',
    NULL,
    'PALO PINTO CK AT WATER PLANT RD NR SANTO TX',
    32.65375,
    -98.1294
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PPWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCKT2',
    '08063040',
    'WGRFC',
    'TX',
    NULL,
    'RICHLAND CK AT HWY 22 NR MERTENS TX',
    32.05467,
    -96.90652
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCKT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRNA2',
    '15239060',
    'AKRFC',
    'AK',
    NULL,
    'MF BRADLEY R BL NF BRADLEY R NR HOMER AK',
    59.79833,
    -150.86333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRNA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRRA2',
    '15239001',
    'AKRFC',
    'AK',
    NULL,
    'BRADLEY R BL DAM NR HOMER AK',
    59.75833,
    -150.85056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PATP4',
    '50092000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE PATILLAS NR PATILLAS PR',
    18.03417,
    -66.0325
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PATP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MANP4',
    '50038100',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE MANATI AT HWY 2 NR MANATI PR',
    18.43097,
    -66.52686
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MANP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PBTP4',
    '50093090',
    NULL,
    'PR',
    NULL,
    'CANAL DE PATILLAS ABV EL LEGADO INTAKE PR',
    17.97833,
    -66.18
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PBTP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TOVP4',
    '50110900',
    NULL,
    'PR',
    NULL,
    'RIO TOA VACA ABV LAGO TOA VACA PR',
    18.1265,
    -66.45778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TOVP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CMAP4',
    '50014800',
    NULL,
    'PR',
    NULL,
    'RIO CAMUY NR BAYANEY PR',
    18.39644,
    -66.81799
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLKP4',
    '50051310',
    NULL,
    'PR',
    NULL,
    'RIO CAYAGUAS AT CERRO GORDO PR',
    18.15386,
    -65.95671
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLKP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JUAP4',
    '50111500',
    NULL,
    'PR',
    NULL,
    'RIO JACAGUAS AT JUANA DIAZ PR',
    18.0543,
    -66.51103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JUAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JDCP4',
    '50111340',
    NULL,
    'PR',
    NULL,
    'CANAL DE JUANA DIAZ AT BO. PENUELAS NR SALINAS PR',
    17.99828,
    -66.34189
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JDCP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NASP4',
    '50045010',
    NULL,
    'PR',
    NULL,
    'RIO DE LA PLATA BLW LA PLATA DAMSITE PR',
    18.34601,
    -66.23881
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NASP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COMP4',
    '50043800',
    NULL,
    'PR',
    NULL,
    'RIO DE LA PLATA AT COMERIO PR',
    18.22216,
    -66.22462
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COMP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAKP4',
    '50053025',
    NULL,
    'PR',
    NULL,
    'RIO TURABO ABV BORINQUEN PR',
    18.16222,
    -66.04
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAKP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCOA2',
    '15215900',
    'AKRFC',
    'AK',
    NULL,
    'GLACIER R TRIB NR CORDOVA AK',
    60.532797,
    -145.380615
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCOA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCSA2',
    '15292000',
    'AKRFC',
    'AK',
    NULL,
    'SUSITNA R AT GOLD CREEK AK',
    62.767778,
    -149.691111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8272227,
    'DCLC1',
    '11465240',
    'CNRFC',
    'CA',
    '18010110',
    'DRY C BLW LAMBERT BR NR GEYSERVILLE CA',
    38.65333,
    -122.92611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCLC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 83230,
    'WPMF1',
    '254157080213800',
    'SERFC',
    'FL',
    '3090202',
    'SNAPPER CREEK NO.5 ABV WATER PIPE NR S. MIAMI~ FL',
    25.69906,
    -80.36061
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10762537,
    'BVRN2',
    '10317480',
    'CNRFC',
    'NV',
    '16040102',
    'BEAVER CK ABV CONF N FK HUMBOLDT RV NR HALLECK~ NV',
    41.32972,
    -115.57194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BVRN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10776466,
    'HNKN2',
    '10313900',
    'CNRFC',
    'NV',
    '16040101',
    'HANKS CK AT MARYS RV RANCH RD NR DEETH~ NV',
    41.47786,
    -115.28664
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HNKN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11120313,
    'RCKN2',
    '10245970',
    'CNRFC',
    'NV',
    '16060005',
    'ROBERTS CK NR EUREKA~ NV',
    39.78978,
    -116.30094
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCKN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22473911,
    'CREI4',
    '05464480',
    'NCRFC',
    'IA',
    NULL,
    'CEDAR RIVER AT EDGEWOOD ROAD AT CEDAR RAPIDS IA',
    42.01167,
    -91.70472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CREI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GKGA2',
    '15478038',
    'AKRFC',
    'AK',
    NULL,
    'GULKANA GLACIER 16 MI N OF PAXSON AK',
    63.25944,
    -145.40528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GKGA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GROA2',
    '15237730',
    'AKRFC',
    'AK',
    NULL,
    'GROUSE C AT GROUSE LK OUTLET NR SEWARD AK',
    60.198333,
    -149.373333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GROA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRSA2',
    '15200280',
    'AKRFC',
    'AK',
    NULL,
    'GULKANA R AT SOURDOUGH AK',
    62.520833,
    -145.530833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ILRA2',
    '15300300',
    'AKRFC',
    'AK',
    NULL,
    'ILIAMNA R NR PEDRO BAY AK',
    59.758611,
    -153.844722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ILRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KDKA2',
    '15295700',
    'AKRFC',
    'AK',
    NULL,
    'TERROR R AT MOUTH NR KODIAK AK',
    57.694722,
    -153.161667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KDKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BMPT2',
    '08075110',
    'WGRFC',
    'TX',
    NULL,
    'BRAYS BAYOU AT MLK JR BLVD HOUSTON TX',
    29.71417,
    -95.33889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BMPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLCT2',
    '08077637',
    'WGRFC',
    'TX',
    NULL,
    'CLEAR LK SECOND OUTFLOW CHANNEL AT KEMAH TX',
    29.55417,
    -95.02556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWAT2',
    '08162501',
    'WGRFC',
    'TX',
    NULL,
    'COLORADO RV NR WADSWORTH TX',
    28.77417,
    -95.9975
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DCFT2',
    '08068305',
    'WGRFC',
    'TX',
    NULL,
    'DRY CK AT FM 2978 THE WOODLANDS TX',
    30.17567,
    -95.60103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HPOT2',
    '08074710',
    'WGRFC',
    'TX',
    NULL,
    'BUFFALO BAYOU AT TURNING BASIN HOUSTON TX',
    29.74917,
    -95.29083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HPOT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPST2',
    '08077740',
    'WGRFC',
    'TX',
    NULL,
    'LA MARQUE LEVEE PUMP STA NR LA MARQUE TX',
    29.34556,
    -94.96306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TPST2',
    '08077658',
    'WGRFC',
    'TX',
    NULL,
    'TX CITY PUMP STA AT TEXAS CITY TX',
    29.35722,
    -94.92472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TPST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FRZT2',
    '08080650',
    'WGRFC',
    'TX',
    NULL,
    'RUNNING WATER DRAW AT SH 214 NR FRIONA TX',
    34.47433,
    -102.73301
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRZT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KLDT2',
    '07299575',
    'ABRFC',
    'TX',
    NULL,
    'N GROESBECK CK TRIB NR KIRKLAND TX',
    34.39519,
    -100.05624
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KLDT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KLCA2',
    '15303900',
    'AKRFC',
    'AK',
    NULL,
    'KUSKOKWIM R AT LISKYS CROSSING NR STONY RIVER AK',
    62.051944,
    -156.210556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KLCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KOWA2',
    '15294080',
    'AKRFC',
    'AK',
    NULL,
    'KROTO CREEK AT OILWELL ROAD NEAR TRAPPER CREEK AK',
    62.128889,
    -150.438889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KOWA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KTIA2',
    '15302200',
    'AKRFC',
    'AK',
    NULL,
    'KOKTULI R NR ILIAMNA AK',
    59.793333,
    -155.5225
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KTIA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CSRT2',
    '08117370',
    'WGRFC',
    'TX',
    NULL,
    'SAN BERNARD RV NR CHESTERVILLE TX',
    29.6605,
    -96.2436
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OCNT2',
    '08117705',
    'WGRFC',
    'TX',
    NULL,
    'SAN BERNARD RV NR SWEENY TX',
    29.1119,
    -95.6766
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OCNT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LOKT2',
    '08080750',
    'WGRFC',
    'TX',
    NULL,
    'CALLAHAN DRAW NR LOCKNEY TX',
    33.99743,
    -101.54884
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOKT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SIWT2',
    '07298150',
    'ABRFC',
    'TX',
    NULL,
    'ROCK CK TRIB NR SILVERTON TX',
    34.47801,
    -101.42997
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SIWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSWT2',
    '08123620',
    'WGRFC',
    'TX',
    NULL,
    'SULPHUR SPGS DRAW NR WELLMAN TX',
    33.05933,
    -102.41531
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8590450,
    'NW737',
    '07048490',
    'LMRFC',
    'AR',
    '11010001',
    'Town Branch Trib at Hwy 16 at Fayetteville~ AR',
    36.03388,
    -94.1525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW737'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EKIW2',
    '03050340',
    'OHRFC',
    'WV',
    NULL,
    'TYGART VALLEY R ABOVE INLET WORKS ELKINS WV',
    38.91536,
    -79.8755
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EKIW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EKOW2',
    '03050450',
    'OHRFC',
    'WV',
    NULL,
    'TYGART VALLEY R ABOVE OUTLET WORKS ELKINS WV',
    38.92126,
    -79.86839
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EKOW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LHCW2',
    '03202928',
    'OHRFC',
    'WV',
    NULL,
    'LITTLE HUFF CR AT LITTLE CUB CR RD AT HANOVER WV',
    37.58522,
    -81.8215
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHCW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RSET2',
    '08374510',
    'WGRFC',
    'TX',
    NULL,
    'RIO GRANDE DWS SANTA ELENA CANYON BBNP TX',
    29.15472,
    -103.59778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CGET2',
    '08022120',
    'WGRFC',
    'TX',
    NULL,
    'SABINE RV NR CARTHAGE TX',
    32.22497,
    -94.22525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CGET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CJNT2',
    '08140860',
    'WGRFC',
    'TX',
    NULL,
    'JIM NED CK AT CR 140 NR COLEMAN TX',
    31.87925,
    -99.27784
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CJNT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KAVT2',
    '08143890',
    'WGRFC',
    'TX',
    NULL,
    'SAN SABA RV AT FM 864 NR FORT MCKAVETT TX',
    30.83456,
    -100.09409
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KAVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CFKW2',
    '03197062',
    'OHRFC',
    'WV',
    NULL,
    'COOKMAN FORK AT INTERSTATE 79 NEAR WALLBACK WV',
    38.59,
    -81.0878
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CFKW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCFW2',
    '03159718',
    'OHRFC',
    'WV',
    NULL,
    'GRASSLICK CREEK TRIB AB I-77 NEAR FAIRPLAIN WV',
    38.726361,
    -81.655361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCFW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRRW2',
    '03159823',
    'OHRFC',
    'WV',
    NULL,
    'GRASS RUN TRIBUTARY ABOVE I-77 NEAR RIPLEY WV',
    38.789444,
    -81.728056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRRW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GNST2',
    '08427000',
    'WGRFC',
    'TX',
    NULL,
    'GIFFIN SPGS AT TOYAHVALE TX',
    30.9475,
    -103.788611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GNST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDAT2',
    '08132450',
    'WGRFC',
    'TX',
    NULL,
    'RED ARROYO AT S. CHADBOURNE ST. NR SAN ANGELO TX',
    31.43578,
    -100.43433
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCVT2',
    '08128030',
    'WGRFC',
    'TX',
    NULL,
    'S CONCHO RV ABV TWIN BUTTES RES NR SAN ANGELO TX',
    31.27276,
    -100.50422
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRHU1',
    '09328990',
    'CBRFC',
    'UT',
    NULL,
    'COLORADO RIVER AB DIRTY DEVIL RIVER NR HITE UT',
    37.89147,
    -110.37084
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRHU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCBU1',
    '09327000',
    'CBRFC',
    'UT',
    NULL,
    'FERRON CR BL MILLSITE RES & DIVS NR FERRON UT',
    39.09526,
    -111.17887
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCBU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MNCV2',
    '0167357970',
    'MARFC',
    'VA',
    NULL,
    'MONCUIN CREEK BELOW US HWY 360 NEAR MANQUIN VA',
    37.71225,
    -77.14636
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNCV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YEGA4',
    '07055608',
    'LMRFC',
    'AR',
    NULL,
    'CROOKED CREEK AT YELLVILLE',
    36.22306,
    -92.67972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YEGA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23980843,
    'CLWW1',
    '12097820',
    'NWRFC',
    'WA',
    NULL,
    NULL,
    47.127326965332,
    -121.802604675293
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLWW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23977624,
    'GRKW1',
    '12113344',
    'NWRFC',
    'WA',
    NULL,
    NULL,
    47.4228782653809,
    -122.265396118164
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRKW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SMDV2',
    '363342076261100',
    'SERFC',
    'VA',
    NULL,
    'SYCAMORE DITCH NEAR CYPRESS CHAPEL VA',
    36.56178,
    -76.43631
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SMDV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MRCV2',
    '01622459',
    'MARFC',
    'VA',
    NULL,
    'MIDDLE RIVER AT ROUTE 721 NEAR CHURCHVILLE VA',
    38.20228,
    -79.15214
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MRCV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NSCV2',
    '0202848919',
    'MARFC',
    'VA',
    NULL,
    'SPRUCE CREEK ABOVE ROUTE 151 NEAR NELLYSFORD VA',
    37.89058,
    -78.91544
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSCV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDWV2',
    '02020258',
    'MARFC',
    'VA',
    NULL,
    'RAMSEYS DRAFT AT ROUTE 629 NEAR WEST AUGUSTA VA',
    38.24417,
    -79.33481
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDWV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CFPV2',
    '02012800',
    'MARFC',
    'VA',
    NULL,
    'JACKSON RIV AT FILTRATION PLANT AT COVINGTON VA',
    37.81083,
    -79.98861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CFPV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LRNV2',
    '0205551460',
    'SERFC',
    'VA',
    NULL,
    'LICK RUN ABOVE PATTON AVENUE AT ROANOKE VA',
    37.27794,
    -79.93756
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRNV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BDYW1',
    '12398600',
    'NWRFC',
    'WA',
    NULL,
    'PEND OREILLE RIVER AT INTERNATIONAL BOUNDARY',
    48.99889,
    -117.3525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BDYW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WSBW1',
    '12450650',
    'NWRFC',
    'WA',
    NULL,
    'WELLS POWERPLANT HEADWATER NEAR PATEROS WA',
    47.94778,
    -119.8625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSBW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBMW1',
    '14019240',
    'NWRFC',
    'WA',
    NULL,
    'COLUMBIA RIVER BELOW MCNARY DAM NEAR UMATILLA OR',
    45.9336,
    -119.32525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBMW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CTCW1',
    '12514400',
    'NWRFC',
    'WA',
    NULL,
    'COLUMBIA RIVER BELOW HWY 395 BRIDGE AT PASCO WA',
    46.22417,
    -119.11528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CTCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LHGW1',
    '13352600',
    'NWRFC',
    'WA',
    NULL,
    'SNAKE RIVER BELOW LOWER MONUMENTAL DAM WA',
    46.55387,
    -118.54776
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHGW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHUV2',
    '01622464',
    'MARFC',
    'VA',
    NULL,
    'MIDDLE RIVER ABOVE ROUTE 250 NEAR CHURCHVILLE VA',
    38.208333,
    -79.134167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHUV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRFV2',
    '01661977',
    'MARFC',
    'VA',
    NULL,
    'CARTER RUN AT ROUTE 681 NEAR JEFFERSONTON VA',
    38.7244,
    -77.9105
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRFV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NELV2',
    '0202848938',
    'MARFC',
    'VA',
    NULL,
    'SPRUCE CREEK AT ROUTE 627 NEAR NELLYSFORD VA',
    37.8836,
    -78.8991
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NELV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RMSV2',
    '02020246',
    'MARFC',
    'VA',
    NULL,
    'RAMSEYS DRAFT AT ROUTE 716 NEAR WEST AUGUSTA VA',
    38.2759,
    -79.3353
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RMSV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TURV2',
    '01655794',
    'MARFC',
    'VA',
    NULL,
    'TURKEY RUN AT ROUTE 643 NEAR CATLETT VA',
    38.6757,
    -77.7475
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TURV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13107201,
    'NW813',
    '05365550',
    'NCRFC',
    'WI',
    '7050005',
    'CHIPPEWA RIVER BELOW DELLS DAM AT EAU CLAIRE~ WI',
    44.83027,
    -91.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW813'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13107207,
    'NW814',
    '05366800',
    'NCRFC',
    'WI',
    '17050005',
    'CHIPPEWA R AT GRAND AVE AT EAU CLAIRE~ WI',
    44.81333,
    -91.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW814'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16630360,
    'NW867',
    '02239600',
    'SERFC',
    'FL',
    '3080102',
    'TRIBUTARY TO SILVER RIVER AT SH 40 NEAR OCALA FL',
    29.22027,
    -82.03388
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW867'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18223461,
    'NW894',
    '02461130',
    'SERFC',
    'AL',
    '3160112',
    'VALLEY CREEK AT CENTER ST AT BIRMINGHAM~ AL',
    33.50833,
    -86.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW894'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9017391,
    'NW747',
    '04108872',
    'NCRFC',
    'MI',
    '4050002',
    'PIGEON RIVER AT WEST OLIVE RD NR PORT SHELDON~ MI',
    42.915,
    -86.13555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW747'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12006203,
    'NW796',
    '04084927',
    'NCRFC',
    'WI',
    '4030204',
    'WEST PLUM CREEK AT NEW ROAD NEAR WRIGHTSTOWN~ WI',
    44.28805,
    -88.18638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW796'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13030921,
    'NW811',
    '04144032',
    'NCRFC',
    'MI',
    '4080203',
    'THREEMILE CREEK AT PRIOR ROAD NEAR DURAND~ MI',
    42.88111,
    -83.99972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW811'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18566335,
    'NW912',
    '02458190',
    'SERFC',
    'AL',
    '3160111',
    'TRIB TO VILLAGE CREEK AT 50th ST IN BIRMINGHAM',
    33.54222,
    -86.7625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW912'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'IRBN6',
    '0423205040',
    'NERFC',
    'NY',
    NULL,
    'IRONDEQUOIT BAY AT MOUTH AT ROCHESTER NY',
    43.23489,
    -77.53386
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IRBN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCIW1',
    '453845121564001',
    'NWRFC',
    'WA',
    NULL,
    'COLUMBIA RIVER AT CASCADE ISLAND WA',
    45.64583,
    -121.94444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCIW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPTW1',
    '14240311',
    'NWRFC',
    'WA',
    NULL,
    'SPIRIT LAKE TUNNEL OUTLET NR SPIRIT LAKE WA',
    46.28222,
    -122.19528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPTW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BABW1',
    '12190400',
    'NWRFC',
    'WA',
    NULL,
    'BAKER RIVER AB BLUM CREEK NEAR CONCRETE WA',
    48.75417,
    -121.54583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BABW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EPAW1',
    '12046260',
    'NWRFC',
    'WA',
    NULL,
    'ELWHA RIVER AT DIVERSION NEAR PORT ANGELES WA',
    48.11222,
    -123.55083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EPAW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LSTW1',
    '12091290',
    'NWRFC',
    'WA',
    NULL,
    'LEACH CR AT MEADOW PARK GC AT UNIVERSITY PLACE WA',
    47.19861,
    -122.51972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSTW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PFSW1',
    '12101470',
    'NWRFC',
    'WA',
    NULL,
    'PUYALLUP RIVER AT 5TH ST BRIDGE AT PUYALLUP WA',
    47.19889,
    -122.28611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PFSW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PMNW1',
    '12096505',
    'NWRFC',
    'WA',
    NULL,
    'PUYALLUP RIVER AT E MAIN BRIDGE AT PUYALLUP WA',
    47.19694,
    -122.24972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PMNW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SNCW1',
    '12166185',
    'NWRFC',
    'WA',
    NULL,
    'NF STILLAGUAMISH R AT C-POST BRIDGE NEAR OSO WA',
    48.28359,
    -121.83052
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPIW1',
    '14240304',
    'NWRFC',
    'WA',
    NULL,
    'SPIRIT LAKE AT TUNNEL AT SPIRIT LAKE WA',
    46.27639,
    -122.16139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPIW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CROW1',
    '12028060',
    'NWRFC',
    'WA',
    NULL,
    'CHEHALIS RIVER NEAR ROCHESTER WA',
    46.806944,
    -123.118889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CROW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18588458,
    'NW913',
    '02449838',
    'SERFC',
    'AL',
    '3160109',
    'DUCK RIVER ABOVE HWY 278 NEAR BERLIN~ ALA.',
    34.16944,
    -86.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW913'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12260370,
    'NW807',
    '040975299',
    'NCRFC',
    'MI',
    '4050001',
    'PRAIRIE RIVER AT US 12 NEAR BURR OAK~ MI',
    41.84722,
    -85.27111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW807'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889676,
    'NW609',
    '11313433',
    'CNRFC',
    'CA',
    '18040003',
    'DUTCH SLOUGH BL JERSEY ISLAND RD A JERSEY ISLAND',
    38.0,
    -121.67777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW609'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889742,
    'NW612',
    '11313434',
    'CNRFC',
    'CA',
    '18040003',
    'OLD R A QUIMBY ISLAND NR BETHEL ISLAND CA',
    38.01694,
    -121.55916
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW612'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1909508,
    'NW617',
    '11313440',
    'CNRFC',
    'CA',
    '18040003',
    'FALSE R NR OAKLEY CA',
    38.05083,
    -121.67777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW617'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5330719,
    'NW659',
    '11460151',
    'CNRFC',
    'CA',
    '18050005',
    'REDWOOD C A HWY 1 BRIDGE A MUIR BEACH CA',
    37.86416,
    -122.57611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW659'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2552667,
    'NW624',
    '11509200',
    'CNRFC',
    'OR',
    '18010204',
    'ADY CANAL AT HIGHWAY 97~ NEAR WORDEN~ OR',
    42.06777,
    -121.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW624'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 230619,
    'NW565',
    '394329104490101',
    'MBRFC',
    'CO',
    '10190003',
    'TOLL GATE CREEK ABOVE 6TH AVE AT AURORA~ CO',
    39.72861,
    -104.83027
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW565'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16248681,
    'NW866',
    '06340590',
    'MBRFC',
    'ND',
    '10130201',
    'KNIFE RIVER NR STANTON~ ND',
    47.35583,
    -101.38972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW866'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17952389,
    'NW889',
    '07288068',
    'LMRFC',
    'MS',
    '8030207',
    'HARRIS BAYOU AT PALMER RD EAST OF ALLIGATOR~ MS',
    34.06777,
    -90.62694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW889'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSSW1',
    '12131500',
    'NWRFC',
    'WA',
    NULL,
    'SOUTH FORK SKYKOMISH RIVER AT SKYKOMISH WA',
    47.710833,
    -121.359722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSSW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1543641,
    'RSKO2',
    '07246500',
    'ABRFC',
    'OK',
    NULL,
    'ARKANSAS RIVER NEAR SALLISAW OK',
    35.34972,
    -94.77111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSKO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCMS1',
    '330428079214800',
    'SERFC',
    'SC',
    NULL,
    'SKRINE CREEK NEAR MCCLELLENVILLESC',
    33.07444,
    -79.36333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCMS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPRS1',
    '021720711',
    'SERFC',
    'SC',
    NULL,
    'COOPER RIVER AT CUSTOMS HOUSE AT CHARLESTON SC',
    32.78028,
    -79.92389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VCKW1',
    '12061250',
    'NWRFC',
    'WA',
    NULL,
    'VANCE CREEK ABOVE KIRKLAND CREEK NEAR POTLATCH WA',
    47.32251,
    -123.2861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VCKW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRCW1',
    '12098920',
    'NWRFC',
    'WA',
    NULL,
    'WHITE RIVER FLUME AT BUCKLEY WA',
    47.16972,
    -122.01083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BKMW4',
    '13011820',
    'NWRFC',
    'WY',
    NULL,
    'BLACKROCK CR BL SPLIT ROCK CR NR MORAN WY',
    43.80711,
    -110.17872
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKMW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCJW4',
    '13018250',
    'NWRFC',
    'WY',
    NULL,
    'FLAT CREEK ABOVE CACHE CREEK AT JACKSON WY',
    43.48738,
    -110.76328
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCJW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRUW4',
    '13014300',
    'NWRFC',
    'WY',
    NULL,
    'GROS VENTRE R AB UPPER SLIDE LK NR KELLY WY',
    43.57597,
    -110.31094
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRUW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCMW4',
    '13012465',
    'NWRFC',
    'WY',
    NULL,
    'LEIDY CREEK AT MOUTH NEAR MORAN WY',
    43.73311,
    -110.31456
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCMW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCMW4',
    '13012475',
    'NWRFC',
    'WY',
    NULL,
    'SOUTH FORK SPREAD CR AT MOUTH NR MORAN WY',
    43.76348,
    -110.32379
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCMW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BSKW4',
    '09219200',
    'CBRFC',
    'WY',
    NULL,
    'BLACKS FORK ABOVE SMITHS FORK NEAR LYMAN WY',
    41.39415,
    -110.20596
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSKW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VGTA1',
    '02423110',
    'SERFC',
    'AL',
    NULL,
    'CAHABA RIVER NEAR TRUSSVILLE ALA',
    33.66556,
    -86.59222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VGTA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PLHC1',
    '11446030',
    'CNRFC',
    'CA',
    NULL,
    'SF AMERICAN R NR PILOT HILL CA',
    38.76306,
    -121.00722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PLHC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLXF1',
    '262528080202700',
    'SERFC',
    'FL',
    NULL,
    'SOUTH LOXAHATCHEE CONSERVATION AREA NO. 1',
    26.42139,
    -80.33786
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLXF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LDLW2',
    '01599200',
    'MARFC',
    'WV',
    NULL,
    'LINTON CREEK NEAR LAUREL DALE WV',
    39.269444,
    -79.131389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LDLW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13633821,
    'NW827',
    '05406479',
    'NCRFC',
    'WI',
    '7070005',
    'BLACK EARTH CK NR TREATMENT PLNT @ CROSS PLAINS~WI',
    43.10166,
    -89.66083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW827'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BPAD2',
    '01651827',
    'MARFC',
    'DC',
    NULL,
    'ANACOSTIA RIVER NR BUZZARD POINT AT WASHINGTON DC',
    38.865278,
    -77.010278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BPAD2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17956783,
    'NW890',
    '07288521',
    'LMRFC',
    'MS',
    '8030207',
    'PORTER BAYOU AT STEPHENSVILLE~ MS',
    33.54222,
    -90.67777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW890'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18370392,
    'NW899',
    '09179450',
    'CBRFC',
    'CO',
    '14030004',
    'DOLORES RIVER NEAR GATEWAY~ CO',
    38.67777,
    -108.96583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW899'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18377596,
    'NW900',
    '09171310',
    'CBRFC',
    'CO',
    '14030003',
    'SOUTH FORK SAN MIGUEL RIVER NEAR OPHIR~ CO',
    37.88111,
    -107.89805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW900'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15059759,
    'NW847',
    '11447830',
    'CNRFC',
    'CA',
    '18020163',
    'SUTTER SLOUGH A COURTLAND CA',
    38.32194,
    -121.57611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW847'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15059771,
    'NW849',
    '11447850',
    'CNRFC',
    'CA',
    '18020163',
    'STEAMBOAT SLOUGH NR WALNUT GROVE CA',
    38.28805,
    -121.59305
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW849'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15059791,
    'NW850',
    '11447903',
    'CNRFC',
    'CA',
    '18040012',
    'GEORGIANA SLOUGH NR SACRAMENTO R',
    38.23722,
    -121.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW850'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UNCC3',
    '01208410',
    'NERFC',
    'CT',
    NULL,
    'HOP BROOK LAKE NR UNION CITY CT',
    41.51472,
    -73.06667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UNCC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WATC3',
    '01208171',
    'NERFC',
    'CT',
    NULL,
    'NAUGATUCK RIVER AT WATERBURY CT',
    41.55694,
    -73.05472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WATC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BHSF1',
    '02322688',
    'SERFC',
    'FL',
    NULL,
    'BLUE HOLE SPRING NR HILDRETH FL',
    29.97972,
    -82.75861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BHSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDDF1',
    '255200080405001',
    'SERFC',
    'FL',
    NULL,
    'EDEN 8 IN WATER CONSERVATION AREA 3-A',
    25.86667,
    -80.68056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NNBF1',
    '261200080275001',
    'SERFC',
    'FL',
    NULL,
    'N. NEW RIVER CANAL AT S-11-B NR ANDYTOWN FL',
    26.20222,
    -80.45361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NNBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NNCF1',
    '261300080280001',
    'SERFC',
    'FL',
    NULL,
    'N. NEW RIVER CANAL AT S-11-C NR ANDYTOWN FL',
    26.22861,
    -80.46028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NNCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NORF1',
    '261150080270001',
    'SERFC',
    'FL',
    NULL,
    'N. NEW RIVER CANAL AT S-11-A NR ANDYTOWN FL',
    26.17778,
    -80.44806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NORF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BIGW1',
    '12035450',
    'NWRFC',
    'WA',
    NULL,
    'BIG CREEK NEAR GRISDALE WA',
    47.37444,
    -123.63556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BIGW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15048189,
    'NW843',
    '11455335',
    'CNRFC',
    'CA',
    '18020163',
    'SACRAMENTO R DEEP WATER SHIP CHANNEL NR RIO VISTA',
    38.25416,
    -121.67777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW843'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15048247,
    'NW845',
    '11455420',
    'CNRFC',
    'CA',
    '18020163',
    'SACRAMENTO R A RIO VISTA CA',
    38.13555,
    -121.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW845'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23065017,
    'NW978',
    '12444550',
    'NWRFC',
    'WA',
    '17020006',
    'BONAPARTE CREEK AT TONASKET~ WA',
    48.71166,
    -119.44055
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW978'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDHF1',
    '262258080273501',
    'SERFC',
    'FL',
    NULL,
    'EDEN 11 IN WATER CONSERVATION AREA 2-A',
    26.37639,
    -80.45528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ERSF1',
    '02291597',
    'SERFC',
    'FL',
    NULL,
    'SOUTH BRANCH ESTERO RIVER AT ESTERO FL',
    26.42875,
    -81.79344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ERSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SEEF1',
    '255014080355801',
    'SERFC',
    'FL',
    NULL,
    'TI-9 IN WATER CONSERVATION AREA 3-B',
    25.83769,
    -80.59933
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SEEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CNTF1',
    '02303424',
    'SERFC',
    'FL',
    NULL,
    'CYPRESS CR AT COUNTY LINE RD AT WESLEY CHAPEL FL',
    28.17131,
    -82.38861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNTF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSRF1',
    '02236350',
    'SERFC',
    'FL',
    NULL,
    'GREEN SWAMP RUN NEAR EVA FL',
    28.31083,
    -81.68556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SISF1',
    '285531082412600',
    'SERFC',
    'FL',
    NULL,
    'CRYSTAL RV AT MOUTH NR SHELL ISL NR CRYSTAL RV FL',
    28.925278,
    -82.690556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SISF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CYTF1',
    '02310752',
    'SERFC',
    'FL',
    NULL,
    'SALT RIVER NEAR CRYSTAL RIVER FL',
    28.901667,
    -82.645833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CYTF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CNMF1',
    '02299735',
    'SERFC',
    'FL',
    NULL,
    'VENICE INLET AT CROW''S NEST MARINA AT VENICE FL',
    27.112222,
    -82.465556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NWRF1',
    '02283500',
    'SERFC',
    'FL',
    NULL,
    'N NEW RIVER CANAL BELOW S351 NR SOUTH  BAY FLA',
    26.697222,
    -80.713889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NWRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CKBF1',
    '02310742',
    'SERFC',
    'FL',
    NULL,
    'CRYSTAL RIVER AT MOUTH OF KINGS BAY FL',
    28.893333,
    -82.605556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CKBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DGIF1',
    '02310673',
    'SERFC',
    'FL',
    NULL,
    'CHASSAHOWITZKA R AT DOG ISL NR CHASSAHOWITZKA FL',
    28.7025,
    -82.639167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DGIF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWFI4',
    '05480930',
    'NCRFC',
    'IA',
    NULL,
    'WHITE FOX CREEK AT CLARION IA',
    42.731917,
    -93.707436
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWFI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWCI4',
    '06605750',
    'MBRFC',
    'IA',
    NULL,
    'WILLOW CREEK NEAR CORNELL IA',
    42.9725,
    -95.1611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWCI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ECLI4',
    '06602190',
    'MBRFC',
    'IA',
    NULL,
    'ELLIOTT CREEK AT LAWTON IA',
    42.475,
    -96.1894
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ECLI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15048245,
    'NW844',
    '11447905',
    'CNRFC',
    'CA',
    '18020163',
    'SACRAMENTO R BL GEORGIANA SLOUGH CA',
    38.23722,
    -121.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW844'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15048187,
    'NW842',
    '11455315',
    'CNRFC',
    'CA',
    '18020163',
    'CACHE SLOUGH A S LIBERTY ISLAND NR RIO VISTA CA',
    38.23722,
    -121.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW842'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15048263,
    'NW846',
    '11337080',
    'CNRFC',
    'CA',
    '18020163',
    'THREEMILE SLOUGH NR RIO VISTA CA',
    38.10166,
    -121.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW846'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23399221,
    'NW985',
    '13212890',
    'NWRFC',
    'ID',
    '17050114',
    'DIXIE DRAIN NR WILDER ID',
    43.72861,
    -116.89805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW985'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23503176,
    'NW987',
    '13306336',
    'NWRFC',
    'ID',
    '17060203',
    'BLACKBIRD CREEK NEAR COBALT~ ID',
    45.08472,
    -114.28805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW987'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23503170,
    'NW986',
    '13306370',
    'NWRFC',
    'ID',
    '17060203',
    'PANTHER CREEK AT COBALT~ ID',
    45.08472,
    -114.23722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW986'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AGRP4',
    '50011180',
    NULL,
    'PR',
    NULL,
    'CANAL DE AGUADILLA ABV LAGO RAMEY AGUADILLA PR',
    18.47464,
    -67.11569
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AGRP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ARHP4',
    '50027000',
    NULL,
    'PR',
    NULL,
    'RIO LIMON ABV LAGO DOS BOCAS PR',
    18.32609,
    -66.62103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ARHP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRRS1',
    '02162285',
    'SERFC',
    'SC',
    NULL,
    'TABLE ROCK RESERVOIR NR CLEVELAND SC',
    35.06417,
    -82.6725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LIVI3',
    '04093100',
    'NCRFC',
    'IN',
    NULL,
    'DEEP RIVER NEAR LIVERPOOL IN',
    41.56556,
    -87.29444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LIVI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCNI3',
    '05536191',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET R AT NORTHCOTE AV AT MUNSTER IN',
    41.5665,
    -87.4857
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCNI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MDRI3',
    '04092944',
    'NCRFC',
    'IN',
    NULL,
    'DEEP RIVER AT GRAND BLVD AT MERRILLVILLE IN',
    41.4524,
    -87.2583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MDRI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BNWK2',
    '03311505',
    'OHRFC',
    'KY',
    NULL,
    'GREEN RIVER AT WATER PLANT AT BROWNSVILLE KY',
    37.203056,
    -86.259722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BNWK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DOTK2',
    '03435105',
    'OHRFC',
    'KY',
    NULL,
    'RED RIVER AT DOT KY',
    36.676667,
    -86.952222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DOTK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23551760,
    'NW988',
    '13311250',
    'NWRFC',
    'ID',
    '17060208',
    'EFSF SALMON R ABV SUGAR CRK NR STIBNITE~ ID',
    44.94888,
    -115.33888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW988'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23681319,
    'NW990',
    '14043840',
    'NWRFC',
    'OR',
    '17070203',
    'MF JOHN DAY RIVER ABV CAMP CREEK~ NR GALENA~ OR',
    44.69472,
    -118.79638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW990'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22893835,
    'NW974',
    '480608115242901',
    'NWRFC',
    'MT',
    '17010102',
    'Libby Wetland Site bl Schrieber Lake nr Libby~ MT',
    48.10166,
    -115.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW974'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9768048,
    'NW752',
    '383926107593001',
    'CBRFC',
    'CO',
    '14020006',
    'LOUTSENHIZER ARROYO AT HWY 50 NEAR OLATHE CO',
    38.66083,
    -107.99972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW752'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13294132,
    'NW818',
    '05427880',
    'NCRFC',
    'WI',
    '7090002',
    'SIXMILE CREEK AT STATE HIGHWAY 19 NEAR WAUNAKEE~WI',
    43.18638,
    -89.47444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW818'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13294166,
    'NW819',
    '05427910',
    'NCRFC',
    'WI',
    '7090002',
    'SIXMILE CREEK @ COUNTY TRNK HGHWY M NR WAUNAKEE~WI',
    43.13555,
    -89.44055
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW819'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2648922,
    'NW626',
    NULL,
    'NCRFC',
    'WI',
    '7030005',
    'DRY RUN AT 190TH STREET NEAR JEWETT~ WI',
    45.10166,
    -92.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW626'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21680904,
    'NW954',
    NULL,
    'SERFC',
    'AL',
    '3150201',
    'AUTAUGA CREEK NEAR PRATTVILLE~ ALA',
    32.47444,
    -86.49138
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW954'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16697771,
    'NW871',
    '02248350',
    'SERFC',
    'FL',
    '3080202',
    'TURNBULL CREEK NR OAK HILL~ FL',
    28.83027,
    -80.86416
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW871'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21538554,
    'NW951',
    '06332523',
    'MBRFC',
    'ND',
    '10110101',
    'E. FORK SHELL CREEK NR PARSHALL~ ND',
    47.94888,
    -102.20333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW951'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21539336,
    'NW952',
    '06332770',
    'MBRFC',
    'ND',
    '10110101',
    'DEEPWATER CREEK AT MOUTH NR RAUB~ ND',
    47.74555,
    -102.10166
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW952'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22201495,
    'NW960',
    '02401895',
    'SERFC',
    'AL',
    '3150106',
    'OHATCHEE CREEK AT OHATCHEE~ ALA.',
    33.77944,
    -85.99972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW960'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11937543,
    'NW794',
    '04043016',
    'NCRFC',
    'MI',
    '4020103',
    'PILGRIM RIVER AT PARADISE RD NR DODGEVILLE~ MI',
    47.08472,
    -88.55916
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW794'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'POCS1',
    '02135615',
    'SERFC',
    'SC',
    NULL,
    'POCOTALIGO RIVER AT I-95 ABOVE MANNINGSC',
    33.72964,
    -80.22019
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'POCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRSK2',
    '03288110',
    'OHRFC',
    'KY',
    NULL,
    'ROYAL SPRINGS AT GEORGETOWN KY',
    38.20944,
    -84.56194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRSK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LXTK2',
    '380249084295001',
    'OHRFC',
    'KY',
    NULL,
    'DOWNTOWN LEX. @ LFUCG BLDG ON E MAIN ST @ LEX. KY',
    38.04694,
    -84.49722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LXTK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15644352,
    'NW859',
    '04188399',
    'OHRFC',
    'OH',
    '4100008',
    'The Outlet near Findlay OH',
    41.03388,
    -83.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW859'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCCL1',
    '073802305',
    'LMRFC',
    'LA',
    NULL,
    '(COE) LAKE PONTCHARTRAIN AT FRENIER LA',
    30.545556,
    -91.874444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCCL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBOM6',
    '02484600',
    'LMRFC',
    'MS',
    NULL,
    'COFFEE BOGUE AT LUDLOW MS',
    32.573889,
    -89.729722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBOM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCFM6',
    '02485730',
    'LMRFC',
    'MS',
    NULL,
    'HOG CREEK AT FLOWOOD DRIVE AT LUCKNEY MS',
    32.3333,
    -90.0838
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCFM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5867675,
    'NW673',
    '01103040',
    'NERFC',
    'MA',
    '1090001',
    'MYSTIC RIVER RT 16 AT MEDFORD~ MA',
    42.40666,
    -71.08472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW673'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11936931,
    'NW791',
    '04040304',
    'NCRFC',
    'MI',
    '4020103',
    'MONTREAL RIVER AT LAC LA BELLE RD NR DELAWARE~ MI',
    47.42361,
    -88.06777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW791'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BFHL1',
    '302320091465900',
    'LMRFC',
    'LA',
    NULL,
    'BAYOU FUSILIER OF THE SWAMPS NEAR HENDERSON LA',
    30.38889,
    -91.78306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BFHL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FAYM6',
    '07290830',
    'LMRFC',
    'MS',
    NULL,
    'LITTLE CREEK NR FAYETTE MS',
    31.67528,
    -91.06944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FAYM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10337790,
    'NW759',
    '03343820',
    'OHRFC',
    'IL',
    '5120112',
    'KICKAPOO CREEK AT 1320E ROAD NR CHARLESTON~ IL',
    39.4575,
    -88.22027
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW759'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11688828,
    'NW780',
    '01589238',
    'MARFC',
    'MD',
    '2060003',
    'GWYNNS FALLS TRIBUTARY AT MCDONOGH~ MD',
    39.40666,
    -76.77944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW780'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7766307,
    'NW723',
    '07257473',
    'ABRFC',
    'AR',
    '11110202',
    'Mill Creek near Hector~ AR',
    35.50833,
    -93.0
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW723'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16247013,
    'NW865',
    '06339960',
    'MBRFC',
    'ND',
    '10130201',
    'GOODMAN CREEK NEAR GOLDEN VALLEY~ ND',
    47.28805,
    -102.08472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW865'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCTA3',
    '09404109',
    'CBRFC',
    'AZ',
    NULL,
    'CATARACT CREEK BELOW TOPOCOBA GORGE NEAR SUPAI AZ',
    36.18333,
    -112.65
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCTA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NCMA3',
    '09400815',
    'CBRFC',
    'AZ',
    NULL,
    'NEWMAN CANYON ABOVE UPPER LAKE MARY AZ',
    35.05494,
    -111.48922
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NCMA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SWBA3',
    '09424600',
    'CBRFC',
    'AZ',
    NULL,
    'LITTLE SYCAMORE WASH NEAR BAGDAD AZ',
    34.54917,
    -113.05778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SWBA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TTCA3',
    '09496910',
    'CBRFC',
    'AZ',
    NULL,
    'TURKEY TRACK CANYON NEAR CEDAR CREEK AZ',
    33.95523,
    -110.16937
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TTCA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ULSA3',
    '09424580',
    'CBRFC',
    'AZ',
    NULL,
    'UPPER LITTLE SYCAMORE WASH NEAR BAGDAD AZ',
    34.57074,
    -113.06756
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ULSA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VAVA3',
    '09504950',
    'CBRFC',
    'AZ',
    NULL,
    'VERDE RIVER ABOVE CAMP VERDE AZ',
    34.6117,
    -111.89843
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VAVA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DUTC2',
    '06709910',
    'MBRFC',
    'CO',
    NULL,
    'DUTCH CR AT PLATTE CANYON DRIVE NEAR LITTLETON CO',
    39.60011,
    -105.04192
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DUTC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ELTC2',
    '09056500',
    'CBRFC',
    'CO',
    NULL,
    'ELLIOTT CREEK FEEDER CANAL TO GREEN MTN RES CO',
    39.87373,
    -106.3306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELTC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WOBF1',
    '02237734',
    'SERFC',
    'FL',
    NULL,
    'WOLF BRANCH AT FCRR NEAR MOUNT DORA FL',
    28.79639,
    -81.60806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WOBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BSDF1',
    '02319950',
    'SERFC',
    'FL',
    NULL,
    'BLUE SPRINGS NEAR DELLFL',
    30.125833,
    -83.226111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCEF1',
    '02245340',
    'SERFC',
    'FL',
    NULL,
    'ST JOHNS R BLW SHANDS BRIDGE NR GRN COVE SPRG FL',
    30.005833,
    -81.615
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRPF1',
    '02245290',
    'SERFC',
    'FL',
    NULL,
    'ST JOHNS RIVER AT RACY PT NEAR HASTINGS FL',
    29.8,
    -81.55
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRJF1',
    '02246621',
    'SERFC',
    'FL',
    NULL,
    'TROUT R NR JACKSONVILLE FLA',
    30.417222,
    -81.696667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRJF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JBLM6',
    '02485820',
    'LMRFC',
    'MS',
    NULL,
    'BELHAVEN CREEK AT LAUREL STREET AT JACKSON MS',
    32.318778,
    -90.162361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JBLM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JBRM6',
    '02485810',
    'LMRFC',
    'MS',
    NULL,
    'BELHAVEN CREEK AT RIVERSIDE DRIVE AT JACKSON MS',
    32.323528,
    -90.171694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JBRM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13203597,
    'NW816',
    '04157060',
    'NCRFC',
    'MI',
    '4080206',
    'SAGINAW RIVER AT MIDLAND STREET AT BAY CITY~ MI',
    43.61,
    -83.89805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW816'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11937025,
    'NW792',
    '04040260',
    'NCRFC',
    'MI',
    '4020103',
    'GRATIOT RIVER AT 5 MILE POINT ROAD NEAR AHMEEK~ MI',
    47.32194,
    -88.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW792'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13311203,
    'NW821',
    '05411900',
    'NCRFC',
    'IA',
    '7060004',
    'Otter Creek at Elgin~ IA',
    42.96583,
    -91.64388
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW821'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11937419,
    'NW793',
    '04043021',
    'NCRFC',
    'MI',
    '4020103',
    'COLE CREEK AT HOUGHTON CANAL ROAD NR HOUGHTON~ MI',
    47.11861,
    -88.62694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW793'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1529633,
    'NW597',
    '07100750',
    'ABRFC',
    'CO',
    '11020003',
    'WALDO CANYON ABV MOUTH NEAR MANITOU SPRINGS~ CO',
    38.89805,
    -104.94888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW597'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889680,
    'NW610',
    '11313431',
    'CNRFC',
    'CA',
    '18040003',
    'HOLLAND CUT NR BETHEL ISLAND CA',
    38.0,
    -121.57611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW610'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13463761,
    'NW824',
    '05536500',
    'NCRFC',
    'IL',
    '7120003',
    'TINLEY CREEK NEAR PALOS PARK~ IL',
    41.64388,
    -87.7625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW824'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MBSF1',
    '02358795',
    'SERFC',
    'FL',
    NULL,
    'JACKSON BLUE SPRING NR MARIANNA FL',
    30.79028,
    -85.14083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MBSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RIPS2',
    '435442103200700',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP AT DEADMAN GULCH NR HAYWARD SD',
    43.91167,
    -103.33528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RIPS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ANOF1',
    '02309848',
    'SERFC',
    'FL',
    NULL,
    'SOUTH BRANCH ANCLOTE RIVER NEAR ODESSA FL',
    28.18544,
    -82.55336
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ANOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BBHF1',
    '02294760',
    'SERFC',
    'FL',
    NULL,
    'BARBER BRANCH NEAR HOMELAND FL',
    27.83833,
    -81.81222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BOLF1',
    '02307362',
    'SERFC',
    'FL',
    NULL,
    'BROOKER CREEK AT TARPON WOODS BLVD NR OLDSMAR FL',
    28.08654,
    -82.69393
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSPF1',
    '02306000',
    'SERFC',
    'FL',
    NULL,
    'SULPHUR SPRINGS AT SULPHUR SPRINGS FL',
    28.02083,
    -82.45194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCKF1',
    '02303350',
    'SERFC',
    'FL',
    NULL,
    'TROUT CREEK NEAR SULPHUR SPRINGS FL',
    28.13472,
    -82.36194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCWF1',
    '02303348',
    'SERFC',
    'FL',
    NULL,
    'TROUT CREEK AT WESLEY CHAPEL FL',
    28.17133,
    -82.36222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCWF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCYF1',
    '02303410',
    'SERFC',
    'FL',
    NULL,
    'CYPRESS CREEK TRIBUTARY NR WESLEY CHAPEL FL',
    28.25936,
    -82.38611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCYF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TSGF1',
    '02307359',
    'SERFC',
    'FL',
    NULL,
    'BROOKER CREEK NEAR TARPON SPRINGS FL',
    28.09614,
    -82.68732
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TSGF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WMSF1',
    '02299260',
    'SERFC',
    'FL',
    NULL,
    'WARM MINERAL SPRINGS NEAR WOODMERE FL',
    27.06033,
    -82.26067
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WMSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BBCG1',
    '02203957',
    'SERFC',
    'GA',
    NULL,
    'BARBASHELA CR AT WOODWAY DR NR STONE MOUNTAIN GA',
    33.73833,
    -84.19139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BFCG1',
    '023362095',
    'SERFC',
    'GA',
    NULL,
    'BURNT FORK CR AT MILLWOOD WAY NEAR CLARKSTON GA',
    33.821389,
    -84.274722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BFCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BKCG1',
    '023362075',
    'SERFC',
    'GA',
    NULL,
    'BURNT FORK CREEK AT MONTREAL RD NEAR TUCKER GA',
    33.835278,
    -84.256944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCFG1',
    '03568400',
    'LMRFC',
    'GA',
    NULL,
    'CHATTANOOGA CREEK AT GA193 NEAR FLINTSTONE GA',
    34.9572,
    -85.3344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCFG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DLCG1',
    '02203831',
    'SERFC',
    'GA',
    NULL,
    'DOOLITTLE CREEK AT FLAT SHOALS RD NR DECATUR GA',
    33.705556,
    -84.2925
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DLCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPTG1',
    '02344673',
    'SERFC',
    'GA',
    NULL,
    'FLAT CREEK (DS OF LAKE) AT PEACHTREE CITY GA',
    33.383056,
    -84.572778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPTG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PIGN7',
    '0208732534',
    'SERFC',
    'NC',
    NULL,
    'PIGEON HOUSE CR AT CAMERON VILLAGE AT RALEIGH NC',
    35.787222,
    -78.654722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PIGN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19516502,
    'NW923',
    '03601600',
    'LMRFC',
    'TN',
    '6040003',
    'DUCK RIVER NEAR SHADY GROVE~ TN.',
    35.72861,
    -87.25416
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW923'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDWM6',
    '07288800',
    'LMRFC',
    'MS',
    NULL,
    'YAZOO RIVER AT REDWOOD MS',
    32.48722,
    -90.81722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDWM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PWRN7',
    '02126375',
    'SERFC',
    'NC',
    NULL,
    'PEE DEE R AT PEE DEE REFUGE NR ANSONVILLE NC',
    35.10322,
    -80.04593
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PWRN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MTOG1',
    '02340324',
    'SERFC',
    'GA',
    NULL,
    'MOUNTAIN OAK CREEK AT US 27 NEAR CHIPLEY GA',
    32.8325,
    -84.84361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTOG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NBSG1',
    '02392920',
    'SERFC',
    'GA',
    NULL,
    'NOONDAY CR US CHASTAIN MEADOWS PKWY MARIETTA GA',
    34.0218,
    -84.5529
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NBSG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NFBG1',
    '02336106',
    'SERFC',
    'GA',
    NULL,
    'N FK PEACHTREE CR AT BRIARWOOD RD NR ATLANTA GA',
    33.841745,
    -84.321903
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFBG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ETRI4',
    '06811800',
    'MBRFC',
    'IA',
    NULL,
    'EAST TARKIO CREEK NEAR STANTON IA',
    41.08,
    -95.0927
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ETRI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TFMN7',
    '0209725960',
    'SERFC',
    'NC',
    NULL,
    'THIRD FORK CREEK AT M.L. KING PARKWAY AT DURHAMNC',
    35.9511,
    -78.9269
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TFMN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OLTI2',
    '03612600',
    'LMRFC',
    'IL',
    NULL,
    'OHIO RIVER AT DAM 53 NEAR GRAND CHAIN IL',
    37.178611,
    -89.07
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OLTI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VRGK2',
    NULL,
    'OHRFC',
    'KY',
    NULL,
    'PRECIPITATION SITE AT VIRGIE KY',
    37.33167,
    -82.585
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VRGK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8834930,
    'RDDN7',
    '02108692',
    'SERFC',
    'NC',
    NULL,
    'Northeast Cape Fear River at Wilmington NC',
    34.25416,
    -77.96583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6331032,
    'CCJG1',
    '02208485',
    'SERFC',
    'GA',
    '3070103',
    'CORNISH CREEK AT LOWER JERSEY RD~ NR COVINGTON~ GA',
    33.71083,
    -83.81111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCJG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19451765,
    'NW920',
    '03111200',
    'OHRFC',
    'PA',
    '5030106',
    'Dunkle Run near Claysville~ PA',
    40.18638,
    -80.44055
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW920'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RXFN6',
    '01355475',
    'NERFC',
    'NY',
    NULL,
    'MOHAWK RIVER AT REXFORD NY',
    42.85111,
    -73.88725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RXFN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLSN7',
    '0214244102',
    'SERFC',
    'NC',
    NULL,
    'CATAWBA RIVER BL LOOKOUT SHOALS DAM NR SHARON NC',
    35.75719,
    -81.08915
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AARN5',
    '083299377',
    'WGRFC',
    'NM',
    NULL,
    'SAN ANTONIO ARROYO AT RIO GRANDE CONFLUENCE IN ABQ',
    35.13639,
    -106.68972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AARN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23844671,
    'NW992',
    '12039510',
    'NWRFC',
    'WA',
    '17100102',
    'COOK CREEK BLW HATCHERY NEAR QUINAULT~ WA',
    47.35583,
    -123.99972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW992'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19453867,
    'NW921',
    '03111955',
    'OHRFC',
    'WV',
    '5030106',
    'WHEELING CREEK NEAR MAJORSVILLE~ WV',
    39.96583,
    -80.54222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW921'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19744396,
    'NW929',
    '03485500',
    'LMRFC',
    'TN',
    '6010103',
    'DOE RIVER AT ELIZABETHTON~ TN',
    36.33888,
    -82.20333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW929'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12005339,
    'DEPW3',
    '04085119',
    'NCRFC',
    'WI',
    '4030204',
    'BOWER CREEK @ COUNTY TRNK HIGHWAY MM NR DE PERE WI',
    44.42249308,
    -87.9400994
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DEPW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18017020,
    'NW892',
    '07285400',
    'LMRFC',
    'MS',
    '8030205',
    'BATUPAN BOGUE AT GRENADA~ MS',
    33.77944,
    -89.79638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW892'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3432490,
    'FTBG1',
    '02342070',
    'SERFC',
    'GA',
    '3130003',
    'UPATOI CREEK AT GA 357  AT FORT BENNING  GA',
    32.37431279,
    -84.9579857
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FTBG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19531580,
    'WTTT1',
    '03597210',
    'LMRFC',
    'TN',
    '6040002',
    'GARRISON FORK ABOVE L&N RAILROAD AT WARTRACE  TN',
    35.51174039,
    -86.323882
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WTTT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2964434,
    'SBLW4',
    '06187950',
    'MBRFC',
    'WY',
    '10070001',
    'Soda Butte Cr nr Lamar Ranger Station YNP',
    44.86898889,
    -110.164775
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBLW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13624197,
    'NW826',
    '05408480',
    'NCRFC',
    'WI',
    '7070006',
    'W FORK KICKAPOO R AT PEACEFUL VALLEY RD NR CASHTON',
    43.69472,
    -90.79638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW826'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9699753,
    'BUCG1',
    '02157490',
    'SERFC',
    'GA',
    '3050107',
    'BEAVERDAM CREEK ABOVE GREER  SC',
    34.97527778,
    -82.1955556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BUCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18693687,
    'FAIM6',
    '02430615',
    'SERFC',
    'MS',
    '3160101',
    'MUD CREEK NR FAIRVIEW  MS',
    34.3925,
    -88.355
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FAIM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCFT2',
    '08180990',
    'WGRFC',
    'TX',
    NULL,
    'LEON CK AT IH 10 AND LOOP 1604 NR SAN ANTONIO TX',
    29.593056,
    -98.599167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15275898,
    'WOCM6',
    '07274252',
    'LMRFC',
    'MS',
    '8030203',
    'OTOUCALOFA CREEK CANAL NR WATER VALLEY  MS',
    34.14416667,
    -89.6525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WOCM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13551311,
    'KCTI2',
    '05579620',
    'NCRFC',
    'IL',
    '7130009',
    'KICKAPOO CREEK TRIBUTARY NEAR BLOOMINGTON  IL',
    40.47277778,
    -88.8797222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KCTI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ASPO2',
    '07329849',
    'ABRFC',
    'OK',
    NULL,
    '01S-03E-01 ABB 1 ANTELOPE SPRING AT SULPHUR OK',
    34.50444,
    -96.94111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ASPO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCGA2',
    '15272502',
    'AKRFC',
    'AK',
    NULL,
    'GLACIER C AT ALYESKA HIGHWAY AT GIRDWOOD AK',
    60.961667,
    -149.131389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCGA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12842790,
    'COTW4',
    '06265337',
    'MBRFC',
    'WY',
    '10080007',
    'COTTONWOOD C AT HIGH ISLAND RNCH NR HAMILTON DOME',
    43.76263889,
    -108.6778889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COTW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19696445,
    'NCCT1',
    '03566525',
    'LMRFC',
    'TN',
    '6020001',
    'NORTH CHICKAMAUGA CREEK NEAR MONTLAKE  TN',
    35.21111,
    -85.21528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NCCT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TBGN5',
    '08308050',
    'WGRFC',
    'NM',
    NULL,
    'RIO TESUQUE BELOW DIVERSIONS NEAR SANTA FE NM',
    35.77152,
    -105.9412
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBGN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COUO2',
    '07315650',
    'ABRFC',
    'OK',
    NULL,
    'RED RIVER NR COURTNEY OK',
    33.9175,
    -97.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COUO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 25386921,
    'BTEL1',
    '073815945',
    'LMRFC',
    'LA',
    NULL,
    'BAYOU TECHE E OF CALUMET FLOOD GATE AT CALUMET LA',
    29.70417,
    -91.37389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BTEL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WOON5',
    '08330200',
    'WGRFC',
    'NM',
    NULL,
    'SAN JOSE DRAIN AT WOODWARD RD AT ALBQ. NM',
    35.04889,
    -106.64861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WOON5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17003688,
    'SJFN5',
    '09367540',
    'CBRFC',
    'NM',
    NULL,
    'SAN JUAN R NR FRUITLAND NM',
    36.74028,
    -108.4025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SJFN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRLN7',
    '0208734210',
    'SERFC',
    'NC',
    NULL,
    'WALNUT CREEK AT TRAILWOOD DRIVE AT RALEIGH NC',
    35.768333,
    -78.691111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRLN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WAWN7',
    '0208734795',
    'SERFC',
    'NC',
    NULL,
    'WALNUT CREEK AT SOUTH WILMINGTON ST AT RALEIGH NC',
    35.756939,
    -78.64067
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WAWN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WFGN5',
    '09430010',
    'CBRFC',
    'NM',
    NULL,
    'WEST FORK GILA RIVER AT GILA CLIFF DWELLINGS NM',
    33.229444,
    -108.265556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WFGN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EFGN5',
    '09430030',
    'CBRFC',
    'NM',
    NULL,
    'GILA RIVER NR GILA HOT SPRINGS NM',
    33.17991,
    -108.20646
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EFGN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19662717,
    'DLPT1',
    '03570835',
    'LMRFC',
    'TN',
    '6020004',
    'SEQUATCHIE RIVER NEAR DUNLAP',
    35.3597242,
    -85.37222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DLPT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18395348,
    'PWTT1',
    '03433637',
    'OHRFC',
    'TN',
    '5130204',
    'SOUTH HARPETH CREEK NEAR PEWITT CHAPEL',
    35.91173135,
    -87.13084
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PWTT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13551309,
    'BKCI2',
    '05579630',
    'NCRFC',
    'IL',
    '7130009',
    'KICKAPOO CREEK NEAR BLOOMINGTON  IL',
    40.4583333,
    -88.8775
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKCI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13551289,
    'BKDI2',
    '05579610',
    'NCRFC',
    'IL',
    '7130009',
    'KICKAPOO CREEK AT 2100E ROAD NEAR BLOOMINGTON  IL',
    40.46777778,
    -88.86388889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKDI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19454497,
    'NW922',
    '03111675',
    'OHRFC',
    'PA',
    '5030106',
    'Job Creek at Delphene~ PA',
    39.84722,
    -80.38972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW922'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16039508,
    'NW864',
    '06463670',
    'MBRFC',
    'NE',
    '10150004',
    'Willow Creek at Atwood Rd nr Carns~ Nebr.',
    42.72861,
    -99.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW864'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12027586,
    'GOMM4',
    '04043140',
    'NCRFC',
    'MI',
    '4020105',
    'GOMANCHE CREEK AT INDIAN ROAD NEAR L''ANSE  MI',
    46.75104286,
    -88.3617967
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GOMM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1446974,
    'AVLT2',
    '08063562',
    'WGRFC',
    'TX',
    '12030109',
    'Chambers Ck at FM 55 nr Avalon~ TX',
    32.16465,
    -96.76201
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AVLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10313558,
    'PAWV1',
    '04280350',
    'NERFC',
    'VT',
    '4150401',
    'METTAWEE RIVER NEAR PAWLET  VT',
    43.37062828,
    -73.21621889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAWV1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1268356,
    'BSGT2',
    '0804956950',
    'WGRFC',
    'TX',
    '12030102',
    'Bear Ck at Shady Grove Rd~ Grand Prairie~ TX',
    32.8045,
    -97.00988
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSGT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2568024,
    'CPKT2',
    '08099382',
    'WGRFC',
    'TX',
    '12070201',
    'Copperas Ck at FM 2247 nr Comanche~ TX',
    32.04903,
    -98.64606
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPKT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1285377,
    'DCWT2',
    '08053430',
    'WGRFC',
    'TX',
    '12030104',
    'Denton Ck at CR 2513 nr Decatur~ TX',
    33.27792,
    -97.41704
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1268382,
    'GLTT2',
    '08049300',
    'WGRFC',
    'TX',
    '12030102',
    'W Fk Trinity Rv at Greenbelt Rd~ Fort Worth~ TX',
    32.78833,
    -97.13972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLTT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1276822,
    'RRDT2',
    '08051135',
    'WGRFC',
    'TX',
    '12030103',
    'Elm Fk Trinity Rv at Greenbelt nr Pilot Point~ TX',
    33.34972,
    -97.03556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RRDT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5588596,
    'SDOT2',
    '08104300',
    'WGRFC',
    'TX',
    '12070203',
    'Salado Ck at Salado~ TX',
    30.94435,
    -97.53408
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SDOT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1292128,
    'WCMT2',
    '08059590',
    'WGRFC',
    'TX',
    '12030106',
    'Wilson Ck Dws of Hwy 75 at McKinney~ TX',
    33.18485,
    -96.63805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1439269,
    'BAHT2',
    '08072600',
    'WGRFC',
    'TX',
    '12040104',
    'Buffalo Bayou at State Hwy 6 nr Addicks~ TX',
    29.76938,
    -95.64317
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BAHT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5577721,
    'BUZT2',
    '08111054',
    'WGRFC',
    'TX',
    '12070103',
    'Bee Ck Trib A at College Station~ TX',
    30.59418,
    -96.29725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BUZT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5577677,
    'CBET2',
    '08111052',
    'WGRFC',
    'TX',
    '12070103',
    'Bee Ck Main at College Station~ TX',
    30.60556,
    -96.29864
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBET2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6203090,
    'NW687',
    '01367805',
    'NERFC',
    'NJ',
    '2020007',
    'Papakating Creek at Roys NJ',
    41.16944,
    -74.66083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW687'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7764181,
    'NW721',
    '07257460',
    'ABRFC',
    'AR',
    '11110202',
    'Mid Fork Ill Bayou Upstream of Hwy 27 nr Hector~AR',
    35.52527,
    -92.94888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW721'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1468038,
    'FCWT2',
    '08067920',
    'WGRFC',
    'TX',
    '12040101',
    'Lake Ck at Sendera Ranch Rd nr Conroe~ TX',
    30.25775,
    -95.56369
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1491354,
    'OALT2',
    '08066175',
    'WGRFC',
    'TX',
    '12030202',
    'Kickapoo Ck at Onalaska~ TX',
    30.88675,
    -95.08039
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OALT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1440261,
    'SMAT2',
    '08072680',
    'WGRFC',
    'TX',
    '12040104',
    'S Mayde Ck at Heathergold Dr nr Addicks~ TX',
    29.80511,
    -95.70961
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SMAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1128207,
    'ACST2',
    '07344100',
    'LMRFC',
    'TX',
    '11140302',
    'Anderson Ck at Hwy 98 nr Simms~ TX',
    33.38444,
    -94.49806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1129421,
    'CTCT2',
    '07343356',
    'LMRFC',
    'TX',
    '11140302',
    'Cuthand Ck at FM 910 nr Cuthand~ TX',
    33.49889,
    -95.05139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CTCT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1121321,
    'OMWT2',
    '07343840',
    'LMRFC',
    'TX',
    '11140303',
    'White Oak Ck at IH 30 nr Omaha~ TX',
    33.27472,
    -94.8025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OMWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4547166,
    'FENW2',
    '03188900',
    'OHRFC',
    'WV',
    '5050005',
    'LAUREL CREEK NEAR FENWICK~ WV',
    38.16361,
    -80.58806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FENW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11228832,
    'HRSN2',
    '10322505',
    'CNRFC',
    'NV',
    '16040104',
    'HORSE CK AT HORSE CYN NR GARDEN GATE PASS~ NV',
    40.11556,
    -116.50344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRSN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11230180,
    'TONN2',
    '10322510',
    'CNRFC',
    'NV',
    '16040104',
    'TONKIN SPG OUTFLOW ABV DENAY CK NR EUREKA~ NV',
    39.90481,
    -116.41253
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TONN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11228062,
    'WILN2',
    '103225055',
    'CNRFC',
    'NV',
    '16040104',
    'WILLOW CK AT ALLISON RANCH NR GARDEN GATE PASS~ NV',
    40.171,
    -116.48669
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WILN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22878631,
    'TBAM8',
    '12301250',
    'NWRFC',
    'MT',
    '17010101',
    'Tobacco River at Eureka~ MT',
    48.87795,
    -115.05446
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBAM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1826834,
    'PTOK2',
    '03284525',
    'OHRFC',
    'KY',
    '5100205',
    'E HICKMAN CR TRIB AT CHILESBURG RD NR LEXINGTON~KY',
    37.98833,
    -84.41111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTOK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1918988,
    'GTWO1',
    '03238495',
    'OHRFC',
    'OH',
    '5090201',
    'White Oak Creek above Georgetown OH',
    38.91972,
    -83.92833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GTWO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOEF1',
    '255634080450001',
    'SERFC',
    'FL',
    NULL,
    'W-11 IN WATER CONSERVATION AREA 3-A',
    25.94278,
    -80.75
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOFF1',
    '260007080464401',
    'SERFC',
    'FL',
    NULL,
    'W-18 IN WATER CONSERVATION AREA 3-A',
    26.00194,
    -80.77889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MUDF1',
    '251209080350100',
    'SERFC',
    'FL',
    NULL,
    'MUD CREEK AT MOUTH NR HOMESTEAD FL',
    25.2033,
    -80.58411
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MUDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCCF1',
    '251003080435500',
    'SERFC',
    'FL',
    NULL,
    'MCCORMICK CREEK AT MOUTH NEAR KEY LARGO FL',
    25.16819,
    -80.73359
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LXHF1',
    '02281200',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO CANAL AT S-6 NEAR SHAWANO',
    26.47167,
    -80.44611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LXHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 89592,
    'NBKF1',
    '251341080291200',
    'SERFC',
    'FL',
    NULL,
    'STILLWATER CREEK NEAR HOMESTEAD FL',
    25.2283,
    -80.48595
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NBKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCAF1',
    '02259100',
    'SERFC',
    'FL',
    NULL,
    'INDIAN PRAIRIE CANAL ABOVE S-72 NR OKEECHOBEE FL',
    27.09645,
    -81.00917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RBAF1',
    '251115081075800',
    'SERFC',
    'FL',
    NULL,
    'RAULERSON BROTHERS CANAL AT CAPE SABLE FL',
    25.18765,
    -81.13278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RBAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RBRF1',
    '02291001',
    'SERFC',
    'FL',
    NULL,
    'BARRON RIVER BELOW SR29-3 AT COPELAND FL',
    25.95794,
    -81.35519
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RBRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RPCF1',
    '02257750',
    'SERFC',
    'FL',
    NULL,
    'HARNEY POND CANAL 2.4 MILES BL S70 NR LAKEPORT FL',
    27.09083,
    -81.13272
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RPCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OTCA2',
    '15085697',
    'AKRFC',
    'AK',
    NULL,
    'HARRIS R AT LOGGING RD BRIDGE NR HOLLIS AK',
    55.395556,
    -132.406944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OTCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SACF1',
    '02289060',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL OUTLETS L-30 TO L-67A NR MIAMI FL',
    25.76111,
    -80.56111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SACF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RECF1',
    '02290928',
    'SERFC',
    'FL',
    NULL,
    'BARRON RIVER AT EVERGLADES CITY FL',
    25.86972,
    -81.3825
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RECF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24494446,
    'BSPI1',
    '13095175',
    'NWRFC',
    'ID',
    '17040212',
    'BRIGGS SPRING AT HEAD NEAR BUHL ID',
    42.67389,
    -114.80917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSPI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24382935,
    'MOTW1',
    '12449500',
    'NWRFC',
    'WA',
    '17020008',
    'METHOW RIVER AT TWISP  WA',
    48.36528,
    -120.115
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOTW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23659568,
    'WIMO3',
    '14034608',
    'NWRFC',
    'OR',
    '17070104',
    'WILLOW CREEK AT MORGAN STREET  AT HEPPNER  OR',
    45.36139,
    -119.55917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WIMO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10396113,
    'FAUU1',
    '10172727',
    'CBRFC',
    'UT',
    '16020304',
    'FAUST CREEK NEAR VERNON  UT',
    40.16056,
    -112.43028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FAUU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3197078,
    'MCWW4',
    '09218500',
    'CBRFC',
    'WY',
    '14040107',
    'BLACKS FORK NEAR MILLBURNE  WY',
    41.03167,
    -110.57861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCWW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GLCA4',
    '07074888',
    'LMRFC',
    'AR',
    NULL,
    'GLAISE CREEK NEAR RIO VISTA AR',
    35.20063,
    -91.43413
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HASA4',
    '07055780',
    'LMRFC',
    'AR',
    NULL,
    'BUFFALO RIVER AT CARVER ACCESS NR HASTY AR',
    35.98314,
    -93.04275
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HASA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HSEA4',
    '07358257',
    'LMRFC',
    'AR',
    NULL,
    'HOT SPRNGS CR US OF T.E. AT GLADE ST AT HOT SP.AR',
    34.52361,
    -93.05444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HSEA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LADA4',
    '07263706',
    'LMRFC',
    'AR',
    NULL,
    'AR RIVER @ LOCK AND DAM 4 NEAR PINE BLUFF AR',
    34.24806,
    -91.90556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LADA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKCA4',
    '07263265',
    'ABRFC',
    'AR',
    NULL,
    'LAKE CONWAY NEAR MAYFLOWER AR',
    34.96966,
    -92.40329
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LRPA4',
    '07064040',
    'LMRFC',
    'AR',
    NULL,
    'LITTLE RIVER NEAR PEACH ORCHARD AR',
    36.29717,
    -90.69908
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRPA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MJBA4',
    '07055790',
    'LMRFC',
    'AR',
    NULL,
    'BIG CREEK NEAR MT. JUDEA AR',
    35.93861,
    -93.07267
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MJBA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOLA4',
    '07260660',
    'ABRFC',
    'AR',
    NULL,
    'AR RIVER @ DAM NO.9 NEAR OPPELO AR',
    35.12389,
    -92.78639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOLA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15238142,
    'HSXA4',
    '07358284',
    'LMRFC',
    'AR',
    NULL,
    'HOT SPRINGS CR DS OF GRAND AVE AT HOT SPRINGS AR',
    34.50278,
    -93.05056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HSXA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSEA4',
    '07048780',
    'LMRFC',
    'AR',
    NULL,
    'RICHLAND CREEK NEAR GOSHEN AR',
    36.04856,
    -93.97422
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSEA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JONA4',
    '071948095',
    'ABRFC',
    'AR',
    NULL,
    'MUD CREEK NEAR JOHNSON AR',
    36.12281,
    -94.16256
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JONA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TKFC1',
    '11299997',
    'CNRFC',
    'CA',
    NULL,
    'STANISLAUS R BL TULLOCH PP NR KNIGHTS FERRY CA',
    37.87611,
    -120.60417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TKFC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17082145,
    'TEIC1',
    '11276600',
    'CNRFC',
    'CA',
    NULL,
    'TUOLUMNE R AB EARLY INTAKE NR MATHER CA',
    37.87944,
    -119.94611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TEIC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLPS2',
    '433141096572100',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP AT WALL LAKE NR SIOUX FALLS SD',
    43.52806,
    -96.95583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLPS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ADDN2',
    '094156395',
    'CBRFC',
    'NV',
    NULL,
    'ASH SPGS DIV DITCH BLW HWY 93 AT ASH SPGS NV',
    37.45972,
    -115.19417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ADDN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CSKN2',
    '09415590',
    'CBRFC',
    'NV',
    NULL,
    '209 S05 E60 10 1 CRYSTAL SPGS NR HIKO NV',
    37.53194,
    -115.23167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSKN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWSN2',
    '09419740',
    'CBRFC',
    'NV',
    NULL,
    'C-1 CHANNEL NR WARM SPGS RD AT HENDERSON NV',
    36.04472,
    -114.95833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWSN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRHO3',
    '14113290',
    'NWRFC',
    'OR',
    NULL,
    'COLUMBIA RIVER AT HOOD RIVER OR',
    45.71431,
    -121.50323
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRHO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCSP4',
    '50039995',
    NULL,
    'PR',
    NULL,
    'LAGO CARITE AT SPILLWAY PR',
    18.07722,
    -66.10722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCSP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SGCF1',
    '02310740',
    'SERFC',
    'FL',
    NULL,
    'SARAGASSA CANAL AT CRYSTAL RIVER FL',
    28.890278,
    -82.595833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12164306,
    'NW803',
    '040871488',
    'NCRFC',
    'WI',
    '4040003',
    'WILSON PARK CK @ ST. LUKES HOSPITAL @ MILWAUKEE~WI',
    42.99972,
    -87.96583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW803'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12164326,
    'NW804',
    '040871473',
    'NCRFC',
    'WI',
    '4040003',
    'WILSON PARK CREEK AT GMIA INFALL AT MILWAUKEE~ WI',
    42.94888,
    -87.89805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW804'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13019635,
    'NW809',
    '041482663',
    'NCRFC',
    'MI',
    '4080204',
    'ALGER CREEK AT HILL ROAD NEAR SWARTZ CREEK~ MI',
    42.94888,
    -83.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW809'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 189899,
    'HGJC2',
    '06711575',
    'MBRFC',
    'CO',
    '10190002',
    'HARVARD GULCH AT HARVARD PARK AT DENVER  CO',
    39.67172,
    -104.97703
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HGJC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BYBA4',
    '07364122',
    'LMRFC',
    'AR',
    NULL,
    'BAYOU BARTHOLOMEW NR MERONEY AR',
    33.95322,
    -91.73342
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BYBA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCHA4',
    '07055565',
    'LMRFC',
    'AR',
    NULL,
    'CROOKED CREEK AT HARRISON AR',
    36.2325,
    -93.09111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCHA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCRA4',
    '07074886',
    'LMRFC',
    'AR',
    NULL,
    'CUTOFF CREEK NEAR RIO VISTA AR',
    35.2885,
    -91.46168
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCRA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2885330,
    'LCEC2',
    '06714400',
    'MBRFC',
    'CO',
    '10190004',
    'S CLEAR CR ABV LOWER CABIN CR RES NR GEORGETOWN CO',
    39.64901,
    -105.70748
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCEC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 397814,
    'SGCA4',
    '07194906',
    'ABRFC',
    'AR',
    '11110103',
    'Spring Creek at Sanders Ave at Springdale~ AR',
    36.19567,
    -94.13589
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 397506,
    'SSCA4',
    '07194933',
    'ABRFC',
    'AR',
    '11110103',
    'Spring Creek at Hwy 112 nr Springdale~ AR',
    36.24378,
    -94.23914
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2885314,
    'LCHC2',
    '06714500',
    'MBRFC',
    'CO',
    '10190004',
    'S CLEAR CR ABV LWR CABIN CR RES SPILWY NR GEORGTWN',
    39.6631,
    -105.70633
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCHC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1748569,
    'TCKN6',
    '01415460',
    'MARFC',
    'NY',
    '2040102',
    'TERRY CLOVE KILL NEAR DE LANCEY NY',
    42.1471111,
    -74.9052222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCKN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2612942,
    'COUN6',
    '01422389',
    'MARFC',
    'NY',
    '2040101',
    'COULTER BROOK NEAR BOVINA CENTER NY',
    42.2386111,
    -74.7361111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COUN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2884976,
    'LDWC2',
    '06719840',
    'MBRFC',
    'CO',
    '10190004',
    'LITTLE DRY CREEK AT WESTMINSTER~ CO',
    39.82655,
    -105.04006
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LDWC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SHWA4',
    '07363054',
    'LMRFC',
    'AR',
    NULL,
    'SALINE RIVER NEAR SHAW AR',
    34.49889,
    -92.56278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SHWA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SWNA4',
    '07263750',
    'LMRFC',
    'AR',
    NULL,
    'AR RIVER @ LOCK AND DAM 3 NEAR SWAN LAKE AR',
    34.16389,
    -91.67889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SWNA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VENA4',
    '07055792',
    'LMRFC',
    'AR',
    NULL,
    'LEFT FORK BIG CREEK NR VENDOR AR',
    35.94679,
    -93.06688
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VENA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCTA4',
    '07358253',
    'LMRFC',
    'AR',
    NULL,
    'WHITTINGTON CR AT TUNNEL ENT AT HOT SPRINGS AR',
    34.51717,
    -93.0575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCTA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WNOA4',
    '07362591',
    'LMRFC',
    'AR',
    NULL,
    'ALUM FORK SALINE RIVER AT WINONA DAM AT REFORM AR',
    34.79778,
    -92.84556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WNOA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HTDI3',
    '05524546',
    'NCRFC',
    'IN',
    NULL,
    'HUNTER DITCH AT GOODLAND IN',
    40.765556,
    -87.276889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HTDI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22287891,
    'NW963',
    '01327500',
    'NERFC',
    'NY',
    '2020003',
    'GLENS FALLS FEEDER AT DUNHAM BASIN NY',
    43.305,
    -73.54222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW963'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 188283,
    'LEGC2',
    '06709740',
    'MBRFC',
    'CO',
    '10190002',
    'LEE GULCH AT LITTLETON~ CO',
    39.59611,
    -105.01603
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LEGC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3472773,
    'PCKM4',
    '04106500',
    'NCRFC',
    'MI',
    NULL,
    'PORTAGE CREEK AT KALAMAZOO MI',
    42.27417,
    -85.57639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCKM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23735827,
    'SRBO3',
    '14136500',
    'NWRFC',
    'OR',
    '17080001',
    'SANDY RIVER BELOW SALMON RIVER NEAR BRIGHTWOOD  OR',
    45.38317478,
    -122.0456378
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRBO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23781441,
    'RCMO3',
    '14181750',
    'NWRFC',
    'OR',
    '17090005',
    'ROCK CREEK NEAR MILL CITY  OR',
    44.7120665,
    -122.4275818
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCMO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'POCM4',
    '04108862',
    'NCRFC',
    'MI',
    NULL,
    'PIGEON RIVER AT 120TH AVENUE NR OLIVE CENTER MI',
    42.93278,
    -86.08194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'POCM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10275900,
    'WEUU1',
    '10136600',
    'CBRFC',
    'UT',
    '16020102',
    'WEBER RIVER AT I-84 AT UINTAH~ UT',
    41.13708,
    -111.91956
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEUU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCOM4',
    '04106320',
    'NCRFC',
    'MI',
    NULL,
    'WEST FORK PORTAGE CREEK NEAR OSHTEMO MI',
    42.2353,
    -85.6483
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCOM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10840230,
    'BSWT2',
    '08178585',
    'WGRFC',
    'TX',
    NULL,
    'SALADO CK AT WILDERNESS RD SAN ANTONIO TX',
    29.63056,
    -98.56528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8942879,
    'ICTN2',
    '103366993',
    'CNRFC',
    'NV',
    '16050101',
    'INCLINE CK ABV TYROL VILLAGE NR INCLINE VILLAGE NV',
    39.25879699,
    -119.9232439
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ICTN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23065131,
    'ANTW1',
    '12444290',
    'NWRFC',
    'WA',
    '17020006',
    'ANTOINE CREEK AT US HWY 97 NEAR ELLISFORDE~ WA',
    48.75944,
    -119.40806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ANTW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LHWN2',
    '09419749',
    'CBRFC',
    'NV',
    NULL,
    'LV WASH ABV HOMESTEAD WEIR NR HENDERSON NV',
    36.09421,
    -114.95518
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHWN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KSGN2',
    '360956115432801',
    'CNRFC',
    'NV',
    NULL,
    '162  S20 E56 31DADA1    KIUP SPRING',
    36.16354,
    -115.72523
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KSGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CIEP4',
    '50047560',
    NULL,
    'PR',
    NULL,
    'RIO DE BAYAMON BLW LAGO DE CIDRA DAM PR',
    18.20278,
    -66.13917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CIEP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GWPC2',
    '09114520',
    'CBRFC',
    'CO',
    NULL,
    'GUNNISON RIVER AT GUNNISON WHITEWATER PARK CO',
    38.53327,
    -106.94909
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GWPC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCAC2',
    '380107107213201',
    'CBRFC',
    'CO',
    NULL,
    'HENSON CREEK ABV ALPINE GULCH NEAR LAKE CITY CO',
    38.01862,
    -107.35889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCAC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 188637,
    'TUYC2',
    '06711040',
    'MBRFC',
    'CO',
    NULL,
    'TURKEY CREEK ABOVE BEAR CREEK LAKE NEAR MORRISON C',
    39.64071,
    -105.15935
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TUYC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1234173,
    'WCKC2',
    '09021000',
    'CBRFC',
    'CO',
    NULL,
    'WILLOW CREEK BELOW WILLOW CREEK RESERVOIR CO.',
    40.14583,
    -105.93944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCKC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CAVC2',
    '09359080',
    'CBRFC',
    'CO',
    NULL,
    'CASCADE CR ABV CASCADE CR DIVERSION NR ROCKWOOD CO',
    37.66728,
    -107.82263
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAVC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GBGC2',
    '383103106594200',
    'CBRFC',
    'CO',
    NULL,
    'GUNNISON RIVER AT CNTY RD 32 BELOW GUNNISON CO',
    38.51726,
    -106.99545
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBGC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 68334,
    'INDF1',
    '264514080550700',
    'SERFC',
    'FL',
    NULL,
    NULL,
    26.7607593536377,
    -80.9179840087891
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDCF1',
    '261319080353201',
    'SERFC',
    'FL',
    NULL,
    'EDEN 9 IN WATER CONSERVATION AREA 3-A',
    26.22194,
    -80.59222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDIF1',
    '260042080351701',
    'SERFC',
    'FL',
    NULL,
    'EDEN 12 IN WATER CONSERVATION AREA 3-A',
    26.01167,
    -80.58806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDIF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDJF1',
    '261035080221701',
    'SERFC',
    'FL',
    NULL,
    'EDEN 13 IN WATER CONSERVATION AREA 2-B',
    26.17639,
    -80.37139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDJF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDKF1',
    '260410080452701',
    'SERFC',
    'FL',
    NULL,
    'EDEN 14 IN WATER CONSERVATION AREA 3-A',
    26.06944,
    -80.7575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDNF1',
    '260355080541401',
    'SERFC',
    'FL',
    NULL,
    'EDEN 6 IN BIG CYPRESS NATIONAL PRESERVE',
    26.06528,
    -80.90389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDNF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EMBF1',
    '02289085',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL EAST END 1 MILE BRIDGE NR MIAMI FL',
    25.76139,
    -80.51667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EMBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EVMF1',
    '252036080324300',
    'SERFC',
    'FL',
    NULL,
    'EVERGLADES 4 IN C-111 BASIN NR HOMESTEAD FL',
    25.33877,
    -80.54666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EVMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22743145,
    'UTIN6',
    '01342602',
    'NERFC',
    'NY',
    NULL,
    'MOHAWK RIVER NEAR UTICA NY',
    43.09369,
    -75.15792
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UTIN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4005288,
    'MMCK2',
    '03309000',
    'OHRFC',
    'KY',
    NULL,
    'GREEN RIVER AT MAMMOTH CAVE KY',
    37.18,
    -86.1125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MMCK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCTP4',
    '50093083',
    NULL,
    'PR',
    NULL,
    'CANAL DE PATILLAS ABV AES INTAKE AT GUAYAMA PR',
    17.96972,
    -66.13917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCTP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLOF1',
    '02236901',
    'SERFC',
    'FL',
    NULL,
    'PALATLAKAHA R BL SPWY AT CH LK OUT NR GROVELANDFL',
    28.59222,
    -81.82278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COOF1',
    '254848080432001',
    'SERFC',
    'FL',
    NULL,
    'SITE 65 IN CONSERVATION AREA 3A NR COOPERTOWN FL',
    25.81427,
    -80.72012
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPRF1',
    '02272676',
    'SERFC',
    'FL',
    NULL,
    'CYPRESS SLOUGH NEAR BASINGER FL',
    27.38,
    -80.97611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LGSF1',
    '02322400',
    'SERFC',
    'FL',
    NULL,
    'GINNIE SPRING NR HIGH SPRINGS FLA',
    29.83583,
    -82.70028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LGSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCVF1',
    '02253500',
    'SERFC',
    'FL',
    NULL,
    'SOUTH CANAL NEAR VERO BEACH FL',
    27.60417,
    -80.38681
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 54318,
    'SPTF1',
    '02277110',
    'SERFC',
    'FL',
    NULL,
    'ST LUCIE ESTUARY AT A1A (STEELE PT)STUART FL',
    27.19944,
    -80.20694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPTF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NVBF1',
    '02252500',
    'SERFC',
    'FL',
    NULL,
    'NORTH CANAL NEAR VERO BEACH FL',
    27.69278,
    -80.42028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NVBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16636378,
    'PLRF1',
    '02237207',
    'SERFC',
    'FL',
    NULL,
    'PALATLAKAHA R BELOW STRUCTURE M-4 NR OKAHUMPKA FL',
    28.71556,
    -81.88417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PLRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'IRWF1',
    '02251800',
    'SERFC',
    'FL',
    NULL,
    'INDIAN RIVER AT WABASSO FL',
    27.754167,
    -80.427778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IRWF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PLGF1',
    '02236900',
    'SERFC',
    'FL',
    NULL,
    'PALATLAKAHA R AT CHERRY LK OUT NEAR GROVELAND FL',
    28.5925,
    -81.8225
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PLGF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PRMF1',
    '02237010',
    'SERFC',
    'FL',
    NULL,
    'PALATLAKAHA R AT STRUCTURE M-6 NR MASCOTTE FL',
    28.64306,
    -81.8725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JBCF1',
    '02292740',
    'SERFC',
    'FL',
    NULL,
    'JACKS BRANCH AT CR 78 NR FT DENAUD FL',
    26.74743,
    -81.51323
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JBCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JCPF1',
    '02308950',
    'SERFC',
    'FL',
    NULL,
    'ST JOE CREEK AT 62ND ST NR PINELLAS PARK FL',
    27.81024,
    -82.72036
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JCPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21484146,
    'LCFF1',
    '02269520',
    'SERFC',
    'FL',
    NULL,
    'LIVINGSTON CREEK NEAR FROSTPROOF FL',
    27.70833,
    -81.44667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16928644,
    'HMOF1',
    '02310678',
    'SERFC',
    'FL',
    NULL,
    'HOMOSASSA SPRINGS AT HOMOSASSA SPRINGS FL',
    28.79944,
    -82.58889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16906283,
    'HRZF1',
    '02302010',
    'SERFC',
    'FL',
    NULL,
    'HILLSBOROUGH R BL CRYSTAL SPR NEAR ZEPHYRHILLS FL',
    28.17861,
    -82.18917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16928268,
    'HRLF1',
    '02310690',
    'SERFC',
    'FL',
    NULL,
    'HALLS RIVER NEAR HOMOSASSA FL',
    28.80111,
    -82.60278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCRF1',
    '02266550',
    'SERFC',
    'FL',
    NULL,
    'REEDY CREEK AT STATE HWY 531 NEAR POINSIANNA FL',
    28.149722,
    -81.441111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PPOP4',
    '50115420',
    NULL,
    'PR',
    NULL,
    'RIO PORTUGUES AT HWY 10 AT PONCE PR',
    18.03971,
    -66.61178
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PPOP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11120297,
    'COCN2',
    '10245960',
    'CNRFC',
    'NV',
    NULL,
    NULL,
    39.7697792053223,
    -116.464447021484
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'STUN6',
    '01362230',
    'NERFC',
    'NY',
    NULL,
    'DIVERSION FROM SCHOHARIE RESERVOIR NY',
    42.11444,
    -74.36417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STUN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PDSF1',
    '02294705',
    'SERFC',
    'FL',
    NULL,
    'PEACE RV DISTRIBUTARY AT DOVER SINK NR BARTOW FL',
    27.87278,
    -81.80167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PDSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRCN6',
    '04216218',
    'NERFC',
    'NY',
    NULL,
    'BLACK ROCK CANAL AT BLACK ROCK LOCK BUFFALO NY',
    42.933668,
    -78.904759
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRCN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15594731,
    'CSNO1',
    '04201400',
    'OHRFC',
    'OH',
    NULL,
    'WEST BRANCH ROCKY RIVER AT WEST VIEW OH',
    41.35083,
    -81.90333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSNO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15547239,
    'SHKN6',
    '04227000',
    'NERFC',
    'NY',
    NULL,
    'CANASERAGA CREEK AT SHAKERS CROSSING NY',
    42.73611,
    -77.84161
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SHKN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21631697,
    'STEN6',
    '04232100',
    'NERFC',
    'NY',
    NULL,
    'STERLING CREEK AT STERLING NY',
    43.32528,
    -76.64722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STEN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MPCO2',
    '07331293',
    'ABRFC',
    'OK',
    NULL,
    'PENNINGTON CREEK NORTH OF MILL CREEK OK',
    34.47745,
    -96.81132
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MPCO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PHBF1',
    '02309447',
    'SERFC',
    'FL',
    NULL,
    'BEE BRANCH NEAR PALM HARBOR FL',
    28.07113,
    -82.76366
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PIAF1',
    '02307669',
    'SERFC',
    'FL',
    NULL,
    'ALLIGATOR CREEK AT HIGHWAY 19 AT CLEARWATER FL',
    27.97319,
    -82.72944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PIAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PIFF1',
    '02310280',
    'SERFC',
    'FL',
    NULL,
    'PITHLACHASCOTEE RIVER NEAR FIVAY JUNCTION FL',
    28.32889,
    -82.53694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PIFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PLPF1',
    '02297345',
    'SERFC',
    'FL',
    NULL,
    'PEACE RIVER AT PLATT FL',
    27.08667,
    -81.99944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PLPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PTHF1',
    '02310300',
    'SERFC',
    'FL',
    NULL,
    'PITHLACHASCOTEE RIVER NEAR NEW PORT RICHEY FL',
    28.25703,
    -82.64308
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16916302,
    'SBCF1',
    '02306904',
    'SERFC',
    'FL',
    NULL,
    'BRUSHY CREEK NEAR SULPHUR SPRINGS',
    28.08386,
    -82.525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ROCF1',
    '02308866',
    'SERFC',
    'FL',
    NULL,
    'ROOSEVELT CANAL BELOW STR 23-8 NR PINELLAS PARK FL',
    27.9075,
    -82.67611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ROCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2028839,
    'NW619',
    '05355092',
    'NCRFC',
    'MN',
    '7040002',
    'CANNON RIVER AT 9TH ST. BRIDGE IN CANNON FALLS~ MN',
    44.52527,
    -92.915
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW619'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12086741,
    'SSGN2',
    '355906115492601',
    'CNRFC',
    'NV',
    NULL,
    NULL,
    35.9837493896484,
    -115.82527923584
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22591043,
    'WWUC1',
    '10255895',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    34.0623321533203,
    -116.819114685059
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WWUC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22592143,
    'MYNC1',
    '10258700',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    33.7527770996094,
    -116.544441223145
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MYNC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20274999,
    'DCJC1',
    '10265100',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    37.7502784729004,
    -118.939445495605
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCJC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TNCG1',
    '02336321',
    'SERFC',
    'GA',
    NULL,
    'TRIB TO NANCY CRK AT PEACHFORD DR NR DUNWOODY GA',
    33.92333,
    -84.30389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TNCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCSG1',
    '02344736',
    'SERFC',
    'GA',
    NULL,
    'WHITEWATER CR AT SHERWOOD RD NR FAYETTEVILLE GA',
    33.421389,
    -84.492778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCSG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WMPG1',
    '02191740',
    'SERFC',
    'GA',
    NULL,
    'CLOUDS CREEK AT WATSON MILL PARK NR CARLTON GA',
    34.020556,
    -83.069444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WMPG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8933802,
    'TBMC1',
    '10339410',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    39.3530540466309,
    -120.119445800781
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBMC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8932072,
    'BFDN2',
    '10348295',
    'CNRFC',
    'NV',
    NULL,
    NULL,
    39.5315818786621,
    -119.714973449707
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BFDN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8935386,
    'OPCN2',
    '10348520',
    'CNRFC',
    'NV',
    NULL,
    NULL,
    39.290828704834,
    -119.830917358398
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OPCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22563462,
    'ONAC1',
    '11071900',
    'CNRFC',
    'CA',
    NULL,
    NULL,
    33.7502784729004,
    -117.445831298828
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ONAC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JLGP4',
    '50110650',
    NULL,
    'PR',
    NULL,
    'RIO JACAGUAS ABV LAGO GUAYABAL',
    18.11659,
    -66.50465
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JLGP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MORP4',
    '50031000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE MANATI AT HWY 155 NR MOROVIS PR',
    18.29944,
    -66.41
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MORP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PRNP4',
    '50115240',
    NULL,
    'PR',
    NULL,
    'RIO PORTUGUES AT PARQUE CEREMONIAL TIBES NR PONCE',
    18.04357,
    -66.62151
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRNP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RLCP4',
    '50128933',
    NULL,
    'PR',
    NULL,
    'CANAL RIEGO DE LAJAS ABV LAJAS FILTRATION PLANT',
    18.04506,
    -67.05255
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RLCP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VALP4',
    '50056400',
    NULL,
    'PR',
    NULL,
    'RIO VALENCIANO NR JUNCOS PR',
    18.21607,
    -65.92617
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VALP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FAEP4',
    '50070900',
    NULL,
    'PR',
    NULL,
    'RIO FAJARDO AT PARAISO NR FAJARDO PR',
    18.28281,
    -65.70107
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FAEP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6246218,
    'WOON4',
    '01377451',
    'MARFC',
    'NJ',
    NULL,
    'PASCACK BK AT WOODCLIFF LK OUTLET AT HILLSDALE NJ',
    41.01194,
    -74.04806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WOON4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6251048,
    'PATN4',
    '01389802',
    'MARFC',
    'NJ',
    NULL,
    'PASSAIC R AT PASSAIC (GREAT) FALLS AT PATERSON NJ',
    40.91583,
    -74.18167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PATN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 166758629,
    'BLSF1',
    '02319302',
    'SERFC',
    'FL',
    NULL,
    'MADISON BLUE SPRING NR BLUE SPRINGS FL',
    30.48028,
    -83.24444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16683929,
    'PELF1',
    '02247222',
    'SERFC',
    'FL',
    NULL,
    'PELLICER CREEK NEAR ESPANOLA FL',
    29.66917,
    -81.25972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PELF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FHVN6',
    '04232133',
    'NERFC',
    'NY',
    NULL,
    'STERLING CREEK AT MOUTH AT NORTH FAIR HAVEN NY',
    43.34325,
    -76.69842
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FHVN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10320306,
    'WKLF1',
    '02327000',
    'SERFC',
    'FL',
    NULL,
    'WAKULLA SPRING NR CRAWFORDVILLE FL',
    30.23472,
    -84.30139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WKLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11622244,
    'HRFK2',
    '03319600',
    'OHRFC',
    'KY',
    NULL,
    'ROUGH RIVER AT HARTFORD KY',
    37.45306,
    -86.91083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRFK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 886673,
    'VLRK2',
    '03211500',
    'OHRFC',
    'KY',
    NULL,
    'JOHNS CREEK NEAR VAN LEAR KY',
    37.74361,
    -82.72417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VLRK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10286212,
    'TERI3',
    '03341500',
    'OHRFC',
    'IN',
    NULL,
    'WABASH RIVER AT TERRE HAUTE IN',
    39.46572,
    -87.4195
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TERI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10834936,
    'RBFT2',
    '08179110',
    'WGRFC',
    'TX',
    NULL,
    'RED BLUFF CK AT FM 1283 NR PIPE CREEK TX',
    29.67306,
    -98.96
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RBFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10840818,
    'MTST2',
    '08178050',
    'WGRFC',
    'TX',
    NULL,
    'SAN ANTONIO RV AT MITCHELL ST SAN ANTONIO TX',
    29.39278,
    -98.49444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3380913,
    'MCSU1',
    '09183600',
    'CBRFC',
    'UT',
    NULL,
    'MILL CREEK BELOW SHELEY TUNNEL NEAR MOAB UT',
    38.48573,
    -109.41043
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCSU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3265786,
    'MCHW4',
    '09222400',
    'CBRFC',
    'WY',
    NULL,
    'MUDDY CREEK NEAR HAMPTON WY',
    41.53806,
    -110.22861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCHW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CMCA3',
    '09428500',
    'CBRFC',
    'AZ',
    NULL,
    'CRIR MAIN CANAL NEAR PARKER AZ',
    34.16778,
    -114.27583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMCA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWSH1',
    '16720000',
    'AKRFC',
    'HI',
    NULL,
    'KAWAINUI STREAM NR KAMUELA HI',
    20.085278,
    -155.681111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWKH1',
    '16010000',
    'AKRFC',
    'HI',
    NULL,
    'KAWAIKOI STREAM NR WAIMEA KAUAI HI',
    22.132806,
    -159.619944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWKH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KUTH1',
    '16208400',
    'AKRFC',
    'HI',
    NULL,
    'KU TREE RESERVOIR NEAR WAHIAWA OAHU HI',
    21.49736,
    -157.98072
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KUTH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KUSH1',
    '16283200',
    'AKRFC',
    'HI',
    NULL,
    'KAHALUU STR NR AHUIMANU OAHU HI',
    21.43872,
    -157.84386
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KUSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KSFH1',
    '16208000',
    'AKRFC',
    'HI',
    NULL,
    'SF KAUKONAHUA STR AT E PUMP NR WAHIAWA OAHU HI',
    21.48881,
    -157.99606
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KSFH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWGH1',
    '16415600',
    'AKRFC',
    'HI',
    NULL,
    'KAWELA GULCH NEAR MOKU MOLOKAI HI',
    21.06997,
    -156.94833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWGH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWWH1',
    '16265000',
    'AKRFC',
    'HI',
    NULL,
    'KAWA STR AT KANEOHE OAHU HI',
    21.40575,
    -157.7905
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWWH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10772174,
    'MBON2',
    '10313400',
    'CNRFC',
    'NV',
    '16040101',
    'MARYS RV BLW ORANGE BRG NR CHARLESTON  NV',
    41.54991275,
    -115.3067288
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MBON2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ORNI4',
    '05420850',
    'NCRFC',
    'IA',
    NULL,
    'LITTLE WAPSIPINICON RIVER NEAR ORAN IA',
    42.71469,
    -92.04156
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ORNI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WWLH1',
    '16212601',
    'AKRFC',
    'HI',
    NULL,
    'WAIKELE STR AT WHEELER FIELD OAHU HI',
    21.47231,
    -158.04447
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WWLH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WHSH1',
    '16294100',
    'AKRFC',
    'HI',
    NULL,
    'WAIAHOLE STREAM ABOVE KAMEHAMEHA HWY OAHU HI',
    21.482028,
    -157.845889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WHSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WESH1',
    '16284200',
    'AKRFC',
    'HI',
    NULL,
    'WAIHEE STR NR KAHALUU OAHU HI',
    21.448056,
    -157.856667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WESH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBII4',
    '05464730',
    'NCRFC',
    'IA',
    NULL,
    'CEDAR RIVER BELOW INDIAN CREEK AT CEDAR RAPIDS IA',
    41.963,
    -91.57708
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBII4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8943705,
    'UTSC1',
    '10336580',
    'CNRFC',
    'CA',
    '16050101',
    'UPPER TRUCKEE RV AT S UPPER TRUCKEE RD NR MEYERS',
    38.79629598,
    -120.0190718
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UTSC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2617174,
    'NHMN6',
    '01428000',
    'MARFC',
    'NY',
    NULL,
    'TENMILE RIVER AT TUSTEN NY',
    41.56389,
    -75.01444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NHMN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3769806,
    'NW642',
    '03062235',
    'OHRFC',
    'WV',
    '5020003',
    'MONONGAHELA RIVER AT FLAGGY MEADOW~ WV',
    39.55916,
    -80.01694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW642'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6873503,
    'NW715',
    '03026480',
    'OHRFC',
    'PA',
    '5010005',
    'East Branch Clarion River near Clermont~ PA',
    41.64388,
    -78.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW715'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23981015,
    'HBCW1',
    '12096865',
    'NWRFC',
    'WA',
    NULL,
    NULL,
    47.0603866577148,
    -121.605651855469
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HBCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4577916,
    'LNGV1',
    '04285800',
    'NERFC',
    'VT',
    NULL,
    'NORTH BRANCH WINOOSKI RIVER AT MONTPELIER VT',
    44.26083,
    -72.57639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNGV1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NRON6',
    '04232076',
    'NERFC',
    'NY',
    NULL,
    'SODUS CREEK AT NORTH ROSE NY',
    43.19139,
    -76.9125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NRON6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NHLN6',
    '04220223',
    'NERFC',
    'NY',
    NULL,
    'SANDY CREEK AT NORTH HAMLIN NY',
    43.339167,
    -77.915278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NHLN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10295736,
    'NW757',
    '01168250',
    'NERFC',
    'MA',
    '1080203',
    'COLD RIVER AT FLORIDA~ MA',
    42.67777,
    -73.01694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW757'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23752006,
    'NFOO3',
    '14144800',
    'NWRFC',
    'OR',
    NULL,
    'MIDDLE FORK WILLAMETTE RIVER NR OAKRIDGE OR',
    43.60194,
    -122.45639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFOO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HZNN2',
    '10351400',
    'CNRFC',
    'NV',
    NULL,
    'TRUCKEE CANAL NR HAZEN NV',
    39.50389,
    -119.04417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HZNN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 664424,
    'HDPU1',
    '10108400',
    'CBRFC',
    'UT',
    NULL,
    'CACHE HIGHLINE CANAL NEAR LOGAN UTAH',
    41.74306,
    -111.76111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HDPU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCFU1',
    '10142000',
    'CBRFC',
    'UT',
    NULL,
    'FARMINGTON CR ABV DIV NR FARMINGTON UTAH',
    41.00139,
    -111.8725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCFU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23894452,
    'MULO3',
    '14313200',
    'NWRFC',
    'OR',
    NULL,
    'N.UMPQUA R ABV WHITE MULE CK NR TOKETEE FALLS OR',
    43.32417,
    -122.19583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MULO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23386379,
    'ARAI1',
    '13190500',
    'NWRFC',
    'ID',
    NULL,
    'SF BOISE RIVER AT ANDERSON RANCH DAM ID',
    43.34361,
    -115.4775
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ARAI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WBFI1',
    '13058520',
    'NWRFC',
    'ID',
    NULL,
    'WILLOW CREEK FLOODWAY CHANNEL NR UCON ID',
    43.57639,
    -111.91306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBFI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5588558,
    'DNGT2',
    '08103940',
    'WGRFC',
    'TX',
    '12070203',
    'Lampasas Rv at Ding Dong~ TX',
    30.97253,
    -97.77853
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DNGT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TADW1',
    '12101000',
    'NWRFC',
    'WA',
    NULL,
    'LAKE TAPPS NEAR SUMNER WA',
    47.24111,
    -122.19056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TADW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPCW1',
    '12089208',
    'NWRFC',
    'WA',
    NULL,
    'CENTRALIA POWER CANAL NEAR MCKENNA WA',
    46.90028,
    -122.49722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPCW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCOW1',
    '12113349',
    'NWRFC',
    'WA',
    NULL,
    'MILL CREEK NEAR MOUTH AT ORILLIA WA',
    47.43028,
    -122.24194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCOW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23838486,
    'BGFW1',
    '12042800',
    'NWRFC',
    'WA',
    NULL,
    'BOGACHIEL RIVER NEAR FORKS WA',
    47.89442,
    -124.35709
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BGFW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LTDW1',
    '12101100',
    'NWRFC',
    'WA',
    NULL,
    'LAKE TAPPS DIVERSION AT DIERINGER WA',
    47.23833,
    -122.22694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LTDW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5499434,
    'GBZT2',
    '08090905',
    'WGRFC',
    'TX',
    '12060201',
    'Brazos Rv ds Lk Granbury nr Granbury~ TX',
    32.38639,
    -97.65222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBZT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17916483,
    'NW888',
    '07364078',
    'LMRFC',
    'AR',
    '8040202',
    'Ouachita River at Felsenthal L&D (lower)',
    33.05083,
    -92.11861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW888'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18377654,
    'NW901',
    '09171240',
    'CBRFC',
    'CO',
    '14030003',
    'LAKE FORK SAN MIGUEL RV ABV TROUT LAKE NR OPHIR CO',
    37.83027,
    -107.88111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW901'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LOPP4',
    '50128945',
    NULL,
    'PR',
    NULL,
    'CANAL DE RIEGO DE LAJAS AT BO. PALMAREJO NR LAJAS',
    18.0392,
    -67.07898
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOPP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GMNP4',
    '50095000',
    NULL,
    'PR',
    NULL,
    'CANAL DE GUAMANI OESTE AT HWY 15 GUAYAMA PR',
    18.00306,
    -66.11556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GMNP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 89926,
    'ACNF1',
    '251032080473400',
    'SERFC',
    'FL',
    NULL,
    NULL,
    25.1755561828613,
    -80.7927780151367
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACNF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HERI3',
    '03352695',
    'OHRFC',
    'IN',
    NULL,
    'HERON LAKE AT INDIANAPOLIS IN',
    39.84722,
    -86.11361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HERI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCKM5',
    '06603920',
    'MBRFC',
    'MN',
    NULL,
    'LOON CREEK NEAR ORLEANS IA',
    43.5211,
    -95.1014
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCKM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AKAM5',
    '05283500',
    'NCRFC',
    'MN',
    NULL,
    'MISSISSIPPI RIVER AT US HWY 169 AT CHAMPLIN MN',
    45.1916,
    -93.3944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AKAM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 948020360,
    'NW1013',
    '11455165',
    'CNRFC',
    'CA',
    '18020163',
    'MINER SLOUGH A HWY 84 BRIDGE',
    38.28805,
    -121.62694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1013'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'METF1',
    '254130080380500',
    'SERFC',
    'FL',
    NULL,
    'NORTHEAST SHARK RVR SLOUGH NO1 NR COOPERTOWN FL',
    25.69165,
    -80.63492
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'METF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MIRF1',
    '260037080303401',
    'SERFC',
    'FL',
    NULL,
    'SITE 76 IN CONSERVATION AREA 3B NR ANDYTOWN FL',
    26.00765,
    -80.48236
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MIRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MODF1',
    '254759080483201',
    'SERFC',
    'FL',
    NULL,
    'W-2 IN WATER CONSERVATION AREA 3-A',
    25.79972,
    -80.80889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MODF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CMIN7',
    '0214267602',
    'SERFC',
    'NC',
    NULL,
    'CATAWBA RIVER DNSTRM DECK MTN IS DAM NR MTN IS NC',
    35.33401,
    -80.98641
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMIN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LWYN2',
    '09419679',
    'CBRFC',
    'NV',
    NULL,
    'LAS VEGAS WASTEWAY NR E LAS VEGAS NV',
    36.10805,
    -115.02199
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LWYN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCRO1',
    '03255390',
    'OHRFC',
    'OH',
    NULL,
    'SHARON CREEK AT SHARONVILLE OH',
    39.27333,
    -84.41111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCRO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWRO2',
    '07331455',
    'ABRFC',
    'OK',
    NULL,
    'LAKE TEXOMA AT CUMBERLAND CUT NR CUMBERLAND OK',
    34.09691,
    -96.55327
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWRO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SHLO2',
    '07241600',
    'ABRFC',
    'OK',
    NULL,
    'SHAWNEE RESERVOIR AT SHAWNEE OK',
    35.34722,
    -97.0625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SHLO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RSSO1',
    '03260600',
    'OHRFC',
    'OH',
    NULL,
    'GREAT MIAMI RIVER AT RUSSELLS POINT OH',
    40.450556,
    -83.906944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSSO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FUQO2',
    '07329610',
    'ABRFC',
    'OK',
    NULL,
    'LAKE FUQUA NEAR DUNCAN OK',
    34.599722,
    -97.670833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FUQO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSQO2',
    '07241780',
    'ABRFC',
    'OK',
    NULL,
    'SQUIRREL CREEK AT SHAWNEE OK',
    35.30417,
    -96.91222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSQO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WWLO2',
    '07241588',
    'ABRFC',
    'OK',
    NULL,
    'WES WATKINS RESERVOIR NEAR MCLOUD OK',
    35.40194,
    -97.09889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WWLO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BTLO2',
    '07174470',
    'ABRFC',
    'OK',
    NULL,
    'CANEY RIVER AT TUXEDO BLVD AT BARTLESVILLE OK',
    36.75667,
    -95.95639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BTLO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CMRO2',
    '07185030',
    'ABRFC',
    'OK',
    NULL,
    'ELM CREEK NEAR COMMERCE OK',
    36.92167,
    -94.91833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMRO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LNRO2',
    '07190100',
    'ABRFC',
    'OK',
    NULL,
    'NEOSHO RIVER (SERVICE ROAD) NEAR LANGLEY OK',
    36.46194,
    -95.03333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNRO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PHSO2',
    '07176321',
    'ABRFC',
    'OK',
    NULL,
    'BIRD CREEK AT SH 99 AT PAWHUSKA OK',
    36.66694,
    -96.31389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHSO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GHWP1',
    '03027200',
    'OHRFC',
    'PA',
    NULL,
    'WEIR 6 AT EAST BRANCH CLARION RIVER DAM PA',
    41.55806,
    -78.59611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GHWP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NICS1',
    '02134900',
    'SERFC',
    'SC',
    NULL,
    'LUMBER RIVER AT NICHOLSSC',
    34.225833,
    -79.13444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NICS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PDFS1',
    '02130810',
    'SERFC',
    'SC',
    NULL,
    'PEEDEE RIVER NEAR FLORENCESC',
    34.308458,
    -79.634471
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PDFS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889722,
    'NW611',
    '11312685',
    'CNRFC',
    'CA',
    '18040003',
    'MIDDLE R NR HOLT CA',
    38.0,
    -121.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW611'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1897374,
    'NW616',
    '11312968',
    'CNRFC',
    'CA',
    '18040003',
    'OLD R NR DELTA MENDOTA CANAL CA',
    37.81333,
    -121.54222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW616'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1894376,
    'NW614',
    '11313240',
    'CNRFC',
    'CA',
    '18040003',
    'GRANT LINE CN NR TRACY CA',
    37.83027,
    -121.54222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW614'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889648,
    'NW606',
    '11313452',
    'CNRFC',
    'CA',
    '18040003',
    'OLD R A FRANKS TRACT NR TERMINOUS CA',
    38.06777,
    -121.57611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW606'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889664,
    'NW608',
    '11313460',
    'CNRFC',
    'CA',
    '18040003',
    'SAN JOAQUIN R A PRISONERS PT NR TERMINOUS CA',
    38.05083,
    -121.55916
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW608'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889600,
    'NW604',
    '11336680',
    'CNRFC',
    'CA',
    '18040012',
    'S MOKELUMNE R A NEW HOPE BR NR WALNUT GROVE CA',
    38.22027,
    -121.49138
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW604'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889612,
    'NW605',
    '11336685',
    'CNRFC',
    'CA',
    '18040012',
    'N MOKELUMNE NR WALNUT GROVE CA',
    38.22027,
    -121.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW605'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'REFS1',
    '021603273',
    'SERFC',
    'SC',
    NULL,
    'ROCKY CREEK NEAR WADE HAMPTONSC',
    34.85611,
    -82.26833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'REFS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCTS1',
    '021622847',
    'SERFC',
    'SC',
    NULL,
    'SLICKING CREEK NEAR ROCKY BOTTOMSC',
    35.06778,
    -82.69806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCTS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSLS1',
    '021622845',
    'SERFC',
    'SC',
    NULL,
    'SOUTH SALUDA RIVER NEAR ROCKY BOTTOMSC',
    35.06083,
    -82.705
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSLS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SSSS1',
    '02162287',
    'SERFC',
    'SC',
    NULL,
    'TABLE ROCK RESERVOIR TAILRACE NR CLEVELAND SC',
    35.06222,
    -82.66972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSSS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCAS1',
    '02110755',
    'SERFC',
    'SC',
    NULL,
    'AIW AT BRIARCLIFFE ACRES AT N. MYRTLE BEACH SC',
    33.79833,
    -78.75333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCAS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11012057,
    'NW768',
    '02231454',
    'SERFC',
    'FL',
    '3080101',
    'SIXMILE CREEK NEAR KENANSVILLE~ FL',
    27.88111,
    -80.81333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW768'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11689678,
    'NW782',
    '01585075',
    'MARFC',
    'MD',
    '2060003',
    'FOSTER BRANCH NEAR JOPPATOWNE~ MD',
    39.40666,
    -76.33888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW782'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12006335,
    'NW798',
    '04084911',
    'NCRFC',
    'WI',
    '4030204',
    'PLUM CREEK NEAR WRIGHTSTOWN~ WI',
    44.305,
    -88.16944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW798'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCSA2',
    '15274600',
    'AKRFC',
    'AK',
    NULL,
    'CAMPBELL C NR SPENARD AK',
    61.139444,
    -149.923333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BNRT2',
    '07315525',
    'ABRFC',
    'TX',
    NULL,
    'BELKNAP CK NR RINGGOLD TX',
    33.80698,
    -97.88631
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BNRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BOMT2',
    '07332605',
    'ABRFC',
    'TX',
    NULL,
    'BOIS D''ARC CK AT HWY 56 NR BONHAM TX',
    33.57579,
    -96.15574
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BTGT2',
    '08143700',
    'WGRFC',
    'TX',
    NULL,
    'BROWNS CK TRIB NR GOLDTHWAITE TX',
    31.51691,
    -98.56681
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BTGT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BVRT2',
    '07315510',
    'ABRFC',
    'TX',
    NULL,
    'BEAVER CK NR RINGGOLD TX',
    33.8554,
    -97.94385
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BVRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CGAT2',
    '08064570',
    'WGRFC',
    'TX',
    NULL,
    'TRINITY RV AT HWY 287 NR CAYUGA TX',
    31.96708,
    -96.04717
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CGAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DOST2',
    '08062575',
    'WGRFC',
    'TX',
    NULL,
    'TRINITY RV AT W CEDAR CREEK PKWY NR DOSSER TX',
    32.31667,
    -96.35931
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DOST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FLDF1',
    '260810080222001',
    'SERFC',
    'FL',
    NULL,
    'SITE 99 NR L-35A IN CONS AREA 2B NR SUNRISE FL',
    26.13644,
    -80.36708
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FLDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NESF1',
    '253828080391100',
    'SERFC',
    'FL',
    NULL,
    'N.E. SHARK RIVER SLOUGH NO. 4 NORTH OF GROSSMAN',
    25.64016,
    -80.65277
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NESF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NFCF1',
    '262038080584600',
    'SERFC',
    'FL',
    NULL,
    'NORTH FEEDER CANAL BLW PC17A NR CLEWISTON FL',
    26.34375,
    -80.97933
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NLXF1',
    '263537080211400',
    'SERFC',
    'FL',
    NULL,
    'NORTH LOXAHATCHEE CONSERVATION AREA NO. 1',
    26.59378,
    -80.35386
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NLXF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCFT2',
    '08104055',
    'WGRFC',
    'TX',
    NULL,
    'CHALK RIDGE FALLS SPGS NR BELTON TX',
    31.018,
    -97.52628
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COOA2',
    '15258000',
    'AKRFC',
    'AK',
    NULL,
    'KENAI R AT COOPER LANDING AK',
    60.492778,
    -149.807778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COOA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DGDC2',
    '06711770',
    'MBRFC',
    'CO',
    NULL,
    'DRY GULCH AT DENVER CO',
    39.73438,
    -105.03956
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DGDC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16802915,
    'PBZF1',
    '02294655',
    'SERFC',
    'FL',
    NULL,
    'PEACE RIVER NEAR BARTOW FL',
    27.88306,
    -81.80444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PBZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LUPA4',
    NULL,
    'LMRFC',
    'AR',
    NULL,
    'LOWER VALLIER(LITBAYMETO)HW/TW NR LODGE CORNERAR',
    34.25158,
    -91.63569
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LUPA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCCN2',
    '09419745',
    'CBRFC',
    'NV',
    NULL,
    'C-1 CHANNEL ABV MOUTH NR HENDERSON NV',
    36.085,
    -114.96833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3981038,
    'BILO1',
    '03260502',
    'OHRFC',
    'OH',
    NULL,
    'GREAT MIAMI RIVER BL INDIAN LAKE AT RUSSELLS PT OH',
    40.46722,
    -83.87556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BILO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GBSI3',
    '413339087223001',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET RIVER AT BURR STREET AT GARY IN',
    41.5605,
    -87.4019
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBSI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRGI3',
    '04093176',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET RIVER AT GRANT ST AT GARY IN',
    41.5661,
    -87.3558
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRGI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HMMI3',
    '05536160',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET RIVER NEAR HAMMOND IN',
    41.568333,
    -87.474444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMMI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SAMP4',
    '50100450',
    NULL,
    'PR',
    NULL,
    'RIO MAJADA AT LA PLENA PR',
    18.04472,
    -66.2075
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAMP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PMOF1',
    '02294759',
    'SERFC',
    'FL',
    NULL,
    'PHOSPHATE MINE OUTFALL CS-8 NEAR BARTOW FL',
    27.84361,
    -81.80389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PMOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WPSC2',
    '06708690',
    'MBRFC',
    'CO',
    NULL,
    'WEST PLUM CREEK AT SEDALIA CO',
    39.42928,
    -104.96778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPSC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PODF1',
    '02310288',
    'SERFC',
    'FL',
    NULL,
    'PITHLACHASCOTEE R BL SUNCOAST PKWY NR FIVAY JCT FL',
    28.29489,
    -82.57283
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PODF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3247574,
    'SPUN6',
    '01349711',
    'NERFC',
    'NY',
    NULL,
    'WEST KILL BELOW HUNTER BROOK NEAR SPRUCETON NY',
    42.185,
    -74.27694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPUN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DTWW4',
    '06427850',
    'MBRFC',
    'WY',
    NULL,
    'BELLE FOURCHE RIVER AT DEVILS TOWER WY',
    44.58944,
    -104.70333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DTWW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KOWH1',
    '16210500',
    'AKRFC',
    'HI',
    NULL,
    'KAUKONAHUA STR AT WAIALUA OAHU HI',
    21.565278,
    -158.120278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KOWH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11749433,
    'EMCS1',
    '02186702',
    'SERFC',
    'SC',
    '3060101',
    'EIGHTEENMILE CREEK BELOW PENDLETON~SC',
    34.645,
    -82.80056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EMCS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RKDI4',
    '05414400',
    'NCRFC',
    'IA',
    NULL,
    'MIDDLE FK LITTLE MAQUOKETA R NR RICKARDSVILLE IA',
    42.56056,
    -90.85972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RKDI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22274808,
    'WCCA1',
    '02408150',
    'SERFC',
    'AL',
    '3150107',
    'WALNUT CREEK ABOVE CLANTON~ ALA',
    32.89028,
    -86.57917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCCA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9202419,
    'MCLN7',
    '0212414900',
    'SERFC',
    'NC',
    '3040105',
    'MALLARD CR BL STONY CR NR HARRISBURG  NC',
    35.33277778,
    -80.7158333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCLN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9755438,
    'LRYN7',
    '0214291555',
    'SERFC',
    'NC',
    '3050101',
    'LONG CREEK NR RHYNE  NC',
    35.30055556,
    -80.9727778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRYN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9755476,
    'PAWN7',
    '0214295600',
    'SERFC',
    'NC',
    '3050101',
    'PAW CR AT WILKINSON BLVD NR CHARLOTTE  NC',
    35.24027778,
    -80.9744444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAWN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9731292,
    'ISNN7',
    '0214657975',
    'SERFC',
    'NC',
    '3050103',
    'IRVINS CREEK AT SR3168 NR CHARLOTTE  NC',
    35.1586111,
    -80.7133333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ISNN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9731320,
    'STSN7',
    '0214678175',
    'SERFC',
    'NC',
    '3050103',
    'STEELE CREEK AT SR1441 NR PINEVILLE  NC',
    35.105,
    -80.9536111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19626108,
    'FCFA1',
    '03576500',
    'LMRFC',
    'AL',
    '6030002',
    'FLINT CREEK NEAR FALKVILLE AL',
    34.37306,
    -86.93361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCFA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 83454,
    'GUSF1',
    '022907085',
    'SERFC',
    'FL',
    '3090202',
    'BLACK CREEK CANAL WEST OF U.S. 1 NR  GOULDS~ FL',
    25.57092,
    -80.37875
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GUSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ALTI2',
    '05587540',
    'NCRFC',
    'IL',
    NULL,
    'MELVIN PRICE LOCK POOL BELOW ALTON',
    38.86139,
    -90.1375
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALTI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LHFA2',
    '15226620',
    'AKRFC',
    'AK',
    NULL,
    'LOWE R AB HORSETAIL FALLS NR VALDEZ AK',
    61.066944,
    -145.906111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHFA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 189861,
    'NW564',
    '06711515',
    'MBRFC',
    'CO',
    '10190002',
    'LITTLE DRY CREEK NR ARAPAHOE RD AT CENTENNIAL~ CO',
    39.59305,
    -104.915
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW564'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4587100,
    'NW649',
    '04292750',
    'NERFC',
    'VT',
    '4150405',
    'MILL RIVER AT GEORGIA SHORE RD~ NR ST ALBANS~ VT',
    44.77944,
    -73.13555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW649'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6866233,
    'NW712',
    '04073466',
    'NCRFC',
    'WI',
    '4030201',
    'SILVER CREEK AT SPAULDING ROAD NEAR GREEN LAKE~ WI',
    43.84722,
    -88.915
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW712'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7043255,
    'NW716',
    '05074500',
    'NCRFC',
    'MN',
    '9020302',
    'RED LAKE RIVER NEAR RED LAKE~ MN',
    47.96583,
    -95.27111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW716'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1529669,
    'NW598',
    '07103100',
    'ABRFC',
    'CO',
    '11020003',
    'WILLIAMS CANYON ABV MOUTH NEAR MANITOU SPRINGS~ CO',
    38.89805,
    -104.93194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW598'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWKA2',
    '15302812',
    'AKRFC',
    'AK',
    NULL,
    'KOKWOK R 22 MI AB NUSHAGAK R NR EKWOK AK',
    59.415,
    -157.8041
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SGLA2',
    '15225997',
    'AKRFC',
    'AK',
    NULL,
    'SOLOMON GULCH AT TOP OF FALLS NR VALDEZ AK',
    61.079167,
    -146.303056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGLA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14704932,
    'NW834',
    '05400625',
    'NCRFC',
    'WI',
    '7070003',
    'LITTLE PLOVER RIVER NEAR PLOVER~ WI',
    44.47444,
    -89.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW834'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15649577,
    'NW860',
    '04191058',
    'OHRFC',
    'OH',
    '4100007',
    'Little Auglaize River at Melrose OH',
    41.08472,
    -84.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW860'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCRL1',
    '295744093303800',
    'LMRFC',
    'LA',
    NULL,
    'CRMS0651-H01-RT',
    29.96222,
    -93.51056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCRL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPHL1',
    '302020091435700',
    'LMRFC',
    'LA',
    NULL,
    'LAKE PELBA AT I-10 NEAR HENDERSON LA',
    30.33889,
    -91.7325
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPHL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MFBL1',
    '295447091191500',
    'LMRFC',
    'LA',
    NULL,
    'MIDDLE FORK BAYOU LONG AT BAYOU LONG',
    29.91306,
    -91.32083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MFBL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PBRL1',
    '301655091440800',
    'LMRFC',
    'LA',
    NULL,
    'PONTOON BRIDGE CANAL NEAR BUTTE LAROSE LA',
    30.28194,
    -91.73556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PBRL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RFRL1',
    '294045092492300',
    'LMRFC',
    'LA',
    NULL,
    'CRMS0615-H01-RT',
    29.67917,
    -92.82306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RFRL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DNAL1',
    '301324090382400',
    'LMRFC',
    'LA',
    NULL,
    'CRMS0061-H01-RT',
    30.223333,
    -90.64
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DNAL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LWCA2',
    '15239050',
    'AKRFC',
    'AK',
    NULL,
    'MF BRADLEY R NR HOMER AK',
    61.810278,
    -150.095
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LWCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LSUA2',
    '15290000',
    'AKRFC',
    'AK',
    NULL,
    'L SUSITNA R NR PALMER AK',
    61.710278,
    -149.229722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSUA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MATA2',
    '15284000',
    'AKRFC',
    'AK',
    NULL,
    'MATANUSKA R AT PALMER AK',
    61.608611,
    -149.073056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MATA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MDBA2',
    '15214000',
    'AKRFC',
    'AK',
    NULL,
    'COPPER R AT MILLION DOLLAR BRIDGE NR CORDOVA AK',
    60.671667,
    -144.744722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MDBA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOOA2',
    '15283700',
    'AKRFC',
    'AK',
    NULL,
    'MOOSE C NR PALMER AK',
    61.682802,
    -149.045541
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOOA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MXYA2',
    '15209700',
    'AKRFC',
    'AK',
    NULL,
    'WF KENNICOTT R AT MCCARTHY AK',
    61.434167,
    -142.940556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MXYA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NRDA2',
    '15302000',
    'AKRFC',
    'AK',
    NULL,
    'NUYAKUK R NR DILLINGHAM AK',
    59.935556,
    -158.187778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NRDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCBA2',
    '15297610',
    'AKRFC',
    'AK',
    NULL,
    'RUSSELL C NR COLD BAY AK',
    55.177778,
    -162.6875
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCBA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RCTA2',
    '15297475',
    'AKRFC',
    'AK',
    NULL,
    'RED CLOUD R TR NR KODIAK AK',
    57.8157,
    -152.624
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCTA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15662798,
    'NW861',
    '04185318',
    'OHRFC',
    'OH',
    '4100006',
    'Tiffin River near Evansport OH',
    41.37277,
    -84.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW861'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18421297,
    'NW904',
    '03423000',
    'OHRFC',
    'TN',
    '5130108',
    'FALLING WATER RIVER NEAR COOKEVILLE~ TN',
    36.06777,
    -85.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW904'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18487568,
    'NW907',
    '03331224',
    'OHRFC',
    'IN',
    '5120106',
    'SHATTO DITCH NEAR MENTONE~ IN',
    41.22027,
    -86.03388
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW907'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19345441,
    'NW919',
    '07367680',
    'LMRFC',
    'AR',
    '8050001',
    'Boeuf River nr Eudora~ AR',
    33.11861,
    -91.33888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW919'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19516762,
    'NW924',
    '03600358',
    'LMRFC',
    'TN',
    '6040003',
    'DUCK RIVER AT CRAIG BRIDGE RD AB WILLIAMSPORT~ TN',
    35.69472,
    -87.16944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW924'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20677939,
    'NW938',
    '09415250',
    'CBRFC',
    'NV',
    '15010005',
    'VIRGIN RV ABV LAKE MEAD NR OVERTON~ NV',
    36.50833,
    -114.33888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW938'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8915907,
    'NW741',
    '10291500',
    'CNRFC',
    'CA',
    '16050301',
    'BUCKEYE CK NR BRIDGEPORT~ CA',
    38.23722,
    -119.32194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW741'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21248258,
    'NW944',
    '07137010',
    'ABRFC',
    'KS',
    '11030001',
    'FRONTIER DITCH RETURN NR COOLIDGE~ KS',
    38.01694,
    -101.94888
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW944'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21250364,
    'NW945',
    '07138064',
    'ABRFC',
    'KS',
    '11030001',
    'SOUTHSIDE DITCH RETURN NR DEERFIELD~ KS',
    37.96583,
    -101.11861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW945'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SHIA2',
    '15276000',
    'AKRFC',
    'AK',
    NULL,
    'SHIP C NR ANCHORAGE AK',
    61.225556,
    -149.635
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SHIA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SIXA2',
    '15271000',
    'AKRFC',
    'AK',
    NULL,
    'SIXMILE C NR HOPE AK',
    60.820833,
    -149.425278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SIXA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SKLA2',
    '15266110',
    'AKRFC',
    'AK',
    NULL,
    'KENAI R BL SKILAK LK OUTLET NR STERLING AK',
    60.466389,
    -150.601389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SKLA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SNOA2',
    '15243900',
    'AKRFC',
    'AK',
    NULL,
    'SNOW R NR SEWARD AK',
    60.29444,
    -149.34604
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SNOA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SUNA2',
    '15292780',
    'AKRFC',
    'AK',
    NULL,
    'SUSITNA R AT SUNSHINE AK',
    62.175278,
    -150.173611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SUNA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SXRA2',
    '15266300',
    'AKRFC',
    'AK',
    NULL,
    'KENAI R AT SOLDOTNA AK',
    60.476667,
    -151.082222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SXRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRTA2',
    '15292700',
    'AKRFC',
    'AK',
    NULL,
    'TALKEETNA R NR TALKEETNA AK',
    62.346944,
    -150.016944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRTA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UNRA2',
    '15238648',
    'AKRFC',
    'AK',
    NULL,
    'UPPER NUKA R NR PARK BOUNDARY NR HOMER AK',
    59.684444,
    -150.703333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UNRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCWN2',
    '10348570',
    'CNRFC',
    'NV',
    NULL,
    'WINTERS CK BLW US ALT 395 NR WASHOE CITY NV',
    39.31239,
    -119.82321
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCWN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 89922,
    'TLRF1',
    '251127080382100',
    'SERFC',
    'FL',
    NULL,
    NULL,
    25.1909942626953,
    -80.6388397216797
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TLRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16770696,
    'FUBF1',
    '255327081275900',
    'SERFC',
    'FL',
    NULL,
    NULL,
    25.8953895568848,
    -81.4554977416992
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FUBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VGSA2',
    '15227090',
    'AKRFC',
    'AK',
    NULL,
    'VALDEZ GLACIER R AT VALDEZ GLACIER LK NR VALDEZ AK',
    61.148889,
    -146.171389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VGSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLVA2',
    '15236895',
    'AKRFC',
    'AK',
    NULL,
    'WOLVERINE GLACIER 24 MI NE OF SEWARD AK',
    60.3875,
    -148.935
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLVA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLCA2',
    '15236900',
    'AKRFC',
    'AK',
    NULL,
    'WOLVERINE C NR LAWING AK',
    60.370556,
    -148.896667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLWA2',
    '15294005',
    'AKRFC',
    'AK',
    NULL,
    'WILLOW C NR WILLOW AK',
    61.780833,
    -149.884444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLWA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YPSA2',
    '15565447',
    'AKRFC',
    'AK',
    NULL,
    'YUKON R AT PILOT STATION AK',
    61.934444,
    -162.880556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YPSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HPPH1',
    '16049000',
    'AKRFC',
    'HI',
    NULL,
    'HANAPEPE RIV BLW MANUAHI STR NR ELEELE KAUAI HI',
    21.95492,
    -159.55078
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HPPH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLLI2',
    '05578300',
    'NCRFC',
    'IL',
    NULL,
    'CLINTON LAKE NEAR LANE IL',
    40.14056,
    -88.88194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLLI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6279141,
    'NW691',
    '02191740',
    'SERFC',
    'GA',
    '3060104',
    'CLOUDS CREEK AT WATSON MILL PARK NR CARLTON~ GA',
    34.01694,
    -83.06777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW691'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2244409,
    'NW622',
    '02366650',
    'SERFC',
    'FL',
    '3140203',
    'PINE LOG CREEK NR EBRO~ FL',
    30.42361,
    -85.88111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW622'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7111205,
    'NW718',
    '05125039',
    'NCRFC',
    'MN',
    '9030001',
    'KEELEY CREEK ABOVE MOUTH NEAR BABBITT~ MN',
    47.77944,
    -91.74555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW718'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4587096,
    'NW648',
    '04292795',
    'NERFC',
    'VT',
    '4150405',
    'STEVENS BROOK AT KELLOGG ROAD~ NEAR ST. ALBANS~ VT',
    44.84722,
    -73.10166
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW648'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7110249,
    'NW717',
    '05124982',
    'NCRFC',
    'MN',
    '9030001',
    'FILSON CREEK IN SWSE SEC. 24~ NEAR WINTON~ MN',
    47.84722,
    -91.67777
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW717'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3820579,
    'NW643',
    '03107698',
    'OHRFC',
    'PA',
    '5030101',
    'Traverse Creek near Kendall~ PA',
    40.52527,
    -80.4575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW643'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CDPP4',
    '50093110',
    NULL,
    'PR',
    NULL,
    'CANAL DE PATILLAS AT INTAKE 113 SALINAS PR',
    17.98294,
    -66.2672
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CDPP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FAJP4',
    '50071000',
    NULL,
    'PR',
    NULL,
    'RIO FAJARDO NR FAJARDO PR',
    18.29895,
    -65.69383
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FAJP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HUSH1',
    '16275000',
    'AKRFC',
    'HI',
    NULL,
    'HEEIA STREAM AT HAIKU VALLEY NR KANEOHE OAHU HI',
    21.409167,
    -157.823333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HUSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BOCI4',
    '06483495',
    'MBRFC',
    'IA',
    NULL,
    'BURR OAK CREEK NEAR PERKINS IA',
    43.245278,
    -96.177222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOCI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GOGU1',
    '10172630',
    'CBRFC',
    'UT',
    NULL,
    'GOGGIN DRAIN NEAR MAGNA UTAH',
    40.81667,
    -112.1
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GOGU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16944520,
    'RABF1',
    '02313098',
    'SERFC',
    'FL',
    '3100208',
    'RAINBOW RIVER NEAR DUNNELLON~ FL',
    29.07131,
    -82.42661
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RABF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SEBP4',
    '50144000',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE ANASCO NR SAN SEBASTIAN PR',
    18.28417,
    -67.05096
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SEBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRUP4',
    '50059050',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE DE LOIZA BLW LOIZA DAMSITE PR',
    18.34227,
    -66.00598
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRUP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3438596,
    'OMHG1',
    '02342881',
    'SERFC',
    'GA',
    '3130003',
    'CHATTAHOOCHEE RIVER AT SPUR 39~ NEAR OMAHA~ GA',
    32.14222,
    -85.04639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OMHG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1047417,
    'PACG1',
    '02217643',
    'SERFC',
    'GA',
    '3070101',
    'PARKS CREEK AT LYLE FIELD RD NR JEFFERSON~ GA',
    34.16206,
    -83.52509
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PACG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11759050,
    'BOWN1',
    '06478522',
    'MBRFC',
    'NE',
    '10170101',
    'Bow Creek near Wynot~ Nebr.',
    42.76528,
    -97.17257
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOWN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GHBP1',
    '03027400',
    'OHRFC',
    'PA',
    NULL,
    'WEIR 1 AT EAST BRANCH CLARION RIVER DAM PA',
    41.554444,
    -78.595278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GHBP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3350287,
    'SIMN7',
    '02084160',
    'SERFC',
    'NC',
    '3020103',
    'CHICOD CR AT SR1760 NEAR SIMPSON  NC',
    35.56167,
    -77.23083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SIMN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10736861,
    'VLMN2',
    '10301120',
    'CNRFC',
    'NV',
    '16050303',
    'WALKER RV AT MILLER LN NR YERINGTON~ NV',
    39.04828,
    -119.13306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VLMN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSBU1',
    '10010020',
    'CBRFC',
    'UT',
    NULL,
    'GSL BREACH AT LAKESIDE UT',
    41.2225,
    -112.84917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSBU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCAT2',
    '08157560',
    'WGRFC',
    'TX',
    NULL,
    'WALLER CK AT E 1ST ST AUSTIN TX',
    30.261944,
    -97.739722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8551785,
    'HCRV2',
    '02025652',
    'MARFC',
    'VA',
    '2080203',
    'HARRIS CREEK BELOW ROUTE 130 NEAR MONROE~ VA',
    37.48528,
    -79.16667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCRV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4590269,
    'HGCV1',
    '04294140',
    'NERFC',
    'VT',
    '4150407',
    'ROCK RIVER NEAR HIGHGATE CENTER~ VT',
    44.96306,
    -72.99194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HGCV1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ARGA2',
    '15905100',
    'AKRFC',
    'AK',
    NULL,
    'ATIGUN R BL GALBRAITH LK NR PUMP STATION 4 AK',
    68.452222,
    -149.373333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ARGA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLAA2',
    '15564879',
    'AKRFC',
    'AK',
    NULL,
    'SLATE C AT COLDFOOT AK',
    67.254444,
    -150.177222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLAA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23065187,
    'JRVW1',
    '12445500',
    'NWRFC',
    'WA',
    '17020006',
    'JOHNSON CREEK NEAR RIVERSIDE~ WA',
    48.49722,
    -119.525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JRVW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23040731,
    'KRBW1',
    '12404900',
    'NWRFC',
    'WA',
    '17020002',
    'KETTLE RIVER NEAR BARSTOW~ WA',
    48.78472,
    -118.12417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KRBW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AAKA2',
    '15055500',
    'AKRFC',
    'AK',
    NULL,
    'ANTLER R BL ANTLER LK NR AUKE BAY AK',
    58.851944,
    -134.708611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AAKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23065279,
    'LLPW1',
    '12447285',
    'NWRFC',
    'WA',
    '17020006',
    'LOUP LOUP CREEK AT MALOTT~ WA',
    48.28333,
    -119.70722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLPW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1814983,
    'TYLW3',
    '04026561',
    'NCRFC',
    'WI',
    '4010302',
    'TYLER FORKS RIVER AT STRICKER ROAD NEAR MELLEN~ WI',
    46.39472,
    -90.59
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TYLW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13082921,
    'BOMW3',
    '053674967',
    'NCRFC',
    'WI',
    '17050007',
    'TROUT CREEK AT TENTH STREET NEAR BLOOMER~ WI',
    45.09944,
    -91.65111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOMW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5895480,
    'BHMW2',
    '01616400',
    'MARFC',
    'WV',
    '2070004',
    'MILL CREEK AT BUNKER HILL~ WV',
    39.33463,
    -78.05344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BHMW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2091809,
    'NW1016',
    '03250322',
    'OHRFC',
    'KY',
    '5100101',
    'ROCK LICK CR AT STATE HWY 158 NR SHARKEY  KY STA D',
    38.23722,
    -83.59305
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1016'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19618866,
    'ESFA1',
    '03574100',
    'LMRFC',
    'AL',
    '6030002',
    'ESTILL FORK AT ESTILLFORK AL',
    34.91,
    -86.16833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ESFA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19647854,
    'SSRA1',
    '03572690',
    'LMRFC',
    'AL',
    '6030001',
    'SOUTH SAUTY CREEK NEAR RAINSVILLE~ ALA',
    34.49861,
    -85.92972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSRA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7818688,
    'CCCA4',
    '07261250',
    'ABRFC',
    'AR',
    '11110205',
    'Cadron Creek West of Conway~ AR',
    35.11472,
    -92.52472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7818014,
    'ENOA4',
    '07261200',
    'ABRFC',
    'AR',
    '11110205',
    'East Fork Cadron Creek near Enola~ AR',
    35.21833,
    -92.27944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ENOA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8944527,
    'TCMC1',
    '10336770',
    'CNRFC',
    'CA',
    '16050101',
    'TROUT CK AT USFS RD 12N01 NR MEYERS  CA',
    38.8632405,
    -119.9582367
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCMC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11779929,
    'LETA4',
    '07076530',
    'LMRFC',
    'AR',
    '11010014',
    'Big Creek near Letona~ AR',
    35.36197,
    -91.80103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LETA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7816620,
    'QUTA4',
    '07260990',
    'ABRFC',
    'AR',
    '11110205',
    'North Fork Cadron Creek near Quitman~ AR',
    35.39417,
    -92.29694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QUTA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ADDO2',
    '07313585',
    'ABRFC',
    'OK',
    NULL,
    'COW CREEK NR ADDINGTON OK',
    34.245,
    -97.9742
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ADDO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SAMA2',
    '15057596',
    'AKRFC',
    'AK',
    NULL,
    'SALMON R AT GUSTAVUS AK',
    58.444722,
    -135.741944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAMA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7787336,
    'REMA4',
    '07260678',
    'ABRFC',
    'AR',
    '11110203',
    'East Fork Point Remove Creek nr Morrilton~ AR',
    35.28417,
    -92.68111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'REMA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11777215,
    'SFGA4',
    '07075270',
    'LMRFC',
    'AR',
    '11010014',
    'South Fork of Little Red River near Scotland~ AR',
    35.56972,
    -92.62194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFGA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11798549,
    'TUCA4',
    '07074670',
    'LMRFC',
    'AR',
    '11010013',
    'Tuckerman Ditch at Tuckerman~ AR',
    35.7265,
    -91.20047
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TUCA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17014191,
    'CBDC2',
    '09359082',
    'CBRFC',
    'CO',
    '14080104',
    'CASCADE CR BLW CASCADE CR DIVERSION NR ROCKWOOD CO',
    37.667,
    -107.82225
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBDC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1327445,
    'HUTC2',
    '09074500',
    'CBRFC',
    'CO',
    '14010004',
    'HUNTER CREEK AT ASPEN~ CO',
    39.19689,
    -106.81892
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HUTC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3255135,
    'LFBC2',
    '09123450',
    'CBRFC',
    'CO',
    '14020002',
    'LAKE FORK BLW LAKE SAN CRISTOBAL NR LAKE CITY~ CO',
    37.98436,
    -107.2921
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LFBC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1529605,
    'FCDC2',
    '07100300',
    'ABRFC',
    'CO',
    '11020003',
    'FOUNTAIN CREEK AT CASCADE~ CO',
    38.89708,
    -104.97178
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCDC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18252975,
    'NATF1',
    '02231291',
    'SERFC',
    'FL',
    '3070205',
    'NASSAU RIVER NEAR TISONIA~ FL',
    30.55056,
    -81.59
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NATF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16667949,
    'RLGF1',
    '02244333',
    'SERFC',
    'FL',
    '3080103',
    'HAW CREEK AB RUSSELLS LANDING NR ST JOHNS PARK FL',
    29.39444,
    -81.37139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RLGF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16634294,
    'ABEF1',
    '02237700',
    'SERFC',
    'FL',
    '3080102',
    'APOPKA-BEAUCLAIR CANAL NEAR ASTATULA~ FL',
    28.72222,
    -81.685
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ABEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21478144,
    'CCVF1',
    '02264000',
    'SERFC',
    'FL',
    '3090101',
    'CYPRESS CREEK AT VINELAND~ FL',
    28.39028,
    -81.51972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21478146,
    'LBUF1',
    '02264030',
    'SERFC',
    'FL',
    '3090101',
    'LK BUENA VIS TR AT HOTEL PL BLVD AT LK BUENA VISTA',
    28.3805,
    -81.50697
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LBUF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16657895,
    'DCDF1',
    '02246804',
    'SERFC',
    'FL',
    '3080103',
    'DUNN CREEK AT DUNN CREEK RD NR EASTPORT~ FL',
    30.45494,
    -81.59689
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CDOA4',
    NULL,
    'LMRFC',
    'AR',
    NULL,
    'TEMPLE ISLAND HW/TW NEAR HUMPHREY AR',
    34.37681,
    -91.71081
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CDOA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NAGA4',
    '07265283',
    'LMRFC',
    'AR',
    NULL,
    'AR RIVER @ DAM NO.2 NEAR GILLETT AR',
    33.98889,
    -91.31306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NAGA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16928268,
    'HRHF1',
    '02310689',
    'SERFC',
    'FL',
    '3100207',
    'HALLS RIVER AT HOMOSASSA SPRINGS FL',
    28.81311,
    -82.60564
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16919448,
    'NACF1',
    '02301740',
    'SERFC',
    'FL',
    '3100206',
    'NORTH ARCHIE CREEK AT PROGRESS BLVD. NEAR TAMPA FL',
    27.89639,
    -82.35
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NACF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ULRA4',
    NULL,
    'LMRFC',
    'AR',
    NULL,
    'UPPER VALLIER HW/TW NEAR LODGE CORNER AR',
    34.27675,
    -91.64439
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ULRA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16916774,
    'SBBF1',
    '02307445',
    'SERFC',
    'FL',
    '3100206',
    'SOUTH BRANCH BROOKER CREEK NEAR OLDSMAR FL',
    28.07706,
    -82.69778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16802169,
    'WLAF1',
    '02295580',
    'SERFC',
    'FL',
    '3100101',
    'LITTLE CHARLIE CREEK NEAR MOUTH NEAR WAUCHULA FL',
    27.57906,
    -81.79683
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16955326,
    'WRCF1',
    '02311000',
    'SERFC',
    'FL',
    '3100208',
    'WITHLACOOCHEE-HILLSBOROUGH OVFLO NEAR RICHLAND~ FL',
    28.27111,
    -82.09806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6411850,
    'CCKG1',
    '02344280',
    'SERFC',
    'GA',
    '3130005',
    'CAMP CREEK AT HELMER ROAD~ NEAR FAYETTEVILLE~ GA',
    33.52583,
    -84.43389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCKG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6363594,
    'CLFG1',
    '02214590',
    'SERFC',
    'GA',
    '3070104',
    'BIG INDIAN CREEK AT US 341~ NEAR CLINCHFIELD~ GA',
    32.42667,
    -83.64444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLFG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMIA3',
    '09423550',
    'CBRFC',
    'AZ',
    NULL,
    'TOPOCK MARSH INLET NEAR NEEDLES CA',
    34.83611,
    -114.58417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMIA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6331758,
    'CNCG1',
    '02208493',
    'SERFC',
    'GA',
    '3070103',
    'CORNISH CREEK AT HAZELBRAND RD NR COVINGTON~ GA',
    33.6285,
    -83.79886
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6331514,
    'EBCG1',
    '02209360',
    'SERFC',
    'GA',
    '3070103',
    'EAST BEAR CREEK AT POPLAR ROAD~ NR MANSFIELD~ GA',
    33.50725,
    -83.77025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EBCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6278153,
    'GCBG1',
    '02191227',
    'SERFC',
    'GA',
    '3060104',
    'GROVE CREEK AT US 441 AND GA 15 NEAR COMMERCE~ GA',
    34.2695,
    -83.46767
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCBG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17596097,
    'SXPC1',
    '11120520',
    'CNRFC',
    'CA',
    NULL,
    'SAN PEDRO C A GOLETA CA',
    34.44861,
    -119.84028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SXPC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PZCC1',
    '10341100',
    'CNRFC',
    'CA',
    NULL,
    'PERAZZO C NR SODA SPRINGS CA',
    39.45486,
    -120.39122
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PZCC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GWKC1',
    '11302000',
    'CNRFC',
    'CA',
    NULL,
    'STANISLAUS R BL GOODWIN DAM NR KNIGHTS FERRY CA',
    37.85167,
    -120.63694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GWKC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6331116,
    'LCBG1',
    '02208487',
    'SERFC',
    'GA',
    '3070103',
    'LITTLE CORNISH CR (COUNTY ROAD 95) NR OXFORD~ GA',
    33.67944,
    -83.82972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCBG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6413022,
    'LKTG1',
    '02344655',
    'SERFC',
    'GA',
    '3130005',
    'FLAT CR DS OF LAKE KEDRON~ NR PEACHTREE CITY~ GA',
    33.42167,
    -84.57833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKTG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6333488,
    'LSMG1',
    '02207135',
    'SERFC',
    'GA',
    '3070103',
    'LITTLE STONE MTN CR NEAR STONE MOUNTAIN~ GA',
    33.83056,
    -84.13944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSMG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6411782,
    'MCFG1',
    '02344327',
    'SERFC',
    'GA',
    '3130005',
    'MORNING CRK AT WESTBRIDGE RD~ NR FAYETTEVILLE~ GA',
    33.54139,
    -84.48417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCFG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5923734,
    'JSEK1',
    '06855850',
    'MBRFC',
    'KS',
    '10250017',
    'BUFFALO C 4 MI E OF JAMESTOWN~ KS',
    39.59279,
    -97.78148
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JSEK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VGBP4',
    '50039500',
    NULL,
    'PR',
    NULL,
    'RIO CIBUCO AT VEGA BAJA PR',
    18.44733,
    -66.37438
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VGBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HMTC2',
    '09032200',
    'CBRFC',
    'CO',
    NULL,
    'HAMILTON CREEK NEAR TABERNASH CO',
    39.99728,
    -105.74681
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMTC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6269218,
    'TRRG1',
    '02181350',
    'SERFC',
    'GA',
    '3060102',
    'TALLULAH R AT TERRORA PWRHSE~ NR TALLULAH FALLS~GA',
    34.75028,
    -83.405
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRRG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4995175,
    'RRDI4',
    '05488110',
    'NCRFC',
    'IA',
    '7100009',
    'Des Moines River near Pella~ IA',
    41.36056,
    -92.97306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RRDI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13375515,
    'GNII4',
    '05418720',
    'NCRFC',
    'IA',
    '7060006',
    'Maquoketa River near Green Island~ IA',
    42.16386,
    -90.33508
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GNII4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13770104,
    'FCKI2',
    '05590520',
    'NCRFC',
    'IL',
    '7140201',
    'KASKASKIA RIVER BELOW FICKLIN~ IL',
    39.79167,
    -88.36778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCKI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10082018,
    'SSJI2',
    '03336890',
    'OHRFC',
    'IL',
    '5120109',
    'SPOON RIVER NEAR ST. JOSEPH~ IL',
    40.16417,
    -88.0275
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SSJI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LTCC2',
    '09032050',
    'CBRFC',
    'CO',
    NULL,
    'LITTLE CABIN CREEK NEAR TABERNASH CO',
    39.97454,
    -105.73954
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LTCC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14785333,
    'EVGI2',
    '05531045',
    'NCRFC',
    'IL',
    '7120004',
    'SALT CREEK AT ELK GROVE VILLAGE~ IL',
    42.01208,
    -88.001
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EVGI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13463871,
    'GLWI2',
    '05536215',
    'NCRFC',
    'IL',
    '7120003',
    'THORN CREEK AT GLENWOOD~ IL',
    41.53028,
    -87.62222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLWI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13871382,
    'KCAI2',
    '05593000',
    'NCRFC',
    'IL',
    '7140202',
    'KASKASKIA RIVER AT CARLYLE~ IL',
    38.61167,
    -89.35611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KCAI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23719395,
    'WSSO3',
    '14095500',
    'NWRFC',
    'OR',
    '17070306',
    'WARM SPRINGS RIVER NEAR SIMNASHO  OR',
    44.96708766,
    -121.4693975
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSSO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20907947,
    'BZRK1',
    '07182200',
    'ABRFC',
    'KS',
    '11070203',
    'SF COTTONWOOD R NR BAZAAR~ KS',
    38.28556,
    -96.5125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BZRK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14768740,
    'LGNW3',
    '055451345',
    'NCRFC',
    'WI',
    '7120006',
    'WHITE RIVER AT CENTER STREET AT LAKE GENEVA  WI',
    42.59055556,
    -88.4336111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LGNW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WHCC2',
    '07134990',
    'ABRFC',
    'CO',
    NULL,
    'WILD HORSE CREEK ABOVE HOLLY CO',
    38.05703,
    -102.13847
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WHCC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DUCF1',
    '022462002',
    'SERFC',
    'FL',
    NULL,
    'DURBIN CREEK NEAR FRUIT COVE FL',
    30.099167,
    -81.526111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DUCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BEDF1',
    '260725080451001',
    'SERFC',
    'FL',
    NULL,
    'EDEN 5 IN WATER CONSERVATION AREA 3-A',
    26.12361,
    -80.75278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BEDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NSRS1',
    '021623961',
    'SERFC',
    'SC',
    NULL,
    'NORTH SALUDA RESERVOIR NEAR TIGERVILLESC',
    35.14167,
    -82.40861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NSRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21164973,
    'CALK1',
    '07144486',
    'ABRFC',
    'KS',
    '11030013',
    'CALFSKIN C AT 119TH ST~ WICHITA~ KS',
    37.67417,
    -97.48028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CALK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18885046,
    'GYSK1',
    '06870300',
    'MBRFC',
    'KS',
    '10260008',
    'GYPSUM C NR GYPSUM~ KS',
    38.62726,
    -97.42756
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GYSK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21165001,
    'KEGK1',
    '07144490',
    'ABRFC',
    'KS',
    '11030013',
    'COWSKIN C AT KELLOGG ST~ WICHITA~ KS',
    37.66568,
    -97.4577
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KEGK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21037175,
    'SLDK1',
    '07148111',
    'ABRFC',
    'KS',
    '11060001',
    'GROUSE C NR SILVERDALE~ KS',
    37.04844,
    -96.89131
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLDK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10188772,
    'NCFK2',
    '03400986',
    'OHRFC',
    'KY',
    '5130101',
    'MARTIN''S FORK NEAR HARLAN~ KY',
    36.84444,
    -83.32361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NCFK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11688422,
    'ABGM2',
    '0158175320',
    'MARFC',
    'MD',
    '2060003',
    'WHEEL CREEK NEAR ABINGDON~ MD',
    39.48164,
    -76.34008
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ABGM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCCF1',
    '262200080210001',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO CA AT S-10-C NR DEERFIELD BCH. FL',
    26.37125,
    -80.35092
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'IPCF1',
    '02258800',
    'SERFC',
    'FL',
    NULL,
    'INDIAN PRAIRIE CANAL AB PUMP STA NR OKEECHOBEE FL',
    27.15438,
    -81.06998
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IPCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4505876,
    'BTMM2',
    '01644390',
    'MARFC',
    'MD',
    '2070008',
    'TEN MILE CREEK NEAR BOYDS~ MD',
    39.216,
    -77.31642
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BTMM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4505772,
    'CTMM2',
    '01644388',
    'MARFC',
    'MD',
    '2070008',
    'TEN MILE CREEK NEAR CLARKSBURG~ MD',
    39.22339,
    -77.31225
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CTMM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11905598,
    'CWLM2',
    '01593370',
    'MARFC',
    'MD',
    '2060006',
    'L PAX RIV TRIB ABOVE WILDE LAKE AT COLUMBIA~ MD',
    39.22578,
    -76.87006
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWLM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OVLS2',
    '433326096405200',
    'MBRFC',
    'SD',
    NULL,
    'PRECIP AT OAK VIEW LIBRARY AT SIOUX FALLS SD',
    43.55722,
    -96.68111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OVLS2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10010638,
    'PVVC1',
    '09429000',
    'CBRFC',
    'CA',
    '15030104',
    'PALO VERDE CANAL NEAR BLYTHE  CA',
    33.73194,
    -114.51111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PVVC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11905670,
    'EBLM2',
    '01593450',
    'MARFC',
    'MD',
    '2060006',
    'L PAX RIV TRIB ABOVE LAKE ELKHORN NR GUILFORD~ MD',
    39.18789,
    -76.83072
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EBLM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11689162,
    'HRSM2',
    '01585219',
    'MARFC',
    'MD',
    '2060003',
    'HERRING RUN AT SINCLAIR LANE AT BALTIMORE~ MD',
    39.31796,
    -76.55513
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRSM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11688792,
    'LUTM2',
    '01583800',
    'MARFC',
    'MD',
    '2060003',
    'LONG QUARTER BRANCH AT LUTHERVILLE~ MD',
    39.42561,
    -76.59619
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LUTM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6720973,
    'SBOM1',
    '01063310',
    'NERFC',
    'ME',
    '1060001',
    'Stony Brook at East Sebago~ Maine',
    43.85556,
    -70.63972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBOM1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6724855,
    'WBKM1',
    '01064118',
    'NERFC',
    'ME',
    '1060001',
    'Presumpscot River at Westbrook~ Maine',
    43.68694,
    -70.34722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBKM1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12027296,
    'FRLM4',
    '04043097',
    'NCRFC',
    'MI',
    '4020105',
    'FALLS RIVER NEAR L''ANSE~ MI',
    46.73472,
    -88.44306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRLM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1769404,
    'IBOM5',
    '04015438',
    'NCRFC',
    'MN',
    '4010201',
    'ST. LOUIS RIVER NEAR SKIBO~ MN',
    47.48111,
    -92.04
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IBOM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7163988,
    'RNYM5',
    '05129515',
    'NCRFC',
    'MN',
    '9030004',
    'RAINY R AT BOAT LANDING BLW INTERNATIONAL FALLS~MN',
    48.59222,
    -93.44667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RNYM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5966530,
    'BOYM7',
    '06901205',
    'MBRFC',
    'MO',
    '10280103',
    'East Locust Creek near Boynton~ MO',
    40.25889,
    -93.08333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOYM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5966260,
    'BRWM7',
    '06901250',
    'MBRFC',
    'MO',
    '10280103',
    'Little East Locust Creek near Browning~ MO',
    40.06556,
    -93.14028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRWM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5967976,
    'CHUM7',
    '06900640',
    'MBRFC',
    'MO',
    '10280103',
    'Muddy Creek near Chula~ MO',
    39.87944,
    -93.39722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHUM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5965096,
    'LCRM7',
    '06899900',
    'MBRFC',
    'MO',
    '10280103',
    'Medicine Creek at Lucerne~ MO',
    40.45444,
    -93.28639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCRM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHMF1',
    '02310674',
    'SERFC',
    'FL',
    NULL,
    'CHASSAHOWITZKA R AT MOUTH NR CHASSAHOWITZKA FL',
    28.694444,
    -82.639167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5969054,
    'MNDM7',
    '06902995',
    'MBRFC',
    'MO',
    '10280103',
    'Hickory Branch near Mendon~ MO',
    39.5825,
    -93.13306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNDM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4637509,
    'BNTM7',
    '07017610',
    'NCRFC',
    'MO',
    '7140104',
    'Big River below Bonne Terre~ MO',
    37.96553,
    -90.57442
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BNTM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRJP4',
    '50059210',
    NULL,
    'PR',
    NULL,
    'QUEBRADA GRANDE AT BO. DOS BOCAS PR',
    18.35044,
    -65.99046
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRJP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SFWT2',
    '08174200',
    'WGRFC',
    'TX',
    NULL,
    'SANDY FORK CK AT HWY 97 NR WAELDER TX',
    29.62491,
    -97.32074
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFWT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3624261,
    'LDDM7',
    '07010040',
    'NCRFC',
    'MO',
    '7140101',
    'Denny Creek at Ladue~ MO',
    38.64289,
    -90.40111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LDDM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3624401,
    'RDPM7',
    '07010088',
    'NCRFC',
    'MO',
    '7140101',
    'River Des Peres at Shrewsbury~ MO',
    38.58683,
    -90.31303
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDPM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5640404,
    'SVLM7',
    '05503100',
    'NCRFC',
    'MO',
    '7110005',
    'Black Creek below Shelbyville~ MO',
    39.73806,
    -91.93611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SVLM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13236539,
    'RCLM8',
    '06295220',
    'MBRFC',
    'MT',
    '10100003',
    'Rosebud C Bel Lame Deer C nr Lame Deer MT',
    45.67583,
    -106.69972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCLM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12377951,
    'LTPM8',
    '06111800',
    'MBRFC',
    'MT',
    '10040103',
    'Big Spring Cr at R&B Trading Post nr Lewistown MT',
    47.08783,
    -109.4575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LTPM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8896032,
    'BOCN7',
    '0209734440',
    'SERFC',
    'NC',
    '3030002',
    'BOLIN CREEK AT VILLAGE DRIVE AT CHAPEL HILL~ NC',
    35.92231,
    -79.066
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOCN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8783803,
    'CMVN7',
    '0208732534',
    'SERFC',
    'NC',
    '3020201',
    'PIGEON HOUSE CR AT CAMERON VILLAGE AT RALEIGH~ NC',
    35.7875,
    -78.65472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMVN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16801573,
    'PCSF1',
    '02294775',
    'SERFC',
    'FL',
    NULL,
    'PEACE RIVER AT CLEAR SPRINGS NEAR BARTOW FL',
    27.86333,
    -81.8075
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17405601,
    'DEDN1',
    '06803502',
    'MBRFC',
    'NE',
    '10200203',
    'Deadmans Run at 38th Street at Lincoln~ Nebr.',
    40.83536,
    -96.66556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DEDN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6741448,
    'ALLN3',
    '01089925',
    'NERFC',
    'NH',
    '1070002',
    'SUNCOOK RIVER AT NH 28~ NEAR SUNCOOK~ NH',
    43.15972,
    -71.40639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALLN3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19335075,
    'PEAN3',
    '01054114',
    'NERFC',
    'NH',
    '1040002',
    'Peabody River at Gorham~ New Hampshire',
    44.38132,
    -71.17066
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PEAN3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5848186,
    'RAYN3',
    '01073319',
    'NERFC',
    'NH',
    '1060003',
    'LAMPREY RIVER AT LANGFORD ROAD~ AT RAYMOND~ NH',
    43.04139,
    -71.20167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RAYN3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9513660,
    'HAMN4',
    '01402630',
    'MARFC',
    'NJ',
    '2030105',
    'Royce Brook at Hamilton Road near Manville NJ',
    40.50611,
    -74.62306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HAMN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9453855,
    'LANN4',
    '01408900',
    'MARFC',
    'NJ',
    '2040301',
    'Cedar Creek at Western Blvd near Lanoka Harbor NJ',
    39.87917,
    -74.19056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LANN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17806268,
    'BAJN5',
    '08329870',
    'WGRFC',
    'NM',
    '13020204',
    'BEAR ARROYO AT JEFFERSON ST AT ALBQ~ NM',
    35.15083,
    -106.59778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BAJN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17806324,
    'NFCN5',
    '08329835',
    'WGRFC',
    'NM',
    '13020203',
    'N. FLOODWAY CHANNEL AT ALBUQUERQUE~ NM',
    35.1175,
    -106.61167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFCN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17844466,
    'RCCN5',
    '08281400',
    'WGRFC',
    'NM',
    '13020102',
    'RIO CHAMA ABOVE CHAMA~ NM',
    36.93494,
    -106.55462
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCCN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SXZF1',
    '02294747',
    'SERFC',
    'FL',
    NULL,
    'SIXMILE CREEK AT BARTOW FL',
    27.8635,
    -81.80948
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SXZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11230250,
    'HCKN2',
    '10322535',
    'CNRFC',
    'NV',
    '16040104',
    'HENDERSON CK BLW VINNINI CK NR EUREKA~ NV',
    39.869,
    -116.16694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCKN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11227656,
    'PCKN2',
    '10322800',
    'CNRFC',
    'NV',
    '16040104',
    'PINE CK AT MODARELLI MINE RD NR HAY RANCH~ NV',
    40.38125,
    -116.12514
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCKN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11230228,
    'PHCN2',
    '10322555',
    'CNRFC',
    'NV',
    '16040104',
    'PETE HANSON CK ABV HENDERSON CK NR EUREKA~ NV',
    39.89031,
    -116.37828
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11849007,
    'QNRN2',
    '103530001',
    'CNRFC',
    'NV',
    '16040201',
    'QUINN RV BLW CONFL E FK S FK QUINN NR MC DERMITT',
    41.974,
    -117.59569
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QNRN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8924007,
    'EFDN2',
    '10309010',
    'CNRFC',
    'NV',
    '16050201',
    'E FK CARSON RV NR DRESSLERVILLE~ NV',
    38.88014,
    -119.69181
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EFDN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8932056,
    'TSGN2',
    '10348036',
    'CNRFC',
    'NV',
    '16050102',
    'TRUCKEE RV AT GLENDALE AVE NR SPARKS~ NV',
    39.52583,
    -119.77664
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TSGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YESG1',
    '02206500',
    'SERFC',
    'GA',
    NULL,
    'YELLOW RIVER NEAR SNELLVILLE GA',
    33.853056,
    -84.078333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YESG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6867135,
    'NW713',
    '04073458',
    'NCRFC',
    'WI',
    '4030201',
    'ROY CREEK AT ROY CREEK ROAD NEAR GREEN LAKE~ WI',
    43.7625,
    -89.01694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW713'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10737511,
    'WBWN2',
    '10301720',
    'CNRFC',
    'NV',
    '16050303',
    'WALKER RV AT PT SITE BLW WEBER RES NR SCHURZ~ NV',
    39.03389,
    -118.86139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBWN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20642190,
    'ASCN2',
    '09415645',
    'CBRFC',
    'NV',
    '15010011',
    'ASH SPGS CK BLW DIV AT HWY 93 AT ASH SPGS~ NV',
    37.46,
    -115.195
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ASCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22069422,
    'CCSN2',
    '09419625',
    'CBRFC',
    'NV',
    '15010015',
    'CORN CK SPGS AT NATIONAL FISH & WILDLIFE HDQRS~ NV',
    36.43884,
    -115.35812
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCSN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22743357,
    'FRFN6',
    '01342682',
    'NERFC',
    'NY',
    '2020004',
    'MOYER CREEK NEAR FRANKFORT NY',
    43.02694,
    -75.10431
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRFN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22739849,
    'GRYN6',
    '01343403',
    'NERFC',
    'NY',
    '2020004',
    'BLACK CREEK NEAR GRAY NY',
    43.28278,
    -74.95919
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRYN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22744699,
    'MOHN6',
    '01342743',
    'NERFC',
    'NY',
    '2020004',
    'FULMER CREEK NEAR MOHAWK NY',
    42.98522,
    -74.99044
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOHN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ALKH1',
    '16725000',
    'AKRFC',
    'HI',
    NULL,
    'ALAKAHI STREAM NEAR KAMUELA HI',
    20.071111,
    -155.670833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALKH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22742959,
    'ORKN6',
    '01338000',
    'NERFC',
    'NY',
    '2020004',
    'ORISKANY CREEK NEAR ORISKANY NY',
    43.14278,
    -75.33781
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ORKN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22288449,
    'TMSN6',
    '01328770',
    'NERFC',
    'NY',
    '2020003',
    'HUDSON RIVER AT THOMSON NY',
    43.12539,
    -73.58492
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMSN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22743091,
    'WHBN6',
    '01339060',
    'NERFC',
    'NY',
    '2020004',
    'SAUQUOIT CREEK AT WHITESBORO NY',
    43.11144,
    -75.294
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WHBN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KOSH1',
    '16210200',
    'AKRFC',
    'HI',
    NULL,
    'KAUKONAHUA STREAM BLW WAHIAWA RESERVOIR OAHU HI',
    21.5,
    -158.050833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KOSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KNNH1',
    '16330000',
    'AKRFC',
    'HI',
    NULL,
    'KAMANANUI STR AT MAUNAWAI OAHU HI',
    21.635472,
    -158.054583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNNH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15559629,
    'BURN6',
    '04219768',
    'NERFC',
    'NY',
    '4130001',
    'EIGHTEENMILE CREEK AT BURT NY',
    43.31381,
    -78.71542
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BURN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15559687,
    'KNVN6',
    '0422016550',
    'NERFC',
    'NY',
    '4130001',
    'OAK ORCHARD CREEK NEAR KENYONVILLE NY',
    43.30111,
    -78.31053
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNVN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13154163,
    'MQGO1',
    '03094704',
    'OHRFC',
    'OH',
    '5030102',
    'Mosquito Creek near Greene Center OH',
    41.48306,
    -80.74611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MQGO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15431096,
    'HRTO1',
    '03115644',
    'OHRFC',
    'OH',
    '5030201',
    'East Fork Duck Creek near Harrietsville OH',
    39.61288,
    -81.37198
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRTO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 254998,
    'LHCO2',
    '07240000',
    'ABRFC',
    'OK',
    '11050002',
    'Lake Hefner Canal near Oklahoma City~ OK',
    35.55306,
    -97.61972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHCO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 430546,
    'TPCO2',
    '07331383',
    'ABRFC',
    'OK',
    '11130304',
    'Pennington Creek at Capitol Ave at Tishomingo~ Ok',
    34.23472,
    -96.6825
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TPCO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23923430,
    'RFHO3',
    '14335072',
    'NWRFC',
    'OR',
    '17100307',
    'ROGUE R AT COLE M RIVERS F HATCHERY NR MCLEOD~ OR',
    42.66583,
    -122.68639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RFHO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23809416,
    'OGRO3',
    '14209250',
    'NWRFC',
    'OR',
    '17090011',
    'OAK GROVE FORK AT RIPPLEBROOK CAMPGROUND~ OR',
    45.07981,
    -122.04286
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OGRO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8153625,
    'CCPP1',
    '01553850',
    'MARFC',
    'PA',
    '2050206',
    'Chillisquaque Creek near Potts Grove~ PA',
    40.97444,
    -76.8
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCPP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9707984,
    'CBRS1',
    '02162035',
    'SERFC',
    'SC',
    '3050106',
    'BROAD RIVER NEAR COLUMBIA~ SC',
    34.04833,
    -81.07333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBRS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18401497,
    'DONT1',
    '03430200',
    'OHRFC',
    'TN',
    '5130203',
    'STONES RIVER AT US HWY 70 NEAR DONELSON~ TN',
    36.18639,
    -86.63278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DONT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19532008,
    'PTVT1',
    '03599419',
    'LMRFC',
    'TN',
    '6040002',
    'DUCK RIVER AT MILE 156 NEAR POTTSVILLE~ TN',
    35.57028,
    -86.87139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTVT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18421703,
    'STWT1',
    '03424860',
    'OHRFC',
    'TN',
    '5130108',
    'CANEY FORK AT STONEWALL',
    36.18611,
    -85.90444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STWT1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 606903,
    'ZACT2',
    '08458995',
    'WGRFC',
    'TX',
    '13080002',
    'Zacate Ck at Laredo~ TX',
    27.5361,
    -99.50018
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ZACT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5781417,
    'BCMT2',
    '08158813',
    'WGRFC',
    'TX',
    '12090205',
    'Bear Ck at Spillar Ranch Rd nr Manchaca~ TX',
    30.16366,
    -97.90702
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCMT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3839263,
    'ECKT2',
    '08187500',
    'WGRFC',
    'TX',
    '12100303',
    'Escondido Ck at Kenedy~ TX',
    28.81972,
    -97.85889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ECKT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10840824,
    'SAFT2',
    '08178500',
    'WGRFC',
    'TX',
    '12100301',
    'San Pedro Ck at Furnish St~ San Antonio~ TX',
    29.40611,
    -98.51056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10835018,
    'SGHT2',
    '08180586',
    'WGRFC',
    'TX',
    '12100302',
    'San Geronimo Ck nr Helotes~ TX',
    29.61973,
    -98.79515
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGHT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5489963,
    'LEUT2',
    '08084200',
    'WGRFC',
    'TX',
    '12060102',
    'Clear Fk Brazos Rv at Lueders~ TX',
    32.795,
    -99.60569
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LEUT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13700695,
    'RUBT2',
    '08080505',
    'WGRFC',
    'TX',
    '12050004',
    'DMF Brazos Rv nr Rule~ TX',
    33.18792,
    -99.96014
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RUBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3379149,
    'CRPU1',
    '09185600',
    'CBRFC',
    'UT',
    '14030005',
    'COLORADO RIVER AT POTASH~ UT',
    38.50472,
    -109.65833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRPU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4931806,
    'GMBU1',
    '09328920',
    'CBRFC',
    'UT',
    '14060008',
    'GREEN RIVER AT MINERAL BOTTOM NR CYNLNDS NTL PARK',
    38.52403,
    -109.99442
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GMBU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10274376,
    'OGAU1',
    '10140700',
    'CBRFC',
    'UT',
    '16020102',
    'OGDEN RIVER NR GIBSON AVENUE AT OGDEN~ UT',
    41.23182,
    -111.9845
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OGAU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4879797,
    'SRIU1',
    '09328910',
    'CBRFC',
    'UT',
    '14060009',
    'SAN RAFAEL RIVER AT MOUTH NEAR GREEN RIVER~ UT',
    38.75776,
    -110.14353
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRIU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5892920,
    'BWSW2',
    '01613030',
    'MARFC',
    'WV',
    '2070004',
    'WARM SPRINGS RUN NEAR BERKELEY SPRINGS~ WV',
    39.64058,
    -78.21894
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BWSW2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15961554,
    'LMDW4',
    '06634060',
    'MBRFC',
    'WY',
    '10180005',
    'L Medicine Bow R ab Sand Cr~ nr Shirley Basin~ WY',
    42.33111,
    -106.15528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LMDW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOSH1',
    '16227500',
    'AKRFC',
    'HI',
    NULL,
    'MOANALUA STREAM NR KANEOHE OAHU HI',
    21.38806,
    -157.84861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SFBV2',
    '0162588350',
    'MARFC',
    'VA',
    NULL,
    'SOUTH FORK BACK CREEK AT RT 814 NEAR SHERANDO VA',
    37.921667,
    -78.987222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFBV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18272094,
    'MUCW4',
    '09258050',
    'CBRFC',
    'WY',
    '14050004',
    'Muddy Creek above Olson Draw~ near Dad~ WY',
    41.47833,
    -107.6025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MUCW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7869281,
    'NMOT2',
    '08189998',
    'WGRFC',
    'TX',
    '12110101',
    'Nueces Rv at CR 414 at Montell~ TX',
    29.52647,
    -100.01837
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NMOT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6412650,
    'WCEG1',
    '02344724',
    'SERFC',
    'GA',
    '3130005',
    'WHITEWATER CREEK AT EASTIN RD~ NR FAYETTEVILLE~ GA',
    33.48722,
    -84.50833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCEG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 318261,
    'LSPT2',
    '08456310',
    'WGRFC',
    'TX',
    '13080001',
    'Las Moras Spgs Dws of pool at Brackettville~TX',
    29.30792,
    -100.41922
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17843682,
    'RFRN5',
    '08282300',
    'WGRFC',
    'NM',
    '13020102',
    'RIO BRAZOS AT FISHTAIL ROAD NR TIERRA AMARILLA~ NM',
    36.73793,
    -106.47077
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RFRN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6415536,
    'SMFG1',
    '02344748',
    'SERFC',
    'GA',
    '3130005',
    'WHITEWATER CR (DS STARRS MILL DAM) FAYETTEVILLE~GA',
    33.32833,
    -84.50917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SMFG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2809679,
    'ACBC1',
    '11172955',
    'CNRFC',
    'CA',
    '18050004',
    'ALAMEDA C BL DIV DAM NR SUNOL CA',
    37.49861,
    -121.77944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACBC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2805115,
    'ALFC1',
    '11179100',
    'CNRFC',
    'CA',
    '18050004',
    'ALAMEDA C NR FREMONT CA',
    37.56667,
    -122.00056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALFC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14917771,
    'MBPC1',
    '11206820',
    'CNRFC',
    'CA',
    '18030007',
    'MARBLE FORK KAWEAH R AB HORSE C NR LODGEPOLE CA',
    36.61167,
    -118.70167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MBPC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20247214,
    'BHSC1',
    '10251290',
    'CNRFC',
    'CA',
    '18090202',
    'BOREHOLE SPG CHANNEL NR TECOPA HOT SPGS~ CA',
    35.88569,
    -116.23425
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BHSC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20247362,
    'WLWC1',
    '10251335',
    'CNRFC',
    'CA',
    '18090202',
    'WILLOW CK AT CHINA RANCH~ CA',
    35.80094,
    -116.19442
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLWC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23605984,
    'RSAI1',
    '13342295',
    'NWRFC',
    'ID',
    '17060306',
    'WEBB CREEK NEAR SWEETWATER ID',
    46.32667,
    -116.83222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSAI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23232999,
    'LLDI1',
    '13118975',
    'NWRFC',
    'ID',
    '17040217',
    'LITTLE LOST RIVER AB FLOOD DIVERSION NR HOWE ID',
    43.88483,
    -113.09701
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLDI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2086989,
    'THCK2',
    '03254693',
    'OHRFC',
    'KY',
    '5100101',
    'THREEMILE CREEK AT THREE MILE ROAD AT COVINGTON~KY',
    39.05014,
    -84.48511
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'THCK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2889178,
    'FELC2',
    '06730160',
    'MBRFC',
    'CO',
    '10190005',
    'FOURMILE CANYON CREEK NEAR SUNSHINE~ CO',
    40.05761,
    -105.34878
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FELC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 189361,
    'LDEC2',
    '06711555',
    'MBRFC',
    'CO',
    '10190002',
    'LITTLE DRY CREEK ABOVE ENGLEWOOD~ CO',
    39.64917,
    -104.97833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LDEC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1239255,
    'TBEC2',
    '09027100',
    'CBRFC',
    'CO',
    '14010001',
    'FRASER RIVER AT TABERNASH~ CO',
    39.99033,
    -105.82978
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBEC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CHFA2',
    '15514000',
    'AKRFC',
    'AK',
    NULL,
    'CHENA R AT FAIRBANKS AK',
    64.845833,
    -147.701111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHFA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21654200,
    'CWCA1',
    '02423160',
    'SERFC',
    'AL',
    '3150202',
    'CAHABA RIVER NEAR WHITES CHAPEL AL',
    33.60361,
    -86.54917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWCA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRHA2',
    '15493400',
    'AKRFC',
    'AK',
    NULL,
    'CHENA R BL HUNTS C NR TWO RIVERS AK',
    64.86,
    -146.803333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRHA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRUA2',
    '15875000',
    'AKRFC',
    'AK',
    NULL,
    'COLVILLE R AT UMIAT AK',
    69.360556,
    -152.121667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRUA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DCKA2',
    '15743850',
    'AKRFC',
    'AK',
    NULL,
    'DAHL C NR KOBUK AK',
    66.946111,
    -156.908889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DCKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ENNA2',
    '15515500',
    'AKRFC',
    'AK',
    NULL,
    'TANANA R AT NENANA AK',
    64.565278,
    -149.091667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ENNA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCCA2',
    '15502000',
    'AKRFC',
    'AK',
    NULL,
    'FISH C BL SOLO C NR CHATANIKA AK',
    65.009722,
    -147.198333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FMRA2',
    '15348000',
    'AKRFC',
    'AK',
    NULL,
    'FORTYMILE R NR STEELE CREEK AK',
    64.309167,
    -141.402222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FMRA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GBDA2',
    '15477740',
    'AKRFC',
    'AK',
    NULL,
    'GOODPASTER R NR BIG DELTA AK',
    64.450556,
    -144.942222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GLSA2',
    '15457790',
    'AKRFC',
    'AK',
    NULL,
    'GOLDSTREAM C BL ALABAM C NR LIVENGOOD AK',
    65.575833,
    -148.388611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HULA2',
    '15980000',
    'AKRFC',
    'AK',
    NULL,
    'HULAHULA R NR KAKTOVIK AK',
    69.711389,
    -144.19
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HULA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KIAA2',
    '15744500',
    'AKRFC',
    'AK',
    NULL,
    'KOBUK R NR KIANA AK',
    66.973611,
    -160.130833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KIAA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KUPA2',
    '15896000',
    'AKRFC',
    'AK',
    NULL,
    'KUPARUK R NR DEADHORSE AK',
    70.283056,
    -148.960833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KUPA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCDA2',
    '15493700',
    'AKRFC',
    'AK',
    NULL,
    'CHENA R BL MOOSE C DAM AK',
    64.800833,
    -147.227778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MDEA2',
    '15803000',
    'AKRFC',
    'AK',
    NULL,
    'MEADE R AT ATKASUK AK',
    70.495833,
    -157.3925
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MDEA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PKSH1',
    '16244000',
    'AKRFC',
    'HI',
    NULL,
    'PUKELE STREAM NEAR HONOLULU OAHU HI',
    21.306667,
    -157.788333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PKSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMDA2',
    '644255147180200',
    'AKRFC',
    'AK',
    NULL,
    'TANANA R UNNAMD SL AT MOOSE C DAM NR NORTH POLE AK',
    64.71528,
    -147.30056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMDA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DANA2',
    '15129300',
    'AKRFC',
    'AK',
    NULL,
    'DANGEROUS R AT HARLEQUIN LK OUTLET NR YAKUTAT AK',
    59.4175,
    -139.0175
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DANA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TAFA2',
    '15485500',
    'AKRFC',
    'AK',
    NULL,
    'TANANA R AT FAIRBANKS AK',
    64.792778,
    -147.838889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TAFA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRBA2',
    '15519100',
    'AKRFC',
    'AK',
    NULL,
    'TOLOVANA R BL ROSEBUD C NR LIVENGOOD AK',
    65.465278,
    -148.628611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRBA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UCHA2',
    '15493000',
    'AKRFC',
    'AK',
    NULL,
    'CHENA R NR TWO RIVERS AK',
    64.901972,
    -146.361333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UCHA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WULA2',
    '15747000',
    'AKRFC',
    'AK',
    NULL,
    'WULIK R BL TUTAK C NR KIVALINA AK',
    67.876111,
    -163.674444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WULA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YEAA2',
    '15356000',
    'AKRFC',
    'AK',
    NULL,
    'YUKON R AT EAGLE AK',
    64.789444,
    -141.328333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YEAA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'YRBA2',
    '15453500',
    'AKRFC',
    'AK',
    NULL,
    'YUKON R NR STEVENS VILLAGE AK',
    65.875556,
    -149.717778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'YRBA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MKEA2',
    '15310002',
    'AKRFC',
    'AK',
    NULL,
    'MOSQUITO FORK 2 MI BL KECHUMSTUK C NR CHICKEN AK',
    64.016111,
    -142.544722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MKEA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLUH1',
    '16704000',
    'AKRFC',
    'HI',
    NULL,
    'WAILUKU RIVER AT PIIHONUA HI',
    19.712139,
    -155.15075
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLUH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4893971,
    'NW656',
    '385202111121601',
    'CBRFC',
    'UT',
    '14070002',
    'MUDDY CREEK BL MILLER CANYON NR EMERY~ UTAH',
    38.88111,
    -111.20333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW656'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CTRI4',
    '05457440',
    'NCRFC',
    'IA',
    NULL,
    'DEER CREEK NEAR CARPENTER IA',
    43.415,
    -92.98472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CTRI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6868045,
    'NW714',
    '040734605',
    'NCRFC',
    'WI',
    '4030201',
    'GREEN LAKE SW INLET @ CT HIGHWY K NR GREEN LAKE~WI',
    43.77944,
    -89.05083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW714'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12175440,
    'NW805',
    '040854592',
    'NCRFC',
    'WI',
    '4030101',
    'FISHER CREEK AT HOWARDS GROVE~ WI',
    43.83027,
    -87.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW805'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1889852,
    'NW613',
    '11336790',
    'CNRFC',
    'CA',
    '18040012',
    'LITTLE POTATO SLOUGH A TERMINOUS CA',
    38.08472,
    -121.49138
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW613'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCKA2',
    '15081497',
    'AKRFC',
    'AK',
    NULL,
    'STANEY C NR KLAWOCK AK',
    55.801389,
    -133.108611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCSA2',
    '15088000',
    'AKRFC',
    'AK',
    NULL,
    'SAWMILL C NR SITKA AK',
    57.05139,
    -135.22778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCSA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SLMA2',
    '15008000',
    'AKRFC',
    'AK',
    NULL,
    'SALMON R NR HYDER AK',
    56.026111,
    -130.065278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SLMA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRYA2',
    '15129500',
    'AKRFC',
    'AK',
    NULL,
    'SITUK R NR YAKUTAT AK',
    59.586667,
    -139.4925
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRYA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'STKA2',
    '15024800',
    'AKRFC',
    'AK',
    NULL,
    'STIKINE R NR WRANGELL AK',
    56.708056,
    -132.130278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STKA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TKUA2',
    '15041200',
    'AKRFC',
    'AK',
    NULL,
    'TAKU R NR JUNEAU AK',
    58.538611,
    -133.700556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TKUA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TYAA2',
    '15056210',
    'AKRFC',
    'AK',
    NULL,
    'TAIYA R NR SKAGWAY AK',
    59.511944,
    -135.344444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TYAA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCRI3',
    '05523865',
    'NCRFC',
    'IN',
    NULL,
    'CARPENTER CREEK AT REMINGTON IN',
    40.778083,
    -87.163361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCRI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TYWA2',
    '15019990',
    'AKRFC',
    'AK',
    NULL,
    'TYEE LK OUTLET NR WRANGELL AK',
    56.2,
    -131.506667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TYWA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14762111,
    'BMCI2',
    '05551330',
    'NCRFC',
    'IL',
    '7120007',
    'MILL CREEK NEAR BATAVIA  IL',
    41.84583,
    -88.34917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BMCI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WFWI1',
    '13058529',
    'NWRFC',
    'ID',
    NULL,
    'WILLOW CR FLOODWAY CHANNEL AT MOUTH NR ID FALLS ID',
    43.57472,
    -112.04806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WFWI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11689198,
    '1589312',
    '01589312',
    'MARFC',
    'MD',
    '2060003',
    'DEAD RUN NEAR CATONSVILLE  MD',
    39.29588889,
    -76.7440833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = '1589312'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'URWA2',
    '15015595',
    'AKRFC',
    'AK',
    NULL,
    'UNUK R BL BLUE R NR WRANGELL AK',
    56.240556,
    -130.880278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'URWA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 189855,
    'HAJC2',
    '06711570',
    'MBRFC',
    'CO',
    '10190002',
    'HARVARD GULCH AT COLORADO BLVD. AT DENVER  CO',
    39.6692,
    -104.94251
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HAJC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WTKA1',
    '02411590',
    'SERFC',
    'AL',
    NULL,
    'COOSA RIVER AT HWY 14 NEAR WETUMPKA AL',
    32.55055,
    -86.19579
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WTKA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18239649,
    'CLPA1',
    '02428400',
    'SERFC',
    'AL',
    NULL,
    'ALABAMA RIVER AT CLAIBORNE L&D NEAR MONROEVILLE',
    31.615,
    -87.55056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLPA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DIXA1',
    '02374050',
    'SERFC',
    'AL',
    NULL,
    'FOLLEY CREEK NEAR DIXIE AL',
    31.1278,
    -86.7965
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DIXA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DOLA1',
    '02378780',
    'SERFC',
    'AL',
    NULL,
    'D''OLIVE CREEK NEAR BRIDGEHEAD AL',
    30.6519,
    -87.8894
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DOLA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TAWA1',
    '02378790',
    'SERFC',
    'AL',
    NULL,
    'TIAWASEE CREEK NEAR BRIDGEHEAD AL',
    30.6427,
    -87.8916
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TAWA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HGLI3',
    '05536165',
    'NCRFC',
    'IN',
    NULL,
    'LITTLE CALUMET RIVER AT HIGHLAND IN',
    41.5696,
    -87.4609
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HGLI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'QJOP4',
    '50049310',
    NULL,
    'PR',
    NULL,
    'QUEBRADA JOSEFINA AT PINERO AVENUE PR',
    18.40917,
    -66.07556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QJOP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ACGA4',
    '07078296',
    'LMRFC',
    'AR',
    NULL,
    'AR POST CANAL AT LOCK NO. 2 NEAR TICHNOR AR',
    34.02639,
    -91.24667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACGA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BSWA4',
    '07054527',
    'LMRFC',
    'AR',
    NULL,
    'WHITE RIVER BELOW BULL SHOALS DAM NEAR FAIRVIEW',
    36.34361,
    -92.57417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSWA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 686141,
    'CYRO2',
    NULL,
    'ABRFC',
    'OK',
    '11130302',
    'Little Washita River ab SCS Pond No 26 nr Cyril OK',
    34.9147879,
    -98.2508838
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CYRO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DBSA4',
    '07364130',
    'LMRFC',
    'AR',
    NULL,
    'DEEP BAYOU NEAR STAR CITY AR',
    33.96313,
    -91.69245
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DBSA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRDA4',
    '07257995',
    'ABRFC',
    'AR',
    NULL,
    'LAKE DARDANELLE AT DARDANELLE AR',
    35.24722,
    -93.17306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRDA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GAPA4',
    '07355870',
    'LMRFC',
    'AR',
    NULL,
    'GAP CR NR WOLF PEN GAP REC AREA NR NUNLEY AR',
    34.48411,
    -94.12614
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GAPA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCRA4',
    '07358570',
    'LMRFC',
    'AR',
    NULL,
    'GULPHA CR AT RIDGEWAY ST AT HOT SPRINGS AR',
    34.494,
    -93.0065
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCRA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12162694,
    'AUKW3',
    '04087170',
    'NCRFC',
    'WI',
    '4040003',
    'MILWAUKEE RIVER AT MOUTH AT MILWAUKEE  WI',
    43.02444444,
    -87.8983333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AUKW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NUNA4',
    '323026092403201',
    'LMRFC',
    'AR',
    NULL,
    'RUSTON OFFICE LOWER MISSISSIPPI-GULF WSC',
    32.50722,
    -92.67556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NUNA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1919306,
    'GRTO1',
    '03238500',
    'OHRFC',
    'OH',
    '5090201',
    'White Oak Creek near Georgetown OH',
    38.85812494,
    -83.9285394
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRTO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6075395,
    'PEPM3',
    '01096503',
    'NERFC',
    'MA',
    '1070004',
    'NISSITISSIT RIVER AT PEPPERELL  MA',
    42.6720338,
    -71.5770126
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PEPM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12533094,
    'LPCM8',
    '06154410',
    'MBRFC',
    'MT',
    '10050009',
    'Little Peoples Creek near Hays MT',
    47.96575556,
    -108.6606944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPCM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12145180,
    'QUAM4',
    '04117000',
    'NCRFC',
    'MI',
    '4050007',
    'QUAKER BROOK NEAR NASHVILLE  MI',
    42.5658695,
    -85.0936085
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'QUAM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9248978,
    'MITN7',
    '02112360',
    'SERFC',
    'NC',
    '3040101',
    'MITCHELL RIVER NEAR STATE ROAD  NC',
    36.31138889,
    -80.8072222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MITN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9251654,
    'RORN7',
    '02112120',
    'SERFC',
    'NC',
    '3040101',
    'ROARING RIVER NEAR ROARING RIVER  NC',
    36.25027778,
    -81.0444444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RORN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2589541,
    'STCN4',
    '01461300',
    'MARFC',
    'NJ',
    '2040105',
    'Wickecheoke Creek at Stockton NJ',
    40.41138889,
    -74.9866667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STCN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4151294,
    'BIGN4',
    '01439800',
    'MARFC',
    'NJ',
    '2040104',
    'Big Flat Brook near Hainesville NJ',
    41.20694444,
    -74.8036111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BIGN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4709768,
    'INRP1',
    '01572950',
    'MARFC',
    'PA',
    '2050305',
    'Indiantown Run near Harper Tavern  PA',
    40.43897778,
    -76.59829919
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INRP1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4529217,
    'INDV2',
    '01659000',
    'MARFC',
    'VA',
    '2070011',
    'N BRANCH CHOPAWAMSIC CR NR INDEPENDENT HILL  VA',
    38.56512118,
    -77.4258175
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INDV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCRA4',
    NULL,
    'LMRFC',
    'AR',
    NULL,
    'LWR VALLIER(LGPONDSLOUGH)HW/TW NR NEW GASCONY AR',
    34.24628,
    -91.66931
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCRA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WPGA4',
    '07355860',
    'LMRFC',
    'AR',
    NULL,
    'BOARD CAMP CR NR WOLF PEN GAP REC NR NUNLEY AR',
    34.48597,
    -94.11786
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPGA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5336451,
    'CYKW4',
    '06301480',
    'MBRFC',
    'WY',
    '10090101',
    'CONEY CREEK ABOVE TWIN LAKES  NEAR BIG HORN  WY',
    44.6013551,
    -107.3175756
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CYKW4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5711951,
    'GRPT2',
    '08134230',
    'WGRFC',
    'TX',
    '12090104',
    'Grape Ck nr Grape Creek  TX',
    31.57515616,
    -100.5856544
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3272710,
    'NCMU1',
    '09372400',
    'CBRFC',
    'UT',
    '14080203',
    'NORTH CREEK NEAR MONTICELLO  UT',
    37.87666,
    -109.4459538
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NCMU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5711909,
    'WVAT2',
    '08133900',
    'WGRFC',
    'TX',
    '12090104',
    'Chalk Ck nr Water Valley  TX',
    31.64654097,
    -100.6906581
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WVAT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4529237,
    'GARV2',
    '01659500',
    'MARFC',
    'VA',
    '2070011',
    'M BRANCH CHOPAWAMSIC CREEK NR GARRISONVILLE  VA',
    38.55734365,
    -77.4252618
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GARV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BSWA3',
    '09424425',
    'CBRFC',
    'AZ',
    NULL,
    'BIG SANDY RIVER AT US 93 BRIDGE NEAR WIKIEUP AZ',
    34.6625,
    -113.5808
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSWA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6078383,
    'NW685',
    '01095434',
    'NERFC',
    'MA',
    '1070004',
    'GATES BROOK NEAR WEST BOYLSTON~ MA',
    42.35583,
    -71.77944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW685'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20427202,
    'GGDA3',
    '09519501',
    'CBRFC',
    'AZ',
    NULL,
    'GILA R BLW GILLESPIE DAM AZ (LOW-FLOW STATION)',
    33.22917,
    -112.76667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GGDA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CYDA3',
    '09522860',
    'CBRFC',
    'AZ',
    NULL,
    'CITY OF YUMA DIVERSION AT AVENUE 9E PUMPING PLANT',
    32.67889,
    -114.47861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CYDA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCGA3',
    '09475500',
    'CBRFC',
    'AZ',
    NULL,
    'FLORENCE-CASA GRANDE CANAL NEAR FLORENCE AZ.',
    33.0875,
    -111.28611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCGA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FISA3',
    '09501150',
    'CBRFC',
    'AZ',
    NULL,
    'FISH CREEK NEAR TORTILLA FLAT AZ',
    33.53278,
    -111.305
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FISA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ECSA3',
    '09446320',
    'CBRFC',
    'AZ',
    NULL,
    'EAGLE CREEK AT EAGLE SCHOOL RD NEAR MORENCI AZ',
    33.35906,
    -109.49153
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ECSA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NTCA3',
    '09489082',
    'CBRFC',
    'AZ',
    NULL,
    'NORTH FORK THOMAS CREEK NEAR ALPINE AZ',
    33.67503,
    -109.27011
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NTCA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPSA3',
    '09471580',
    'CBRFC',
    'AZ',
    NULL,
    'SAN PEDRO RIVER AT ST DAVID AZ',
    31.90531,
    -110.24606
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPSA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18508400,
    'NW909',
    '03322985',
    'OHRFC',
    'IN',
    '5120101',
    'WABASH RIVER NEAR BLUFFTON~ IN',
    40.72861,
    -85.13555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW909'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8492938,
    'PCKV2',
    '01674182',
    'MARFC',
    'VA',
    '2080105',
    'POLECAT CREEK AT ROUTE 301 NEAR PENOLA~ VA',
    37.96033,
    -77.34344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCKV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CSCN2',
    '10311750',
    'CNRFC',
    'NV',
    NULL,
    'CARSON R ABV SIXMILE CYN CK BLW DAYTON NV',
    39.28092,
    -119.52514
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DVWN2',
    '10348550',
    'CNRFC',
    'NV',
    NULL,
    'DAVIS CK AT DAVIS CK CMPGRD RD NR WASHOE CITY NV',
    39.30342,
    -119.82817
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DVWN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCAN2',
    '10351900',
    'CNRFC',
    'NV',
    NULL,
    'HARDSCRABBLE CK ABV TH RANCH AT SUTCLIFFE NV',
    39.93873,
    -119.6481
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCAN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCBN2',
    '10351950',
    'CNRFC',
    'NV',
    NULL,
    'HARDSCRABBLE CK BLW TH RANCH AT SUTCLIFFE NV',
    39.94369,
    -119.61803
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCBN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MSQA3',
    '09415060',
    'CBRFC',
    'AZ',
    NULL,
    'MESQUITE CANAL NR MESQUITE NV',
    36.80989,
    -114.01475
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MSQA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17595453,
    'MSEC1',
    '11119750',
    'CNRFC',
    'CA',
    NULL,
    'MISSION C NR MISSION ST NR SANTA BARBARA CA',
    34.4275,
    -119.72528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MSEC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WCBC1',
    '10336674',
    'CNRFC',
    'CA',
    NULL,
    'WARD C BL CONFLUENCE NR TAHOE CITY CA',
    39.14083,
    -120.21111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCBC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21439507,
    'LKHC1',
    '09427500',
    'CBRFC',
    'CA',
    NULL,
    'LAKE HAVASU NEAR PARKER DAM AZ-CA',
    34.31611,
    -114.15639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKHC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21412853,
    'CIDC1',
    '09429500',
    'CBRFC',
    'CA',
    NULL,
    'COLORADO RIVER BELOW IMPERIAL DAM AZ-CA',
    32.86758,
    -114.47211
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CIDC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22595619,
    'BPCC1',
    '10255810',
    'CNRFC',
    'CA',
    NULL,
    'BORREGO PALM C NR BORREGO SPRINGS CA',
    33.27917,
    -116.43056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BPCC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EASC1',
    '11465690',
    'CNRFC',
    'CA',
    NULL,
    'COLGAN C NR SANTA ROSA CA',
    38.402222,
    -122.731944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EASC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ICSC1',
    '11173800',
    'CNRFC',
    'CA',
    NULL,
    'INDIAN C NR SUNOL CA',
    37.561111,
    -121.796944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ICSC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ZNRC1',
    '10255550',
    'CNRFC',
    'CA',
    NULL,
    'NEW R NR WESTMORLAND CA',
    33.10472,
    -115.66361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ZNRC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DMBC1',
    '373025122065901',
    'CNRFC',
    'CA',
    NULL,
    'SAN FRANCISCO BAY A OLD DUMBARTON BR NR NEWARK CA',
    37.50694,
    -122.11639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DMBC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OVCC1',
    '10337810',
    'CNRFC',
    'CA',
    NULL,
    'NF SQUAW C A OLYMPIC VALLEY CA',
    39.19878,
    -120.24111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OVCC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NFAC1',
    '11433790',
    'CNRFC',
    'CA',
    NULL,
    'NF AMERICAN R A AUBURN DAM SITE NR AUBURN CA',
    38.88306,
    -121.06194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFAC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22549123,
    'SXMC1',
    '11046050',
    'CNRFC',
    'CA',
    NULL,
    'SANTA MARGARITA R A MOUTH NR OCEANSIDE CA',
    33.23781,
    -117.39531
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SXMC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MECC1',
    '11289000',
    'CNRFC',
    'CA',
    NULL,
    'MODESTO CN NR LA GRANGE CA',
    37.6725,
    -120.47389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MECC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22658651,
    'GVVC1',
    '10260855',
    'CNRFC',
    'CA',
    NULL,
    'GRASS VALLEY LK TUNNEL OUTLET A LAKE ARROWHEAD CA',
    34.26,
    -117.20444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GVVC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OWSC1',
    '11452800',
    'CNRFC',
    'CA',
    NULL,
    'CACHE C OVERFLOW WEIR FROM SETTLING BAS NR WOOD''LD',
    38.68278,
    -121.67194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OWSC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OTFC1',
    '11452900',
    'CNRFC',
    'CA',
    NULL,
    'CACHE C OUTFLOW FROM SETTLING BASIN NR WOODLAND CA',
    38.67861,
    -121.67167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OTFC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AFDC1',
    '11446220',
    'CNRFC',
    'CA',
    NULL,
    'AMERICAN R BL FOLSOM DAM NR FOLSOM CA',
    38.70453,
    -121.16436
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AFDC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AWBC1',
    '11446980',
    'CNRFC',
    'CA',
    NULL,
    'AMERICAN R BL WATT AVE BRDG NR CARMICHAEL CA',
    38.56722,
    -121.38722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AWBC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AWPC1',
    '11446700',
    'CNRFC',
    'CA',
    NULL,
    'AMERICAN R A WILLIAM B POND PARK A CARMICHAEL CA',
    38.59139,
    -121.33167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AWPC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 188073,
    'LGLC2',
    '06711780',
    'MBRFC',
    'CO',
    '10190002',
    'LAKEWOOD GULCH AT DENVER~ CO',
    39.73519,
    -105.03136
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LGLC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRYN2',
    '10301115',
    'CNRFC',
    'NV',
    NULL,
    'WALKER RVR BLW YERINGTON WEIR NR YERINGTON NV',
    39.0203,
    -119.1561
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRYN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HTDC2',
    '09041900',
    'CBRFC',
    'CO',
    NULL,
    'MONTE CRISTO DIVERSION NEAR HOOSIER PASS CO',
    39.38083,
    -106.07083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HTDC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LENC2',
    '06719560',
    'MBRFC',
    'CO',
    NULL,
    'LENA GULCH AT LAKEWOOD CO',
    39.7404,
    -105.14883
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LENC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MDWC2',
    '09032990',
    'CBRFC',
    'CO',
    NULL,
    'MEADOW CREEK BLW MEADOW CREEK RES NR TABERNASH CO',
    40.05203,
    -105.75417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MDWC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SBUC2',
    '06714360',
    'MBRFC',
    'CO',
    NULL,
    'SAND CRK ABV BURLINGTON DITCH NR COMMERCE CITY CO',
    39.81025,
    -104.95033
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBUC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLCC2',
    '09124010',
    'CBRFC',
    'CO',
    NULL,
    'HENSON CREEK AT LAKE CITY CO',
    38.02567,
    -107.31635
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLCC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RPLC2',
    '384047104510301',
    'ABRFC',
    'CO',
    NULL,
    'RIPLEY DITCH FROM L. FOUNTAIN CR AT FT. CARSON CO',
    38.67972,
    -104.85083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RPLC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'STCC2',
    '383637104531301',
    'ABRFC',
    'CO',
    NULL,
    'STROBEL DITCH FROM TURKEY CR AT FT. CARSON CO',
    38.61028,
    -104.88694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STCC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PRIL1',
    '07377210',
    'LMRFC',
    'LA',
    NULL,
    'SANDY CREEK NR PRIDE LA',
    30.670556,
    -90.96
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PRIL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPAC2',
    '09352800',
    'CBRFC',
    'CO',
    NULL,
    'LOS PINOS R ABV VALLECITO RES NR BAYFIELD CO',
    37.41472,
    -107.51278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPAC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WINC3',
    '01186090',
    'NERFC',
    'CT',
    NULL,
    'MAD RIVER DETENTION RESERVOIR NR WINSTED CT',
    41.93139,
    -73.0925
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WINC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRLC3',
    '01185850',
    'NERFC',
    'CT',
    NULL,
    'COLEBROOK RIVER LAKE NR COLEBROOK CT',
    42.00611,
    -73.03667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRLC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HMDC3',
    '01205560',
    'NERFC',
    'CT',
    NULL,
    'HALL MEADOW BRK RESERVOIR NR TORRINGTON CT',
    41.86889,
    -73.1675
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HMDC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NBLC3',
    '01206940',
    'NERFC',
    'CT',
    NULL,
    'NORTHFIELD BROOK LAKE AT THOMASTON CT',
    41.68158,
    -73.09111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NBLC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMSC3',
    '01206600',
    'NERFC',
    'CT',
    NULL,
    'THOMASTON RESERVOIR NR THOMASTON',
    41.695,
    -73.06222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMSC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TORC3',
    '01205650',
    'NERFC',
    'CT',
    NULL,
    'EAST BRANCH DETENTION RES. NR TORRINGTON CT',
    41.83639,
    -73.12056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TORC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRBC3',
    '01208011',
    'NERFC',
    'CT',
    NULL,
    'BLACK ROCK LAKE NR THOMASTON CT',
    41.65667,
    -73.10639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRBC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ANDF1',
    '261023080443001',
    'SERFC',
    'FL',
    NULL,
    'SITE 62 IN CONSERVATION AREA 3A NR ANDYTOWN FL',
    26.17422,
    -80.75075
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ANDF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BRJF1',
    '02246751',
    'SERFC',
    'FL',
    NULL,
    'BROWARD RIVER BL BISCAYNE BLVD NR JACKSONVILLE FL',
    30.443333,
    -81.668333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRJF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BUCF1',
    '301124081395901',
    'SERFC',
    'FL',
    NULL,
    'ST.JOHNS RIVER BUCKMAN BRIDGE AT JACKSONVILLEFL',
    30.19,
    -81.666389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BUCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCJF1',
    '02246825',
    'SERFC',
    'FL',
    NULL,
    'CLAPBOARD CREEK NR JACKSONVILLE FL',
    30.448333,
    -81.518333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCJF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DPBF1',
    '302309081333001',
    'SERFC',
    'FL',
    NULL,
    'ST JOHNS R DAMES POINT BRIDGE AT JACKSONVILLE FL',
    30.385833,
    -81.558333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DPBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BDEF1',
    '260536080302501',
    'SERFC',
    'FL',
    NULL,
    'EDEN 4 IN WATER CONSERVATION AREA 3-A',
    26.09333,
    -80.50694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BDEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JULF1',
    '02246160',
    'SERFC',
    'FL',
    NULL,
    'JULINGTON CRK AT OLD ST AUGUST RD NR BAYARD FL',
    30.143333,
    -81.555833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JULF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PCUF1',
    '02246518',
    'SERFC',
    'FL',
    NULL,
    'POTTSBURG CRK AT US90 NR S. JACKSONVILLE FL',
    30.286944,
    -81.57
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCUF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDEF1',
    '295500081180000',
    'SERFC',
    'FL',
    NULL,
    'TOLOMATO RIVER NEAR ST AUGUSTINE FL',
    29.91667,
    -81.3
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ADYF1',
    '261117080315201',
    'SERFC',
    'FL',
    NULL,
    'SITE 63 IN CONSERVATION AREA NO. 3A NR ANDYTOWN FL',
    26.18847,
    -80.531
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ADYF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPEM1',
    '432742070225401',
    'NERFC',
    'ME',
    NULL,
    'SACO RIVER AT CAMP ELLIS NEAR SACO MAINE',
    43.461628,
    -70.3816
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPEM1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BOYF1',
    '263180080205001',
    'SERFC',
    'FL',
    NULL,
    'SITE 7 IN CONS AREA NO. 1 NR SHAWANO FL',
    26.52255,
    -80.3361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOYF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCNF1',
    '02286328',
    'SERFC',
    'FL',
    NULL,
    'C-8 CANAL AT NE 135 ST AT NORTH MIAMI FL',
    25.90022,
    -80.19497
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCNF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8590446,
    'NW736',
    '07048480',
    'LMRFC',
    'AR',
    '11010001',
    'College Branch at MLK Blvd at Fayetteville~ AR',
    36.05083,
    -94.16944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW736'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12227771,
    'GLCM4',
    '04126802',
    'NCRFC',
    'MI',
    NULL,
    'CRYSTAL RIVER AT CO HWY 675 NR GLEN ARBOR~ MI',
    44.9033,
    -85.9622
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GLCM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WPTM6',
    '02480345',
    'LMRFC',
    'MS',
    NULL,
    'TCHOUTACABOUFFA RIVER TRIB NEAR WHITE PLAINS MS',
    30.590833,
    -88.906667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPTM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRHF1',
    '255300080370001',
    'SERFC',
    'FL',
    NULL,
    'SITE 69 IN CONSERVATION AREA 3BNR COOPERTOWN FL',
    25.90661,
    -80.58876
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRHF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRLF1',
    '255828080401301',
    'SERFC',
    'FL',
    NULL,
    'SITE 64 IN CONSERVATION AREA 3A NR COOPERTOWN FL.',
    25.97514,
    -80.66961
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CSPF1',
    '262240080258001',
    'SERFC',
    'FL',
    NULL,
    'SITE 17 NR L-38 CONS AREA 2A NR CORAL SPRINGS FL',
    26.28639,
    -80.41111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CSSF1',
    '261710080190001',
    'SERFC',
    'FL',
    NULL,
    'SITE 19 IN CONSERVATION AREA 2A NR CORAL SPRINGS',
    26.28147,
    -80.30664
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CSSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CUOF1',
    '251154080471900',
    'SERFC',
    'FL',
    NULL,
    'CUTHBERT LAKE OUTLET NEAR FLAMINGO FL',
    25.19836,
    -80.78867
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CUOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DLBF1',
    '263050080145001',
    'SERFC',
    'FL',
    NULL,
    'SITE 8T IN CONS AREA NO.1 NR BOYNTON BCH FL',
    26.49967,
    -80.23443
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DLBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DLRF1',
    '263000080120001',
    'SERFC',
    'FL',
    NULL,
    'SITE 8C NR L-40 IN CONS AREA NO.1 NR BOYNTON BCH.',
    26.49928,
    -80.22211
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DLRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRBF1',
    '262750080175001',
    'SERFC',
    'FL',
    NULL,
    'SITE 9 IN CONSERVATION AREA NO.1 IN BOYNTON BCH FL',
    26.45961,
    -80.29092
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDAF1',
    '254707080370201',
    'SERFC',
    'FL',
    NULL,
    'EDEN 10 IN WATER CONSERVATION AREA 3-B',
    25.78528,
    -80.61722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EDBF1',
    '255708080295501',
    'SERFC',
    'FL',
    NULL,
    'EDEN 7 IN WATER CONSERVATION AREA 3-B',
    25.95222,
    -80.49861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EDBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HEEF1',
    '251946080254800',
    'SERFC',
    'FL',
    NULL,
    'EVERGLADES 1 IN C-111 BASIN NR HOMESTEAD FL',
    25.33033,
    -80.43517
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HEEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HEMF1',
    '261808081042800',
    'SERFC',
    'FL',
    NULL,
    'WEST FEEDER CANAL ABV WEST WEIR NR  CLEWISTON FL',
    26.30233,
    -81.07444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HEMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HETF1',
    '251716080342100',
    'SERFC',
    'FL',
    NULL,
    'EVERGLADES 5A IN C-111 BASIN NR HOMESTEAD FL',
    25.28611,
    -80.57255
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HETF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLAF1',
    '262100080190001',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO CA AT S-10-A NR DEERFIELD BCH. FL',
    26.36,
    -80.3125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLLF1',
    '262300080220001',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO CANAL AT S-10-D NR DEERFIELD BCH. FL',
    26.38722,
    -80.38056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HPBF1',
    '261541080050000',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO INLET AT POMPANO BEACH FL',
    26.26141,
    -80.08322
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HPBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HPCF1',
    '02257790',
    'SERFC',
    'FL',
    NULL,
    'HARNEY POND CANAL ABOVE S-71 NR LAKEPORT FL',
    27.03892,
    -81.0715
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HPCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCBF1',
    '02280500',
    'SERFC',
    'FL',
    NULL,
    'HILLSBORO CANAL BELOW S351 NR SOUTH BAY FLA',
    26.7,
    -80.7125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LASF1',
    '255154080371300',
    'SERFC',
    'FL',
    NULL,
    'L-67A CANAL AT S-152 NR COOPERTOWN FL',
    25.86515,
    -80.62057
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LASF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LDCF1',
    '02290709',
    'SERFC',
    'FL',
    NULL,
    '10B BLACK CR CANAL AT OLD CUTLER RD NR GOU',
    25.55986,
    -80.35956
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LDCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LOPF1',
    '255138081321701',
    'SERFC',
    'FL',
    NULL,
    'FAKA UNION BOUNDARY NEAR PANTHER KEY FL',
    25.86052,
    -81.53795
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LVCF1',
    '265501080364900',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 8 CANAL NEAR CANAL POINT FL',
    26.91694,
    -80.61361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LVCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LTWF1',
    '02290768',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 31 NORTH EXTENSION AT 7 MILE NR WEST MIAMI F',
    25.66339,
    -80.49817
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LTWF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LTMF1',
    '02290767',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 31 NORTH EXTENSION AT 5 MILE NR WEST MIAMI F',
    25.68583,
    -80.49722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LTMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LEMF1',
    '02290766',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 31 NORTH EXTENSION AT 4 MILE NR WEST MIAMI F',
    25.70217,
    -80.49597
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LEMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LNEF1',
    '022907647',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 31 NORTH EXTENSION 1 MILE NR WEST MIAMI FL',
    25.74633,
    -80.49764
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LWMF1',
    '02290765',
    'SERFC',
    'FL',
    NULL,
    'LEVEE 31 NORTH EXTENSION AT 3 MILE NR WEST MIAMI F',
    25.71758,
    -80.49656
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LWMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JOEF1',
    '251355080312800',
    'SERFC',
    'FL',
    NULL,
    'JOE BAY 2E NEAR KEY LARGO FL',
    25.23263,
    -80.52487
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JOEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EPAN1',
    '410536096194801',
    'MBRFC',
    'NE',
    NULL,
    'MID-EAST CHUTE OF PLATTE R NR CAMP ASHLAND NEBR.',
    41.09336,
    -96.33017
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EPAN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5673157,
    'LKPT2',
    '08105886',
    'WGRFC',
    'TX',
    '12070205',
    'Lake Ck at Lake Ck Pkwy nr Austin~ TX',
    30.46556,
    -97.78789
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OCFF1',
    '251033080440800',
    'SERFC',
    'FL',
    NULL,
    'OYSTER CREEK NEAR FLAMINGO FL',
    25.17589,
    -80.73558
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OCFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCON6',
    '01354330',
    'NERFC',
    'NY',
    NULL,
    'MOHAWK RIVER AT LOCK 8 NEAR SCHENECTADY NY',
    42.82816,
    -73.99037
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCON6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1238533,
    'SWFC2',
    '09035900',
    'CBRFC',
    'CO',
    '14010001',
    'SOUTH FORK OF WILLIAMS FORK NEAR LEAL  CO',
    39.79583,
    -106.03
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SWFC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1239423,
    'WFSC2',
    '09035500',
    'CBRFC',
    'CO',
    '14010001',
    'WILLIAMS FORK BELOW STEELMAN CREEK  CO.',
    39.77889,
    -105.92778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WFSC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24491716,
    'BXCI1',
    '13095500',
    'NWRFC',
    'ID',
    '17040212',
    'BOX CANYON SPRINGS NR WENDELL ID',
    42.7075,
    -114.81028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BXCI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3398496,
    'PBPI3',
    '04095090',
    'NCRFC',
    'IN',
    '4040001',
    'PORTAGE-BURNS WATERWAY AT PORTAGE  IN',
    41.62222,
    -87.17583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PBPI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24357005,
    'MCNM8',
    '12374250',
    'NWRFC',
    'MT',
    '17010212',
    'Mill Cr ab Bassoo Cr nr Niarada MT',
    47.82983,
    -114.69783
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCNM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10093088,
    'WRPU1',
    '10129300',
    'CBRFC',
    'UT',
    '16020101',
    'WEBER RIVER NEAR PEOA  UTAH',
    40.75111,
    -111.36972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRPU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRSF1',
    '254754080344300',
    'SERFC',
    'FL',
    NULL,
    'SHARK RIVER SLOUGH NO.1 IN CONS.3B NR COOPERTOWN',
    25.79756,
    -80.57886
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRVF1',
    '254315080331500',
    'SERFC',
    'FL',
    NULL,
    'NORTHEAST SHARK RVR SLOUGH NO2 NR COOPERTOWN FL',
    25.7197,
    -80.55736
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23801360,
    'ZOLO3',
    '14201300',
    'NWRFC',
    'OR',
    '17090009',
    'ZOLLNER CREEK NEAR MT ANGEL  OR',
    45.10056,
    -122.82056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ZOLO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10320230,
    'NW758',
    '02326885',
    'SERFC',
    'FL',
    '3120001',
    'ST. MARKS RIVER SWALLET NEAR WOODVILLE~FL',
    30.28805,
    -84.1525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW758'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCAF1',
    '254540080361500',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL AT S-355A NEAR MIAMI FL',
    25.76169,
    -80.59108
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10274468,
    'WEOU1',
    '10137000',
    'CBRFC',
    'UT',
    '16020102',
    'WEBER RIVER AT OGDEN  UTAH',
    41.22778,
    -111.9875
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEOU1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TAMF1',
    '254543080405401',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL AT S-12-D NEAR MIAMI FL',
    25.76194,
    -80.68167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TAMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SWEF1',
    '255250080335001',
    'SERFC',
    'FL',
    NULL,
    'SITE 71 IN CONSERVATION AREA 3B NR COOPERTOWN FL.',
    25.88444,
    -80.5565
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SWEF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16916356,
    'ROKF1',
    '02307000',
    'SERFC',
    'FL',
    '3100206',
    'ROCKY CREEK NEAR SULPHUR SPRINGS FL',
    28.03667,
    -82.57611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ROKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7952754,
    'HCUC1',
    '11355500',
    'CNRFC',
    'CA',
    '18020003',
    'HAT C NR HAT CREEK CA',
    40.68911,
    -121.42281
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCUC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCBF1',
    '254540080325700',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL AT S-355B NEAR MIAMI FL',
    25.76169,
    -80.55344
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCOF1',
    '02288800',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL OUTLETS MONROE TO CARNESTOWN FLA',
    25.88734,
    -81.26184
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCCF1',
    '02289500',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL NEAR CORAL GABLES FL',
    25.761944,
    -80.33
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WEBF1',
    '02289080',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL WEST END 1 MILE BRIDGE NR MIAMI FL',
    25.76139,
    -80.53667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLOF1',
    '251203080480600',
    'SERFC',
    'FL',
    NULL,
    'WEST LAKE OUTLET TO LONG LAKE NEAR FLAMINGO FL',
    25.20072,
    -80.80161
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'APFF1',
    '02237698',
    'SERFC',
    'FL',
    NULL,
    'APOPKA FLOW-WAY FEEDER CANAL NEAR ASTATULA FL',
    28.66611,
    -81.70611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'APFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BYLF1',
    '02266293',
    'SERFC',
    'FL',
    NULL,
    '10B LATERAL 405 AB S-405 NEAR VINELAND FLA',
    28.39444,
    -81.58464
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BYLF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRCF1',
    '251253080320100',
    'SERFC',
    'FL',
    NULL,
    'TROUT CREEK AT MOUTH NEAR KEY LARGO FL',
    25.21491,
    -80.5335
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMMF1',
    '254543080491101',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL AT S-12-A NR MIAMI FL',
    25.76194,
    -80.81972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMMF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMIF1',
    '02289019',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL AT S-12-B  NR MIAMI FL',
    25.76183,
    -80.76947
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMIF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WLCF1',
    '02287497',
    'SERFC',
    'FL',
    NULL,
    'N.W. WELLFIELD CANAL NR DADE BROWARD LEVEE NR PENN',
    25.89111,
    -80.42028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TMCF1',
    '02289041',
    'SERFC',
    'FL',
    NULL,
    'TAMIAMI CANAL BELOW S-12-C NEARMIAMI FLA',
    25.76111,
    -80.72611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TMCF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21477692,
    'BOTF1',
    '02264140',
    'SERFC',
    'FL',
    NULL,
    'BONNET CREEK NEAR KISSIMMEE FL',
    28.30778,
    -81.52472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOTF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ULRF1',
    '02289035',
    'SERFC',
    'FL',
    NULL,
    'THREE MILE CANAL BELOW G409 NEAR CLEWISTON FL',
    26.32719,
    -80.88161
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ULRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'UNRF1',
    '255026080231300',
    'SERFC',
    'FL',
    NULL,
    'SNAPPER CREEK CNL EXT AT NW74 ST NR HIALEAH FL',
    25.84056,
    -80.38694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'UNRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LXRF1',
    '265906080093500',
    'SERFC',
    'FL',
    NULL,
    'LOXAHATCHEE RIVER AT MILE 9.1 NEAR JUPITER FL',
    26.985,
    -80.159722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LXRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDFF1',
    '282435080375500',
    'SERFC',
    'FL',
    NULL,
    'CANAVERAL BARGE CANAL AT PORT CANAVERAL FL',
    28.4706,
    -80.6319
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDFF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MASF1',
    '02237000',
    'SERFC',
    'FL',
    NULL,
    'PALATLAKAHA RIVER NEAR MASCOTTE FL',
    28.61556,
    -81.86472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MASF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LXKF1',
    '265929080091800',
    'SERFC',
    'FL',
    NULL,
    'LOX RIVER AT OUTLET OF KITCHINGS CREEK FL',
    26.99139,
    -80.155
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LXKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FECF1',
    '02251767',
    'SERFC',
    'FL',
    NULL,
    'FELLSMERE CANAL NEAR MICCO FL',
    27.83028,
    -80.53444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FECF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCVF1',
    '02253000',
    'SERFC',
    'FL',
    NULL,
    'MAIN CANAL AT VERO BEACH FL',
    27.6475,
    -80.40583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21477658,
    'LLBF1',
    '02264060',
    'SERFC',
    'FL',
    NULL,
    'LATERAL 101 AT S-101 NEAR LAKE BUENA VISTA FL',
    28.37083,
    -81.52917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LNVF1',
    '02266295',
    'SERFC',
    'FL',
    NULL,
    'LATERAL 410 AT S-410 NEAR VINELAND FL',
    28.36611,
    -81.59861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNVF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16633716,
    'HANF1',
    '02238000',
    'SERFC',
    'FL',
    NULL,
    'HAYNES CREEK AT LISBON FL',
    28.87056,
    -81.78389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HANF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SGSF1',
    '02236160',
    'SERFC',
    'FL',
    NULL,
    'SILVER GLEN SPRINGS NEAR ASTOR FL',
    29.24444,
    -81.64278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SGSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDGF1',
    '291323081010500',
    'SERFC',
    'FL',
    NULL,
    'HALIFAX RV AT MAIN ST BRIDGE AT DAYTONA BEACH FL',
    29.223056,
    -81.01806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDGF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21478164,
    'RCSF1',
    '02266495',
    'SERFC',
    'FL',
    NULL,
    'REEDY CREEK AT S-40 NEAR LOUGHMAN FL',
    28.27556,
    -81.54417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RCSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BSSF1',
    '02291524',
    'SERFC',
    'FL',
    NULL,
    'SPRING CREEK HEADWATER NEAR BONITA SPRINGS FL',
    26.36233,
    -81.79036
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSSF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CBYF1',
    '02308861',
    'SERFC',
    'FL',
    NULL,
    'CROSS BAYOU CNL AT CEDAR BRK DR AT PINELLAS PK FL',
    27.87306,
    -82.72694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBYF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCZF1',
    '02273230',
    'SERFC',
    'FL',
    NULL,
    'C-41 CANAL NEAR BRIGHTON FL',
    27.21361,
    -81.20167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BVKF1',
    '02307200',
    'SERFC',
    'FL',
    NULL,
    'BROOKER CREEK AT VAN DYKE RD NEAR CITRUS PARK FL',
    28.12861,
    -82.57056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BVKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BPBF1',
    '02310368',
    'SERFC',
    'FL',
    NULL,
    'BEAR CREEK NEAR BAYONET POINT FL',
    28.33878,
    -82.69878
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BPBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSZF1',
    '275226081481900',
    'SERFC',
    'FL',
    NULL,
    'GATOR SINK NEAR BARTOW FL',
    27.87389,
    -81.80528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6802796,
    'NW711',
    '04072076',
    'NCRFC',
    'WI',
    '4030103',
    'SILVER CREEK AT FLORIST DRIVE AT ONEIDA~ WI',
    44.49138,
    -88.16944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW711'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GTRF1',
    '02293262',
    'SERFC',
    'FL',
    NULL,
    'GATOR SLOUGH WEST OF US-41 NEAR FT. MYERS FL',
    26.74203,
    -81.92094
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GTRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8915933,
    'NW742',
    '10289500',
    'CNRFC',
    'CA',
    '16050301',
    'GREEN CK NR BRIDGEPORT~ CA',
    38.16944,
    -119.22027
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW742'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HSRF1',
    '02310743',
    'SERFC',
    'FL',
    NULL,
    'HUNTER SPR RUN AT BEACH LANE AT CRYSTAL RIVER FL',
    28.89444,
    -82.59333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HSRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HTZF1',
    '02304517',
    'SERFC',
    'FL',
    NULL,
    'HILLSBOROUGH R BL HANNAHS WHIRL NR SULPHUR SPGS FL',
    28.01516,
    -82.44241
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HTZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8915935,
    'NW743',
    '10290500',
    'CNRFC',
    'CA',
    '16050301',
    'ROBINSON CK AT TWIN LKS OUTLET NR BRIDGEPORT~ CA',
    38.16944,
    -119.32194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW743'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LEKF1',
    '280311082282601',
    'SERFC',
    'FL',
    NULL,
    'LAKE ECKLES NEAR CARROLLWOOD FL',
    28.05318,
    -82.47401
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LEKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LPZF1',
    '02294261',
    'SERFC',
    'FL',
    NULL,
    'LAKE PARKER OUTLET AT COMBEE SETTLEMENT FL',
    28.05735,
    -81.90215
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LPZF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LWOF1',
    '02310322',
    'SERFC',
    'FL',
    NULL,
    'LAKE WORRELL OUTFALL NR NEW PORT RICHEY EAST FL',
    28.28056,
    -82.66778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LWOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10247043,
    'ORRF1',
    '02293055',
    'SERFC',
    'FL',
    NULL,
    'ORANGE RIVER NEAR BUCKINGHAM FL',
    26.69194,
    -81.75944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ORRF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RDBF1',
    '02293202',
    'SERFC',
    'FL',
    NULL,
    'CALOOSAHATCHEE RIVER AT FORT MYERS FL',
    26.65071,
    -81.86913
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RDBF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16801585,
    'PHOF1',
    '02294781',
    'SERFC',
    'FL',
    NULL,
    'PEACE RIVER NEAR HOMELAND FL',
    27.82083,
    -81.79972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PHOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PWAF1',
    '02295440',
    'SERFC',
    'FL',
    NULL,
    'PEACE RIVER AT STATE HWY 664A NEAR WAUCHULA FL',
    27.57556,
    -81.80472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PWAF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16949806,
    'RUTF1',
    '02312722',
    'SERFC',
    'FL',
    NULL,
    'WITHLACOOCHEE RIVER NR RUTLAND FL',
    28.85167,
    -82.22111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RUTF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RSKF1',
    '02309200',
    'SERFC',
    'FL',
    NULL,
    'RATTLESNAKE CREEK AT BELLEAIR FL',
    27.94241,
    -82.804
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RSKF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9731286,
    'CAMN7',
    '02146562',
    'SERFC',
    'NC',
    '3050103',
    'CAMPBELL CREEK NR CHARLOTTE  NC',
    35.18666667,
    -80.7366667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CAMN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SRAG1',
    '02196670',
    'SERFC',
    'GA',
    NULL,
    'SAVANNAH RIVER JEFFERSON DAVIS BR AT AUGUSTA GA',
    33.47667,
    -81.95722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRAG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23894288,
    'NUMO3',
    '14316500',
    'NWRFC',
    'OR',
    '17100301',
    'N UMPQUA RIVER ABV COPELAND CK NR TOKETEE FALLS OR',
    43.29595427,
    -122.5367127
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NUMO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23923628,
    'RPRO3b',
    '14330000',
    'NWRFC',
    'OR',
    '17100307',
    'ROGUE RIVER BELOW PROSPECT  OR',
    42.72957187,
    -122.516147
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RPRO3b'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ACMG1',
    '02198820',
    'SERFC',
    'GA',
    NULL,
    'ABERCORN CREEK AT MOUTH NEAR SAVANNAH GA',
    32.24917,
    -81.15361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACMG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23002102,
    'PCGI1',
    '12413360',
    'NWRFC',
    'ID',
    '17010302',
    'EF PINE CREEK ABV GILBERT CR NEAR PINEHURST ID',
    47.44027778,
    -116.1752778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PCGI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17687965,
    'SXGC1',
    '11162570',
    'CNRFC',
    'CA',
    '18050006',
    'SAN GREGORIO C A SAN GREGORIO CA',
    37.32583,
    -122.38556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SXGC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GCYG1',
    '021989715',
    'SERFC',
    'GA',
    NULL,
    'SAVANNAH RIVER AT GARDEN CITY GA',
    32.115556,
    -81.129444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCYG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PWMG1',
    '02198955',
    'SERFC',
    'GA',
    NULL,
    'MIDDLE RIVER AT FISH HOLE AT PORT WENTWORTH GA',
    32.14278,
    -81.13528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PWMG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COBG1',
    '02203873',
    'SERFC',
    'GA',
    NULL,
    'COBBS CREEK AT RAINBOW DR NEAR DECATUR GA',
    33.709444,
    -84.239444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COBG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HCCG1',
    '02343208',
    'SERFC',
    'GA',
    NULL,
    'HODCHODKEE CREEK NEAR LUMPKIN GA',
    32.05111,
    -84.77639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HCCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DWLG1',
    '02389050',
    'SERFC',
    'GA',
    NULL,
    'ETOWAH RIVER 0.2 MI DS GA 53 NR DAWSONVILLE GA',
    34.379444,
    -84.0647
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DWLG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OHCG1',
    '02341200',
    'SERFC',
    'GA',
    NULL,
    'OSSAHATCHEE CREEK NEAR HAMILTON GA',
    32.68833,
    -84.85667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OHCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1997544,
    'FARG1',
    '02314495',
    'SERFC',
    'GA',
    NULL,
    'SUWANNEE RIVER ABOVE FARGO GA',
    30.7075,
    -82.53917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FARG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15594723,
    'SRNO1',
    '04201495',
    'OHRFC',
    'OH',
    NULL,
    'BALDWIN CREEK AT STRONGSVILLE OH',
    41.34944,
    -81.82583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SRNO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20107115,
    'NW931',
    NULL,
    'SERFC',
    'GA',
    '3060109',
    'BEAR CREEK AT HICKORY BEND~ NEAR RINCON~ GA',
    32.33888,
    -81.13555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW931'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HNWH1',
    '16508000',
    'AKRFC',
    'HI',
    NULL,
    'HANAWI STREAM NEAR NAHIKU MAUI HI',
    20.806861,
    -156.114222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HNWH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HPOH1',
    '16587000',
    'AKRFC',
    'HI',
    NULL,
    'HONOPOU STREAM NEAR HUELO MAUI HI',
    20.885639,
    -156.252556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HPOH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HOHH1',
    '16212490',
    'AKRFC',
    'HI',
    NULL,
    'HONOULIULI STR AT H-1 FREEWAY NR WAIPAHU OAHU HI',
    21.37814,
    -158.04478
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOHH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HKPH1',
    '16114800',
    'AKRFC',
    'HI',
    NULL,
    'HANAKAPIAI STREAM ABV HANAKAPIAI FALLS KAUAI HI',
    22.17583,
    -159.59847
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HKPH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HKSH1',
    '16620000',
    'AKRFC',
    'HI',
    NULL,
    'HONOKOHAU STREAM NEAR HONOKOHAU MAUI HI',
    20.9625,
    -156.589444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HKSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HOUH1',
    '16212480',
    'AKRFC',
    'HI',
    NULL,
    'HONOULIULI STREAM TRIBUTARY NEAR WAIPAHU OAHU HI',
    21.401667,
    -158.066944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOUH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HLAH1',
    '16097500',
    'AKRFC',
    'HI',
    NULL,
    'HALAULANI STR AT ALT 400 FT NR KILAUEA KAUAI HI',
    22.178639,
    -159.418611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HLAH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KNAH1',
    '16296500',
    'AKRFC',
    'HI',
    NULL,
    'KAHANA STR AT ALT 30 FT NR KAHANA OAHU HI',
    21.540611,
    -157.882556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNAH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KKLH1',
    '16618000',
    'AKRFC',
    'HI',
    NULL,
    'KAHAKULOA STREAM NEAR HONOKOHAU MAUI HI',
    20.978611,
    -156.554444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KKLH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KKGH1',
    '16414200',
    'AKRFC',
    'HI',
    NULL,
    'KAUNAKAKAI GULCH AT ALTITUDE 75 FEET MOLOKAI HI',
    21.096389,
    -157.017778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KKGH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KMSH1',
    '16264600',
    'AKRFC',
    'HI',
    NULL,
    'KAWAINUI MARSH NR LEVEE STA 15+00 OAHU HI',
    21.394167,
    -157.749361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KMSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KNEH1',
    '16274100',
    'AKRFC',
    'HI',
    NULL,
    'KANEOHE STR BLW KAMEHAMEHA HWY OAHU HI',
    21.41181,
    -157.79811
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNEH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HWSH1',
    '16226200',
    'AKRFC',
    'HI',
    NULL,
    'N. HALAWA STR NR HONOLULU OAHU HI',
    21.382,
    -157.90333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HWSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KLKH1',
    '16094150',
    'AKRFC',
    'HI',
    NULL,
    'KA LOKO RESERVOIR NEAR KILAUEA KAUAI HI',
    22.178972,
    -159.379056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KLKH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KWNH1',
    '16264690',
    'AKRFC',
    'HI',
    NULL,
    'KAWAINUI STREAM NEAR KAILUA OAHU HI',
    21.39286,
    -157.77419
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KWNH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MNWH1',
    '16241600',
    'AKRFC',
    'HI',
    NULL,
    'MANOA STREAM AT WOODLAWN DRIVE OAHU HI',
    21.308333,
    -157.809444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNWH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MKSH1',
    '16238000',
    'AKRFC',
    'HI',
    NULL,
    'MAKIKI STREAM AT KING ST. BRIDGE OAHU HI',
    21.296667,
    -157.836667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MKSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MKHH1',
    '16211600',
    'AKRFC',
    'HI',
    NULL,
    'MAKAHA STR NR MAKAHA OAHU HI',
    21.501583,
    -158.180167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MKHH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MKLH1',
    '16036000',
    'AKRFC',
    'HI',
    NULL,
    'MAKAWELI RIVER NR WAIMEA KAUAI HI',
    21.97139,
    -159.64614
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MKLH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GURP4',
    '50057000',
    NULL,
    'PR',
    NULL,
    'RIO GURABO AT GURABO PR',
    18.25849,
    -65.96808
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GURP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NLIH1',
    '16717000',
    'AKRFC',
    'HI',
    NULL,
    'HONOLII STREAM NR PAPAIKOU HI',
    19.764333,
    -155.151833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NLIH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MWSH1',
    '16254000',
    'AKRFC',
    'HI',
    NULL,
    'MAKAWAO STR NR KAILUA OAHU HI',
    21.3595,
    -157.762167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MWSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NHSH1',
    '16226400',
    'AKRFC',
    'HI',
    NULL,
    'N. HALAWA STR NR QUAR. STN. AT HALAWA OAHU HI',
    21.371944,
    -157.912778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NHSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PAGH1',
    '16770500',
    'AKRFC',
    'HI',
    NULL,
    'PAAUAU GULCH AT PAHALA HI',
    19.207778,
    -155.477222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAGH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OPAH1',
    '16345000',
    'AKRFC',
    'HI',
    NULL,
    'OPAEULA STR NR WAHIAWA OAHU HI',
    21.562222,
    -158.000278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OPAH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OLBH1',
    '16071500',
    'AKRFC',
    'HI',
    NULL,
    'LEFT BRANCH OPAEKAA STR NR KAPAA KAUAI HI',
    22.075611,
    -159.395889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OLBH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'OHGH1',
    '16501200',
    'AKRFC',
    'HI',
    NULL,
    'OHEO GULCH AT DAM NEAR KIPAHULU MAUI HI',
    20.66836,
    -156.05222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OHGH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9673562,
    'LMTS1',
    '02171001',
    'SERFC',
    'SC',
    NULL,
    'SANTEE RIVER @ LK MARION TAILRACE NR PINEVILLE SC',
    33.45,
    -80.16389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LMTS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WKGH1',
    '16240500',
    'AKRFC',
    'HI',
    NULL,
    'WAIAKEAKUA STR AT HONOLULU OAHU HI',
    21.328333,
    -157.799722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WKGH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WISH1',
    '16238500',
    'AKRFC',
    'HI',
    NULL,
    'WAIHI STREAM AT HONOLULU OAHU HI',
    21.328333,
    -157.800833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WISH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WIEH1',
    '16019000',
    'AKRFC',
    'HI',
    NULL,
    'WAIALAE STR AT ALT 3820 FT NR WAIMEA KAUAI HI',
    22.085833,
    -159.569083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WIEH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WKNH1',
    '16294900',
    'AKRFC',
    'HI',
    NULL,
    'WAIKANE STR AT ALT 75 FT AT WAIKANE OAHU HI',
    21.497083,
    -157.862972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WKNH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRSH1',
    '16210000',
    'AKRFC',
    'HI',
    NULL,
    'WAHIAWA RESERVOIR AT SPILLWAY AT WAHIAWA OAHU HI',
    21.496389,
    -158.050278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WSFH1',
    '16060000',
    'AKRFC',
    'HI',
    NULL,
    'SF WAILUA RIVER NR LIHUE KAUAI HI',
    22.036694,
    -159.380167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WSFH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MAAI4',
    '06816290',
    'MBRFC',
    'IA',
    NULL,
    'WEST NODAWAY RIVER AT MASSENA IA',
    41.245542,
    -94.757755
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MAAI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MWCI4',
    '05460100',
    'NCRFC',
    'IA',
    NULL,
    'WILLOW CREEK NEAR MASON CITY IA',
    43.148571,
    -93.268816
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MWCI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WMSH1',
    '16552800',
    'AKRFC',
    'HI',
    NULL,
    'WAIKAMOI STR ABV KULA PL INTAKE NR OLINDA MAUIHI',
    20.80539,
    -156.23122
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WMSH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRVH1',
    '16031000',
    'AKRFC',
    'HI',
    NULL,
    'WAIMEA RIVER NEAR WAIMEA KAUAI HI',
    21.98039,
    -159.66008
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRVH1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ELGI4',
    '05411900',
    'NCRFC',
    'IA',
    NULL,
    'OTTER CREEK AT ELGIN IA',
    42.95242,
    -91.64297
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELGI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LCYI4',
    '05472390',
    'NCRFC',
    'IA',
    NULL,
    'MIDDLE CREEK NEAR LACEY IA',
    41.42139,
    -92.65111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCYI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LFSI4',
    '06600030',
    'MBRFC',
    'IA',
    NULL,
    'LITTLE FLOYD RIVER NEAR SANBORN IA',
    43.1861,
    -95.725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LFSI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1897336,
    'NW615',
    '11304810',
    'CNRFC',
    'CA',
    '18040003',
    'SAN JOAQUIN R BL GARWOOD BRIDGE A STOCKTON CA',
    37.94888,
    -121.32194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW615'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AMAI4',
    '05453200',
    'NCRFC',
    'IA',
    NULL,
    'PRICE CREEK AT AMANA IA',
    41.805,
    -91.87306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AMAI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRLI1',
    '13343000',
    'NWRFC',
    'ID',
    NULL,
    'CLEARWATER RIVER NEAR LEWISTON ID',
    46.43417,
    -116.96778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRLI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 950441,
    'NW574',
    '07099238',
    'ABRFC',
    'CO',
    '11020002',
    'TELLER RESERVOIR SPILLWAY NEAR STONE CITY~ CO',
    38.44055,
    -104.83027
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW574'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23239060,
    'ACKI1',
    '13131000',
    'NWRFC',
    'ID',
    NULL,
    'ANTELOPE CREEK NR DARLINGTON ID',
    43.73361,
    -113.51472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACKI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRYI1',
    '13038000',
    'NWRFC',
    'ID',
    NULL,
    'DRY BED NR RIRIE ID',
    43.63889,
    -111.71611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRYI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BBNI1',
    '13068300',
    'NWRFC',
    'ID',
    NULL,
    'BLACKFOOT RIVER BELOW NORTH CANAL AT BLACKFOOT ID',
    43.16833,
    -112.33472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBNI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BBYI1',
    '13068495',
    'NWRFC',
    'ID',
    NULL,
    'BLACKFOOT RIVER BYPASS NR BLACKFOOT ID',
    43.17083,
    -112.38778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BBYI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DMPI2',
    '05530100',
    'NCRFC',
    'IL',
    NULL,
    'DES PLAINES R AT ALGONQUIN RD AT DES PLAINES IL',
    42.0317,
    -87.8781
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DMPI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRWI2',
    '05535400',
    'NCRFC',
    'IL',
    NULL,
    'WF OF NB CHICAGO RIVER AT DEERFIELD IL',
    42.167222,
    -87.856944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRWI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4779053,
    'NW653',
    '01470755',
    'MARFC',
    'PA',
    '2040203',
    'Maiden Creek near Virginville~ PA',
    40.52527,
    -75.88111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW653'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6249688,
    'NW688',
    '01391102',
    'MARFC',
    'NJ',
    '2030103',
    'Saddle River below Hohokus Brook at Paramus NJ',
    40.96583,
    -74.08472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW688'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1332944,
    'NW587',
    '09111250',
    'CBRFC',
    'CO',
    '14020001',
    'COAL CREEK ABV MCCORMICK DTCH AT CRESTED BUTTE~ CO',
    38.88111,
    -106.99972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW587'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9496458,
    'NW749',
    '01305000',
    'NERFC',
    'NY',
    '2030202',
    'CARMANS RIVER AT YAPHANK NY',
    40.83027,
    -72.915
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW749'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15059765,
    'NW848',
    '11447890',
    'CNRFC',
    'CA',
    '18020163',
    'SACRAMENTO R AB DELTA CROSS CHANNEL CA',
    38.25416,
    -121.52527
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW848'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRCI2',
    '05539670',
    'NCRFC',
    'IL',
    NULL,
    'DES PLAINES RIVER AT CHANNAHON IL',
    41.41444,
    -88.21444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRCI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5863429,
    'NW672',
    '01105638',
    'NERFC',
    'MA',
    '1090001',
    'WEIR RIVER AT LEAVITT STREET AT HINGHAM~ MA',
    42.23722,
    -70.88111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW672'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18393910,
    'NW902',
    '0343233905',
    'OHRFC',
    'TN',
    '5130204',
    'HARPETH RIVER AT MILE 90.5 NEAR FRANKLIN~ TN',
    35.89805,
    -86.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW902'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NBPI2',
    '05536085',
    'NCRFC',
    'IL',
    NULL,
    'NB CHICAGO RIVER AT N PULASKI ROAD AT CHICAGO IL',
    41.974722,
    -87.728333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NBPI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17958013,
    'NW891',
    '333420090445900',
    'LMRFC',
    'MS',
    '8030207',
    'PORTER BAYOU TRIB NO 1 NW OF FRAZIER~ MS',
    33.57611,
    -90.74555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW891'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22590045,
    'NW965',
    '10255890',
    'CNRFC',
    'CA',
    '18100201',
    'EF WHITEWATER R DIV NR BANNING CA',
    34.05083,
    -116.81333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW965'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22590047,
    'NW966',
    '10255897',
    'CNRFC',
    'CA',
    '18100201',
    'SF WHITEWATER R DIV NR BANNING CA',
    34.05083,
    -116.83027
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW966'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'KRDI2',
    '412320088154101',
    'NCRFC',
    'IL',
    NULL,
    'KANKAKEE R AT INFLOW OF POWER PLANT NR LORENZO IL',
    41.38889,
    -88.26139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KRDI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19784539,
    'NW930',
    '11253130',
    'CNRFC',
    'CA',
    '18030009',
    'SAN JOAQUIN R A N SAN MATEO RD NR MENDOTA CA',
    36.77944,
    -120.305
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW930'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CVLI2',
    '05586647',
    'NCRFC',
    'IL',
    NULL,
    'MACOUPIN CREEK AT HWY 108 NR CARLINVILLE IL',
    39.27667,
    -89.83611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CVLI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22271988,
    'SCOA1',
    '02406930',
    'SERFC',
    'AL',
    '3150107',
    'SHIRTEE CREEK NEAR ODENA~ ALABAMA',
    33.21167,
    -86.27333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCOA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2888998,
    'BLOC2',
    '06730500',
    'MBRFC',
    'CO',
    '10190005',
    'BOULDER CREEK AT MOUTH NEAR LONGMONT  CO',
    40.13878,
    -105.02022
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLOC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 230645,
    'CHDC2',
    '06713000',
    'MBRFC',
    'CO',
    '10190003',
    'CHERRY CREEK BELOW CHERRY CREEK LAKE  CO',
    39.65361,
    -104.8625
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CHDC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8590430,
    'FYFA4',
    '07048550',
    'LMRFC',
    'AR',
    '11010001',
    'West Fork White River east of Fayetteville~ AR',
    36.05389,
    -94.08306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FYFA4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14786047,
    'LLCI2',
    '05537500',
    'NCRFC',
    'IL',
    '7120004',
    'LONG RUN NEAR LEMONT  IL',
    41.642531,
    -87.9992252
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLCI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2888948,
    'LHCC2',
    '06724970',
    'MBRFC',
    'CO',
    '10190005',
    'LEFT HAND CREEK AT HOVER ROAD NEAR LONGMONT~ CO',
    40.13428,
    -105.13082
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHCC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 188107,
    'WEIC2',
    '06711618',
    'MBRFC',
    'CO',
    '10190002',
    'WEIR GULCH UPSTREAM FROM 1ST AVE. AT DENVER~ CO',
    39.71725,
    -105.04201
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WEIC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BFLT2',
    '08091250',
    'WGRFC',
    'TX',
    NULL,
    'PALUXY RV AT FM 2870 NR BLUFF DALE TX',
    32.30221,
    -97.95963
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BFLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MTCI3',
    '04092977',
    'NCRFC',
    'IN',
    NULL,
    'TURKEY CREEK AT 61ST AV AT MERRILLVILLE IN',
    41.5074,
    -87.3237
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTCI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCSI3',
    '04092964',
    'NCRFC',
    'IN',
    NULL,
    'TURKEY CREEK AT JOLIET ST AT SCHERERVILLE IN',
    41.4923,
    -87.433
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCSI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EHTK1',
    '07155590',
    'ABRFC',
    'KS',
    NULL,
    'CIMARRON R NR ELKHART KS',
    37.121389,
    -101.8975
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EHTK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10125524,
    'GRTK1',
    '06914000',
    'MBRFC',
    'KS',
    NULL,
    'POTTAWATOMIE C NR GARNETT KS',
    38.33361,
    -95.24861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRTK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLGK1',
    '07137000',
    'ABRFC',
    'KS',
    NULL,
    'FRONTIER DITCH NR COOLIDGE KS',
    38.03833,
    -102.03861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLGK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'COCA2',
    '15261000',
    'AKRFC',
    'AK',
    NULL,
    'COOPER C AT MOUTH NR COOPER LANDING AK',
    60.479979,
    -149.882711
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COCA2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1337252,
    'ACSC2',
    '09132095',
    'CBRFC',
    'CO',
    '14020004',
    'ANTHRACITE CREEK ABOVE MOUTH NEAR SOMERSET',
    38.93722,
    -107.35833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACSC2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRMK1',
    '07179785',
    'ABRFC',
    'KS',
    NULL,
    'NF COTTONWOOD R NR DURHAM KS',
    38.49683,
    -97.26117
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRMK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EWEL1',
    '07380260',
    'LMRFC',
    'LA',
    NULL,
    'EMPIRE WATERWAY SOUTH OF EMPIRE LA',
    29.304444,
    -89.596944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EWEL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GIBL1',
    '292952090565300',
    'LMRFC',
    'LA',
    NULL,
    'CRMS0411-H01-RT',
    29.497778,
    -90.948056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GIBL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EUGL1',
    '073816525',
    'LMRFC',
    'LA',
    NULL,
    'MOUTH OF ATCHAFALAYA RIVER AT ATCHAFALAYA BAY',
    29.43025,
    -91.33389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EUGL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CCCL1',
    '300312091320000',
    'LMRFC',
    'LA',
    NULL,
    'ARM OF GRAND LAKE NEAR CROOK CHENE COVE',
    30.05333,
    -91.53333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CCCL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16932102,
    'ACOF1',
    '02309740',
    'SERFC',
    'FL',
    '3100207',
    'ANCLOTE RIVER NEAR ODESSA FL',
    28.22924,
    -82.59549
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ACOF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16801879,
    'BGPF1',
    '02295194',
    'SERFC',
    'FL',
    '3100101',
    'PEACE RIVER AT BOWLING GREEN FL',
    27.64583,
    -81.8025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BGPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16801931,
    'CBWF1',
    '02295520',
    'SERFC',
    'FL',
    '13100101',
    'LITTLE CHARLIE CREEK NEAR BOWLING GREEN FL',
    27.62708,
    -81.72775
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CBWF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GRVL1',
    '300602090375100',
    'LMRFC',
    'LA',
    NULL,
    'CRMS5373-H01-RT',
    30.100556,
    -90.630833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRVL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HBRL1',
    '07378008',
    'LMRFC',
    'LA',
    NULL,
    'HURRICANE CREEK AT BATON ROUGE LA',
    30.481944,
    -91.1225
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HBRL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NFDM3',
    '01161280',
    'NERFC',
    'MA',
    NULL,
    'CONNECTICUT RIVER NEAR NORTHFIELD MA',
    42.68333,
    -72.47194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NFDM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3761686,
    'ELGL1',
    '07349890',
    'LMRFC',
    'LA',
    NULL,
    'RED CHUTE BAYOU NEAR ELM GROVE LA.',
    32.3625,
    -93.50278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELGL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3761614,
    'FRCL1',
    '07349374',
    'LMRFC',
    'LA',
    NULL,
    'FLAT RIVER NEAR CURTIS LA',
    32.43889,
    -93.62694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRCL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 941140108,
    'HOSL1',
    '07344400',
    'LMRFC',
    'LA',
    NULL,
    'RED RIVER NEAR HOSSTON LA.',
    32.89306,
    -93.82222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOSL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3760196,
    'REDL1',
    '07349849',
    'LMRFC',
    'LA',
    NULL,
    'RED CHUTE BAYOU AT DOGWOOD TR. RD. NR BOSSIER CITY',
    32.56972,
    -93.63472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'REDL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MNLL1',
    '292952089453800',
    'LMRFC',
    'LA',
    NULL,
    'CRMS0282-H01-RT',
    29.497778,
    -89.760556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNLL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3727857,
    'NW641',
    '06892518',
    'MBRFC',
    'KS',
    '10270104',
    'KANSAS R NR LAKE QUIVIRA~ KS',
    39.03388,
    -94.79638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW641'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1440289,
    'NW591',
    '08073100',
    'WGRFC',
    'TX',
    '12040104',
    'Langham Ck at Addicks Res Outflow nr Addicks~ TX',
    29.79638,
    -95.62694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW591'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1523673,
    'NW595',
    '08042558',
    'WGRFC',
    'TX',
    '12040202',
    'W Fk Double Bayou at Eagle Ferry Rd nr Anahuac~ TX',
    29.67777,
    -94.66083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW595'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PVHM3',
    '420259070105600',
    'NERFC',
    'MA',
    NULL,
    'PROVINCETOWN TIDE GAGE PROVINCETOWN MA',
    42.0497,
    -70.1822
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PVHM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TBML1',
    '304616091365800',
    'LMRFC',
    'LA',
    NULL,
    'TAIL BAY AT MORGANZA FLOODWAY TEMPORARY GAGE',
    30.77111,
    -91.61611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TBML1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WBCL1',
    '292939089544400',
    'LMRFC',
    'LA',
    NULL,
    'WILKINSON BAYOU CUTOFF NORTH OF WILKINSON BAY LA',
    29.49414,
    -89.91231
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBCL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GBRL1',
    '07352820',
    'LMRFC',
    'LA',
    NULL,
    'GRAND BAYOU RESERVOIR AT SPILLWAY NR COUSHATTA LA',
    31.99879,
    -93.21327
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBRL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SBDL1',
    '07349450',
    'LMRFC',
    'LA',
    NULL,
    'BODCAU BAYOU NEAR SPRINGHILL LA',
    33.00389,
    -93.51667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBDL1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CLIM3',
    '01095503',
    'NERFC',
    'MA',
    NULL,
    'NASHUA RIVER WATER STREET BRIDGE AT CLINTON MA',
    42.41944,
    -71.66611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLIM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'POPM3',
    '413601070275800',
    'NERFC',
    'MA',
    NULL,
    'POPPONESSET BAY MASHPEE NECK RD NEAR MASHPEE MA',
    41.60016,
    -70.46612
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'POPM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11688596,
    'DELM2',
    '01589197',
    'MARFC',
    'MD',
    NULL,
    'GWYNNS FALLS NEAR DELIGHT MD',
    39.44294,
    -76.78342
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DELM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'NPTM3',
    '01100870',
    'NERFC',
    'MA',
    NULL,
    'MERRIMACK RIVER AT NEWBURYPORT MA',
    42.81556,
    -70.87333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NPTM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SESM3',
    '414507070091400',
    'NERFC',
    'MA',
    NULL,
    'SESUIT HARBOR TIDE GAGE AT DENNIS MA',
    41.7519,
    -70.1539
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SESM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FCCM2',
    '01642199',
    'MARFC',
    'MD',
    NULL,
    'CARROLL CREEK ABOVE ROCK CREEK AT FREDERICK MD',
    39.42442,
    -77.42978
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FCCM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BMTM2',
    '01585225',
    'MARFC',
    'MD',
    NULL,
    'MOORES RUN TRIB. NEAR TODD AVE AT BALTIMORE MD',
    39.336667,
    -76.540556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BMTM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HDTM2',
    '01601420',
    'MARFC',
    'MD',
    NULL,
    'HOFFMAN DRAINAGE TUNNEL AT CLARYSVILLE MD',
    39.638333,
    -78.893056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HDTM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WPRM2',
    '01585175',
    'MARFC',
    'MD',
    NULL,
    'UNNAMED DITCH ON WILSON PT RD AT MIDDLE RIVER MD',
    39.332222,
    -76.427778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPRM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11687584,
    'WRBM2',
    '01581750',
    'MARFC',
    'MD',
    NULL,
    'WINTERS RUN HD OF OTTER PT CREEK NEAR BEL AIR MD',
    39.51553,
    -76.36883
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRBM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FDTM2',
    '01642198',
    'MARFC',
    'MD',
    NULL,
    'CARROLL CREEK NEAR FREDERICK MD',
    39.43603,
    -77.44058
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FDTM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ORRM2',
    '01583570',
    'MARFC',
    'MD',
    NULL,
    'POND BRANCH AT OREGON RIDGE MD',
    39.48031,
    -76.6875
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ORRM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BNPT2',
    '08075605',
    'WGRFC',
    'TX',
    NULL,
    'BERRY BAYOU AT NEVADA ST HOUSTON TX',
    29.65639,
    -95.22889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BNPT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HALM1',
    '01049330',
    'NERFC',
    'ME',
    NULL,
    'KENNEBEC RIVER AT HALLOWELL MAINE',
    44.286667,
    -69.788056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HALM1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCBM4',
    '04127200',
    'NCRFC',
    'MI',
    NULL,
    'BOARDMAN RIVER AT BEITNER RD NR TRAVERSE CITY MI',
    44.6753,
    -85.6308
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCBM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14770846,
    'WNNI2',
    '05548105',
    'NCRFC',
    'IL',
    '7120006',
    'NIPPERSINK CREEK ABOVE WONDER LAKE  IL',
    42.38528,
    -88.36944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WNNI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3398494,
    'IHCI3',
    '04092750',
    'NCRFC',
    'IN',
    '4040001',
    'INDIANA HARBOR CANAL AT EAST CHICAGO  IN',
    41.64917,
    -87.46861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'IHCI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6333794,
    'PBCG1',
    '02204037',
    'SERFC',
    'GA',
    '3070103',
    'POLE BRIDGE CR AT EVANS MILL RD NEAR LITHONIA~ GA',
    33.66833,
    -84.15111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PBCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6333710,
    'SFHG1',
    '02203960',
    'SERFC',
    'GA',
    '3070103',
    'SNAPFINGER CR AT THOMPSON MILL RD NR LITHONIA~ GA',
    33.69667,
    -84.19861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFHG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6333546,
    'SOCG1',
    '02207160',
    'SERFC',
    'GA',
    '3070103',
    'STONE MOUNTAIN CREEK AT GA 124~ NEAR LITHONIA~ GA',
    33.77333,
    -84.07722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SOCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ASZM7',
    '05509300',
    'NCRFC',
    'MO',
    NULL,
    'SALT RIVER NEAR ASHBURN MO',
    39.52194,
    -91.2025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ASZM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7709954,
    'GBGM3',
    '01198000',
    'NERFC',
    'MA',
    '1100005',
    'GREEN RIVER NEAR GREAT BARRINGTON  MA',
    42.19291,
    -73.39123
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBGM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CRZM7',
    '05507795',
    'NCRFC',
    'MO',
    NULL,
    'CANNON RE REG DAM POOL NEAR CENTER MO',
    39.57306,
    -91.57083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRZM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BDGM4',
    '04122025',
    'NCRFC',
    'MI',
    NULL,
    'MUSKEGON RIVER AT BRIDGETON MI',
    43.347222,
    -85.939444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BDGM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BUCM4',
    '04119160',
    'NCRFC',
    'MI',
    NULL,
    'BUCK CREEK AT WILSON AVENUE AT GRANDVILLE MI',
    42.9025,
    -85.7627
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BUCM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DESM7',
    '07019500',
    'NCRFC',
    'MO',
    NULL,
    'JOACHIM CREEK AT DE SOTO MO',
    38.126667,
    -90.556389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DESM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4390865,
    'KRRM7',
    '06893562',
    'MBRFC',
    'MO',
    NULL,
    'BRUSH CREEK AT ROCKHILL ROAD IN KANSAS CITY MO',
    39.03925,
    -94.57872
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KRRM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12135268,
    'BCMM4',
    '04122100',
    'NCRFC',
    'MI',
    '4060102',
    'BEAR CREEK NEAR MUSKEGON  MI',
    43.28861,
    -86.22278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCMM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3646792,
    'NW640',
    '06888990',
    'MBRFC',
    'KS',
    '10270102',
    'KANSAS R AT TOPEKA WATER PLANT~ KS',
    39.06777,
    -95.71166
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW640'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12242498,
    'SCWM4',
    '04112000',
    'NCRFC',
    'MI',
    '4050004',
    'SLOAN CREEK NEAR WILLIAMSTON  MI',
    42.67583,
    -84.36389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCWM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4390909,
    'LRLM7',
    '06893820',
    'MBRFC',
    'MO',
    '10300101',
    'Little Blue R. at Lees Summit Rd in Independence',
    39.01736,
    -94.38728
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LRLM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7607329,
    'WCBM7',
    '07052160',
    'LMRFC',
    'MO',
    NULL,
    'WILSON CREEK NEAR BATTLEFIELD MO',
    37.11775,
    -93.40386
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCBM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GBSM8',
    '06043120',
    'MBRFC',
    'MT',
    NULL,
    'GALLATIN RIVER ABOVE DEER CREEK NEAR BIG SKY MT',
    45.297222,
    -111.211389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBSM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6010272,
    'LBDM7',
    '06935550',
    'MBRFC',
    'MO',
    '10300200',
    'Missouri River near Labadie~ MO',
    38.56583,
    -90.83917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LBDM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DNNM6',
    '02430005',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WW BELOW JAMIE WHITTEN L&D NR DENNIS MS',
    34.52,
    -88.32361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DNNM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKAM6',
    '02433496',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WATERWAY BL AMORY L&D AT AMORY MS',
    34.00944,
    -88.49139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKAM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKBM6',
    '02433151',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WATERWAY BL WILKINS L&D NR SMITHVILLE MS',
    34.063,
    -88.428
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKBM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKCM6',
    '02431011',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WATERWAY BL FULTON L&D NR FULTON MS',
    34.25497,
    -88.42417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKCM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKDM6',
    '02430626',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WATERWAY BL RANKIN L&D NR FULTON MS',
    34.36036,
    -88.40878
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKDM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LKEM6',
    '02430161',
    'SERFC',
    'MS',
    NULL,
    'TENN-TOM WATERWAY BL MONTGOMERY L&D NR FULTON MS',
    34.46244,
    -88.36831
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LKEM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FRAM8',
    '06175510',
    'MBRFC',
    'MT',
    NULL,
    'MISSOURI R AT E FRAZER PUMP PLANT NR FRAZER MT',
    48.03333,
    -106.0
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRAM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FRZM8',
    '06175100',
    'MBRFC',
    'MT',
    NULL,
    'MISSOURI R AT W FRAZER PUMP PLANT NR FRAZER MT',
    48.03056,
    -106.12222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRZM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9202911,
    'INDN7',
    '0212467595',
    'SERFC',
    'NC',
    NULL,
    'GOOSE CREEK AT SR1525 NR INDIAN TRAIL NC',
    35.125,
    -80.60278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22152807,
    'LLDN7',
    '03455773',
    'LMRFC',
    'NC',
    NULL,
    'LAKE LOGAN AT DAM NEAR HAZELWOOD NC',
    35.4225,
    -82.92222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LLDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCTN7',
    '0214265828',
    'SERFC',
    'NC',
    NULL,
    'MCDOWELL CR TRIB AT SR2131 NR HICKS CROSSROADS NC',
    35.40179,
    -80.91752
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCTN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BKJN7',
    '02087337',
    'SERFC',
    'NC',
    NULL,
    'WALNUT CREEK AT BUCK JONES ROAD AT RALEIGH NC',
    35.773105,
    -78.736725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKJN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'JHSN7',
    '02087339',
    'SERFC',
    'NC',
    NULL,
    'LAKE JOHNSON ABOVE DAM AT RALEIGH NC',
    35.762718,
    -78.70558
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JHSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HUSN7',
    '02105511',
    'SERFC',
    'NC',
    NULL,
    'CAPE FEAR R AT LOCK NO. 3 NR TARHEEL NC (AUX)',
    34.83472,
    -78.82444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HUSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HRMN7',
    '02098206',
    'SERFC',
    'NC',
    NULL,
    'HAW RIVER NEAR MONCURE NC',
    35.63222,
    -79.06
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRMN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HRVN7',
    '02098198',
    'SERFC',
    'NC',
    NULL,
    'HAW R BELOW B. EVERETT JORDAN DAM NR MONCURE NC',
    35.64944,
    -79.06556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRVN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11239079,
    'SWFN7',
    '0209205053',
    'SERFC',
    'NC',
    NULL,
    'SWIFT CREEK AT HWY 43 NR STREETS FERRY NC',
    35.23083,
    -77.11389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SWFN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8696507,
    'HYLN7',
    '02077280',
    'SERFC',
    'NC',
    NULL,
    'HYCO LAKE AT DAM NR ROXBORO NC',
    36.51167,
    -79.04722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HYLN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 933020125,
    'CRDN7',
    '0208706575',
    'SERFC',
    'NC',
    NULL,
    'BEAVERDAM CREEK AT DAM NEAR CREEDMOOR NC',
    36.02361,
    -78.68917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CRDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8782461,
    'LCRN7',
    '0208700550',
    'SERFC',
    'NC',
    NULL,
    'LITTLE LICK CREEK AT NC HWY 98 AT OAK GROVE NC',
    35.98233,
    -78.8245
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LCRN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1969301,
    'PAMN7',
    '02084472',
    'SERFC',
    'NC',
    NULL,
    'PAMLICO RIVER AT WASHINGTON NC',
    35.54333,
    -77.06194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAMN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8893374,
    'NEDN7',
    '0209741387',
    'SERFC',
    'NC',
    NULL,
    'NORTHEAST CREEK TRIB AT SR1182 NR LOWES GROVE NC',
    35.91539,
    -78.89353
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NEDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8777941,
    'HBSN7',
    '02085039',
    'SERFC',
    'NC',
    NULL,
    'ENO RIVER AT COLE MILL RD NR HUCKLEBERRY SPRING',
    36.05942,
    -78.97808
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HBSN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3982120,
    'WLBO1',
    '03266560',
    'OHRFC',
    'OH',
    '5080001',
    'Mad River at West Liberty OH - 03266560',
    40.25222,
    -83.74972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLBO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21975983,
    'MACN6',
    '04234254',
    'NERFC',
    'NY',
    '4140201',
    'GANARGUA CREEK AT MACEDON NY',
    43.06747,
    -77.29822
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MACN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21873481,
    'CTWN8',
    '06329610',
    'MBRFC',
    'ND',
    NULL,
    'YELLOWSTONE R NO. 2 NR CARTWRIGHT ND',
    47.86194,
    -103.96639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CTWN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3935574,
    'WMBO1',
    '03246500',
    'OHRFC',
    'OH',
    '5090202',
    'East Fork Little Miami River at Williamsburg OH',
    39.0525,
    -84.05056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WMBO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DSHN1',
    '06883530',
    'MBRFC',
    'NE',
    NULL,
    'LITTLE BLUE RIVER AT COUNTY LINE NR DESHLER NEBR.',
    40.214722,
    -97.820833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DSHN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 940110239,
    'WLRN8',
    '06330110',
    'MBRFC',
    'ND',
    NULL,
    'MISSOURI R NO. 9 AT WILLISTON ND',
    48.13694,
    -103.60444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLRN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CPPN8',
    '05056995',
    'NCRFC',
    'ND',
    NULL,
    'SHEYENNE RIVER ON HWY 200 NEAR COOPERSTOWN ND',
    47.43917,
    -98.01917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPPN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HORN8',
    '05059310',
    'NCRFC',
    'ND',
    NULL,
    'SHEYENNE RIVER DIVERSION NR HORACE ND',
    46.75167,
    -96.92583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HORN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MFGN5',
    '09430020',
    'CBRFC',
    'NM',
    NULL,
    'W FORK GILA R BLW MDL FORK NR GILA HOT SPRINGS NM',
    33.2218,
    -108.2419
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MFGN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CATN5',
    '09443800',
    'CBRFC',
    'NM',
    NULL,
    'WHITEWATER CREEK AT CATWALK NRT NEAR GLENWOOD NM',
    33.37494,
    -108.83708
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CATN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'EAAN5',
    '08329720',
    'WGRFC',
    'NM',
    NULL,
    'EMBUDO ARROYO AT ALBUQUERQUE NM',
    35.10222,
    -106.4925
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EAAN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HAHN5',
    '08329838',
    'WGRFC',
    'NM',
    NULL,
    'SF HAHN ARROYO IN ALBUQUERQUE NM',
    35.12111,
    -106.56778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HAHN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HBCN5',
    '09367580',
    'CBRFC',
    'NM',
    NULL,
    'HOGBACK CANAL NEAR WATERFLOW NM',
    36.74642,
    -108.53797
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HBCN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LSDN5',
    '07214680',
    'ABRFC',
    'NM',
    NULL,
    'LA SIERRA DITCH NEAR HOLMAN NM',
    36.05027,
    -105.45458
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LSDN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MRLN5',
    '09442980',
    'CBRFC',
    'NM',
    NULL,
    'MINERAL CREEK NEAR GLENWOOD NM',
    33.42061,
    -108.82594
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MRLN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ETDN5',
    '07202500',
    'ABRFC',
    'NM',
    NULL,
    'EAGLE TAIL DITCH NR MAXWELL NM',
    36.64864,
    -104.55916
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ETDN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'HAAN5',
    '08329840',
    'WGRFC',
    'NM',
    NULL,
    'HAHN ARROYO IN ALBUQUERQUE NM',
    35.12588,
    -106.5903
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HAAN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'CWAN5',
    '08329700',
    'WGRFC',
    'NM',
    NULL,
    'CAMPUS WASH AT ALBUQUERQUE NM',
    35.09389,
    -106.62361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CWAN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GEYN2',
    '10245100',
    'CNRFC',
    'NV',
    NULL,
    'GEYSER CK AT SPGS ORIFICE NR MINERVA NV',
    38.68,
    -114.66667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GEYN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10409034,
    'BSSN2',
    '10243224',
    'CBRFC',
    'NV',
    NULL,
    'BIG SPGS CK SOUTH CHANNEL NR BAKER NV',
    38.69917,
    -114.13111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BSSN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MCCN2',
    '10336715',
    'CNRFC',
    'NV',
    NULL,
    'MARLETTE CK NR CARSON CITY NV',
    39.17222,
    -119.90694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TPCN2',
    '10297010',
    'CNRFC',
    'NV',
    NULL,
    'TOPAZ CANAL BLW TOPAZ LAKE NR TOPAZ CA',
    38.69556,
    -119.50806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TPCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BLEN6',
    '04214231',
    'NERFC',
    'NY',
    NULL,
    'S BR EIGHTEENMILE CR AT BLEY RD AT EDEN VALLEY',
    42.680278,
    -78.878611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BLEN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRCN2',
    '10350500',
    'CNRFC',
    'NV',
    NULL,
    'TRUCKEE RV AT CLARK NV',
    39.56545,
    -119.48619
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'GSUN2',
    '362727116013501',
    'CNRFC',
    'NV',
    NULL,
    '230  S17 E53 21CAC 1    GRAPEVINE SPRINGS',
    36.4575,
    -116.02639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSUN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PTRN6',
    '01359135',
    'NERFC',
    'NY',
    NULL,
    'PATROON CREEK AT ALBANY NY',
    42.66344,
    -73.74486
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTRN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AGBP4',
    '50011128',
    NULL,
    'PR',
    NULL,
    'CANAL MOCA ABV AGUADILLA PLANT AGUADILLA PR',
    18.44922,
    -67.13732
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AGBP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SPGO1',
    '03255300',
    'OHRFC',
    'OH',
    NULL,
    'MILL CREEK AT KEMPER ROAD AT SHARONVILLE OH',
    39.284444,
    -84.433056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SPGO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8944415,
    'EGLN2',
    '103367592',
    'CNRFC',
    'NV',
    '16050101',
    'EAGLE ROCK CK NR STATELINE  NV',
    38.95657448,
    -119.9276805
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EGLN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'AMDO1',
    '402823080552300',
    'OHRFC',
    'OH',
    NULL,
    'YELLOW CREEK AT AMSTERDAM OH',
    40.47306,
    -80.92306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AMDO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'FSCO2',
    '07334238',
    'ABRFC',
    'OK',
    NULL,
    'SHEEP CREEK SPRING NEAR FITTSTOWN OK',
    34.57306,
    -96.6475
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FSCO2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23752448,
    'NMFO3',
    '14147500',
    'NWRFC',
    'OR',
    NULL,
    'N FK OF M FK WILLAMETTE R NR OAKRIDGE OR',
    43.75694,
    -122.50417
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NMFO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'USAP4',
    '50043197',
    NULL,
    'PR',
    NULL,
    'RIO USABON AT HWY 162 NR BARRANQUITAS PR',
    18.16154,
    -66.30942
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'USAP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RBPS1',
    '02169505',
    'SERFC',
    'SC',
    NULL,
    'ROCKY BRANCH AT PICKENS ST AT COLUMBIA SC',
    33.994722,
    -81.023889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RBPS1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MOCP4',
    '50147800',
    NULL,
    'PR',
    NULL,
    'RIO CULEBRINAS AT HWY 404 NR MOCA PR',
    18.36174,
    -67.09254
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MOCP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'VERP4',
    '50064200',
    NULL,
    'PR',
    NULL,
    'RIO GRANDE NR EL VERDE PR',
    18.34541,
    -65.84198
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'VERP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LFTV2',
    '0162246784',
    'MARFC',
    'VA',
    NULL,
    'BUCKHORN CREEK ABOVE RT 250 NR LONE FOUNTAIN VA',
    38.2853,
    -79.2375
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LFTV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MAUP4',
    '50090500',
    NULL,
    'PR',
    NULL,
    'RIO MAUNABO AT LIZAS PR',
    18.02722,
    -65.94
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MAUP4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14787595,
    'LISI2',
    '05540195',
    'NCRFC',
    'IL',
    '7120004',
    'ST. JOSEPH CREEK AT ROUTE 34 AT LISLE  IL',
    41.80194,
    -88.06889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LISI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DDBT2',
    '08067690',
    'WGRFC',
    'TX',
    NULL,
    'LAKE CK NR DOBBIN TX',
    30.37153,
    -95.76914
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DDBT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BFFT2',
    '08083280',
    'WGRFC',
    'TX',
    NULL,
    'ELM CK AT FM 89 NR BUFFALO GAP TX',
    32.27519,
    -99.83572
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BFFT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9734048,
    'WXCN7',
    '02147126',
    'SERFC',
    'NC',
    '3050103',
    'WAXHAW CREEK AT SR1103 NEAR JACKSON  NC',
    34.83694444,
    -80.7916667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WXCN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 933090052,
    'NW1010',
    '02272676',
    'SERFC',
    'FL',
    '3090101',
    'CYPRESS SLOUGH NEAR BASINGER~ FL',
    27.37277,
    -80.98277
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1010'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 166743841,
    'NW1007',
    '02303350',
    'SERFC',
    'FL',
    '3100205',
    'TROUT CREEK NEAR SULPHUR SPRINGS FL',
    28.13555,
    -82.35583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1007'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 27683990,
    'NW1004',
    '02303410',
    'SERFC',
    'FL',
    '3100205',
    'CYPRESS CREEK TRIBUTARY NR WESLEY CHAPEL FL',
    28.25416,
    -82.38972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1004'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 166743866,
    'NW1008',
    '02307359',
    'SERFC',
    'FL',
    '3100206',
    'BROOKER CREEK NEAR TARPON SPRINGS FL',
    28.08472,
    -82.69472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1008'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 933100018,
    'NW1011',
    '02310300',
    'SERFC',
    'FL',
    '3100207',
    'PITHLACHASCOTEE RIVER NEAR NEW PORT RICHEY FL',
    28.25416,
    -82.64388
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1011'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 25727251,
    'NW1002',
    '02354350',
    'SERFC',
    'GA',
    '3130009',
    'CHICKASAWHATCHEE CREEK NEAR ALBANY~ GA',
    31.59305,
    -84.4575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1002'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6078329,
    'HOLM3',
    '01095375',
    'NERFC',
    'MA',
    '1070004',
    'QUINAPOXET RIVER AT CANADA MILLS NEAR HOLDEN  MA',
    42.37286967,
    -71.8281279
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HOLM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11689712,
    'TTDM2',
    '01589317',
    'MARFC',
    'MD',
    '2060003',
    'TRIBUTARY TO DEAD RUN TRIBUTARY AT WOODLAWN  MD',
    39.327,
    -76.74513889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TTDM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4390589,
    'SBCM7',
    '06893970',
    'MBRFC',
    'MO',
    '10300101',
    'Spring Branch Ck at Holke Rd in Independence  MO',
    39.08833866,
    -94.3435605
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SBCM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7053819,
    'GRZM5',
    '05078470',
    'NCRFC',
    'MN',
    '9020303',
    'Judicial Ditch 64 Near Mentor  MN (SW4)',
    47.73777778,
    -96.2025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GRZM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2649086,
    'WLWW3',
    '05341752',
    'NCRFC',
    'WI',
    '7030005',
    'WILLOW RIVER @ WILLOW R STATE PARK NR BURKHARDT WI',
    45.01166667,
    -92.7083333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLWW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2453120,
    'STYW3',
    '05382257',
    'NCRFC',
    'WI',
    '7040006',
    'STILLWELL CREEK AT YARD ROAD NEAR TOMAH  WI',
    44.00055556,
    -90.6811111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STYW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6865553,
    'GKEW3',
    '04073462',
    'NCRFC',
    'WI',
    '4030201',
    'WHITE CREEK AT SPRING GROVE ROAD NR GREEN LAKE  WI',
    43.8161111,
    -88.9283333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GKEW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10312264,
    'PTCN6',
    '04276842',
    'NERFC',
    'NY',
    '4150408',
    'PUTNAM CREEK EAST OF CROWN POINT CENTER NY',
    43.9425,
    -73.4636111
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PTCN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8503542,
    'CMCV2',
    '01673638',
    'MARFC',
    'VA',
    '2080106',
    'COHOKE MILL CREEK NEAR LESTER MANOR  VA',
    37.6268101,
    -76.962467
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CMCV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8280869,
    'GSRC1',
    '11467510',
    'CNRFC',
    'CA',
    '18010109',
    'SF GUALALA R NR THE SEA RANCH CA',
    38.70907868,
    -123.4266753
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GSRC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 24452801,
    'WIFI1',
    '13058510',
    'NWRFC',
    'ID',
    '17040201',
    'SAND CREEK NEAR UCON ID',
    43.57416667,
    -111.895
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WIFI1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23773411,
    'LOCO3',
    '14161500',
    'NWRFC',
    'OR',
    '17090004',
    'LOOKOUT CREEK NEAR BLUE RIVER  OR',
    44.2095708,
    -122.256733
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOCO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23785723,
    'WNFO3',
    '14187000',
    'NWRFC',
    'OR',
    '17090006',
    'WILEY CREEK NEAR FOSTER  OR',
    44.37234769,
    -122.6231384
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WNFO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12227771,
    'CGAM4',
    '04126805',
    'NCRFC',
    'MI',
    NULL,
    'CRYSTAL RIVER AT STATE HWY-22 NEAR GLEN ARBOR~ MI',
    44.915,
    -85.98277
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CGAM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5866419,
    'ALEM3',
    '01103025',
    'NERFC',
    'MA',
    '1090001',
    'ALEWIFE BROOK NEAR ARLINGTON  MA',
    42.40704077,
    -71.1339422
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ALEM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6078267,
    'STEM3',
    '01095220',
    'NERFC',
    'MA',
    '1070004',
    'STILLWATER RIVER NEAR STERLING  MA',
    42.41092507,
    -71.7911829
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STEM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23719623,
    'NW1015',
    '14093000',
    'NWRFC',
    'OR',
    '17070306',
    'SHITIKE CREEK NEAR WARM SPRINGS  OR',
    44.7642856,
    -121.2364393
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1015'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23805106,
    'SCOO3',
    '14202980',
    'NWRFC',
    'OR',
    '17090010',
    'SCOGGINS CK BLW HENRY HAGG LAKE  NR GASTON  OR',
    45.47444,
    -123.18638
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCOO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14780320,
    'NW836',
    '411955088280601',
    'NCRFC',
    'IL',
    '7120005',
    'HANSON GRAVEL PIT AT CULVERT NEAR MORRIS~ IL',
    41.32194,
    -88.47444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW836'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23300536,
    'NW984',
    '13172455',
    'NWRFC',
    'ID',
    '17050103',
    'SNAKE RIVER BLW BYPASS NR MURPHY~ ID',
    43.25416,
    -116.38972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW984'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8625037,
    'GCKV2',
    '02059485',
    'SERFC',
    'VA',
    '3010101',
    'GOOSE CREEK AT RT 747 NEAR BUNKER HILL  VA',
    37.26638889,
    -79.5877778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GCKV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3432050,
    'PKCG1',
    '02341725',
    'SERFC',
    'GA',
    '3130003',
    'PINE KNOT CREEK NEAR EELBEECK  GA',
    32.4393099,
    -84.7332603
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PKCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8624161,
    'BOCV2',
    '02061000',
    'SERFC',
    'VA',
    '3010101',
    'BIG OTTER RIVER NEAR BEDFORD  VA',
    37.36403026,
    -79.41919689
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BOCV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1815815,
    'NW601',
    '04026450',
    'NCRFC',
    'WI',
    '4010302',
    'BAD RIVER NEAR MELLEN~ WI',
    46.27111,
    -90.71166
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW601'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3577682,
    'NW637',
    '03414100',
    'OHRFC',
    'KY',
    '5130103',
    'CUMBERLAND RIVER AT BURKESVILLE~ KY',
    36.79638,
    -85.35583
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW637'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17556267,
    'WBWI4',
    '0546494170',
    'NCRFC',
    'IA',
    '7080206',
    'WB Wapsinonoc Cr at College St at West Branch~ IA',
    41.67358,
    -91.34369
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBWI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23751940,
    'NW1017',
    '14145500',
    'NWRFC',
    'OR',
    '17090001',
    'MF WILLAMETTE RIVER ABV SALT CRK  NEAR OAKRIDGE OR',
    43.72095655,
    -122.4378256
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1017'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18507740,
    'BCCO1',
    '03322485',
    'OHRFC',
    'OH',
    '5120101',
    'Beaver Creek near Celina OH',
    40.53472,
    -84.57639
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCCO1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14771186,
    'ETCI2',
    '05550300',
    'NCRFC',
    'IL',
    '7120006',
    'TYLER CREEK AT ELGIN  IL',
    42.05833,
    -88.30389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ETCI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6658313,
    'OTRM5',
    '05046000',
    'NCRFC',
    'MN',
    '9020103',
    'OTTER TAIL RIVER BL ORWELL D NR FERGUS FALLS  MN',
    46.20972,
    -96.18472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'OTRM5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 4867791,
    'CEZM7',
    '05507800',
    'NCRFC',
    'MO',
    '7110007',
    'Salt River near Center  MO',
    39.57406,
    -91.57181
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CEZM7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5765175,
    'JSLT2',
    '08149900',
    'WGRFC',
    'TX',
    '12090204',
    'S Llano Rv at Flat Rock Ln at Junction~ TX',
    30.47901,
    -99.77804
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JSLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11947259,
    'BGLM4',
    '04036000',
    'NCRFC',
    'MI',
    '4020102',
    'WEST BRANCH ONTONAGON RIVER NEAR BERGLAND  MI',
    46.5875,
    -89.54167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BGLM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18631042,
    'AVLA1',
    '02444161',
    'SERFC',
    'AL',
    '3160106',
    'TOMBIGBEE RIVER BEL BEVIL L&D NR PICKENSVILLE~ AL',
    33.21056,
    -88.28861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'AVLA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18548516,
    'CLDA1',
    '02469762',
    'SERFC',
    'AL',
    '3160203',
    'TOMBIGBEE R BL COFFEEVILLE L&D NEAR COFFEEVILLE',
    31.75833,
    -88.12917
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLDA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14293805,
    'CNDN8',
    '05056100',
    'NCRFC',
    'ND',
    '9020201',
    'MAUVAIS COULEE NR CANDO  ND',
    48.44806,
    -99.10222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CNDN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14827685,
    'KNDN8',
    '05059000',
    'NCRFC',
    'ND',
    '9020204',
    'SHEYENNE RIVER NEAR KINDRED  ND',
    46.63167,
    -97.00028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'KNDN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14837877,
    'HNYI2',
    '05558300',
    'NCRFC',
    'IL',
    '7130001',
    'ILLINOIS RIVER AT HENRY~ IL',
    41.10722,
    -89.35611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HNYI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21384136,
    'BWAA3',
    '09426000',
    'CBRFC',
    'AZ',
    '15030204',
    'BILL WILLIAMS RIVER BELOW ALAMO DAM  AZ',
    34.23083,
    -113.60806
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BWAA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 20585206,
    'SFLA3',
    '09383409',
    'CBRFC',
    'AZ',
    '15020001',
    'SOUTH FORK LITTLE COLORADO RIVER NEAR GREER~ AZ',
    34.0555,
    -109.40117
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SFLA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18215736,
    'WLDA1',
    '02466031',
    'SERFC',
    'AL',
    '3160113',
    'BLACK WARRIOR RIVER BELOW SELDEN DAM NR EUTAW  AL',
    32.77778,
    -87.84056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLDA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14787003,
    'NAWI2',
    '05540130',
    'NCRFC',
    'IL',
    '7120004',
    'WEST BRANCH DU PAGE RIVER NEAR NAPERVILLE  IL',
    41.72056,
    -88.13194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NAWI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14786891,
    'DEBI2',
    '05540160',
    'NCRFC',
    'IL',
    '7120004',
    'EAST BRANCH DU PAGE RIVER NEAR DOWNERS GROVE  IL',
    41.83167,
    -88.04778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DEBI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 476941,
    'JNPF1',
    '02367310',
    'SERFC',
    'FL',
    '3140102',
    'JUNIPER CREEK AT STATE HWY 85 NR NICEVILLE  FLA.',
    30.55722,
    -86.51944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'JNPF1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10854075,
    'WLFA1',
    '02378170',
    'SERFC',
    'AL',
    '3140107',
    'WOLF CREEK BELOW FOLEY  ALA',
    30.38806,
    -87.65278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WLFA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21250038,
    'FRMK1',
    '07138075',
    'ABRFC',
    'KS',
    '11030001',
    'FARMERS DITCH NR DEERFIELD  KS',
    37.99806,
    -101.06028
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FRMK1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6116268,
    'XBRM3',
    '01111212',
    'NERFC',
    'MA',
    '1090003',
    'BLACKSTONE RIVER  RT 122 BRIDGE NEAR UXBRIDGE  MA',
    42.05472,
    -71.61694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'XBRM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6148415,
    'WTHC3',
    '01124151',
    'NERFC',
    'CT',
    '1100001',
    'QUINEBAUG RIVER AT WEST THOMPSON  CT',
    41.94357,
    -71.8996
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WTHC3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3473151,
    'WPCM4',
    '04106400',
    'NCRFC',
    'MI',
    '4050003',
    'WEST FORK PORTAGE CREEK AT KALAMAZOO  MI',
    42.24444,
    -85.61444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WPCM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2613498,
    'EWAN6',
    '01422747',
    'MARFC',
    'NY',
    '2040101',
    'EAST BROOK EAST OF WALTON NY',
    42.17267,
    -75.12192
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'EWAN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17866812,
    'RNFN5',
    '08294195',
    'WGRFC',
    'NM',
    '13020101',
    'RIO NAMBE ABOVE NAMBE FALLS DAM NEAR NAMBE  NM',
    35.85006,
    -105.89444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RNFN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 15907963,
    'SCLA3',
    '09480000',
    'CBRFC',
    'AZ',
    '15050301',
    'SANTA CRUZ RIVER NEAR LOCHIEL  AZ',
    31.35528,
    -110.58889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCLA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 21458394,
    'PURA1',
    '02427830',
    'SERFC',
    'AL',
    '3150203',
    'PURSLEY CREEK ABOVE ESTELLE  AL',
    33.74028,
    -86.8125
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PURA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23659590,
    'HWLO3',
    '14034470',
    'NWRFC',
    'OR',
    '17070104',
    'WILLOW CREEK ABV WILLOW CR LAKE  NR HEPPNER  OR',
    45.34083,
    -119.51472
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HWLO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 947070191,
    'TDAO3',
    '14105700',
    'NWRFC',
    'OR',
    '17070105',
    'COLUMBIA RIVER AT THE DALLES  OR',
    45.60828,
    -121.18992
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TDAO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23780701,
    'BRBO3',
    '14179000',
    'NWRFC',
    'OR',
    '17090005',
    'BREITENBUSH R ABV FRENCH CR NR DETROIT  OR.',
    44.75278,
    -122.12778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BRBO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 23923278,
    'MCLO3',
    '14337600',
    'NWRFC',
    'OR',
    '17100307',
    'ROGUE RIVER NEAR MCLEOD  OR',
    42.65556,
    -122.71389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCLO3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 398638,
    'NW570',
    '07194809',
    'ABRFC',
    'AR',
    '11110103',
    'Niokaska Creek at Township St at Fayetteville~ AR',
    36.08472,
    -94.13555
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW570'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 7764183,
    'NW722',
    '07257450',
    'ABRFC',
    'AR',
    '11110202',
    'East Fork Illinois Bayou nr Hector~ AR',
    35.54222,
    -92.93194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW722'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 494794,
    'NW572',
    '07160810',
    'ABRFC',
    'OK',
    '11050003',
    'Wildhorse Creek near Perkins~ Ok',
    35.99972,
    -97.11861
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW572'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1327159,
    'NW585',
    '09075400',
    'CBRFC',
    'CO',
    '14010004',
    'CASTLE CREEK AT ASPEN~ CO',
    39.18638,
    -106.84722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW585'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5867679,
    'NW674',
    '01104415',
    'NERFC',
    'MA',
    '1090001',
    'CAMBRIDGE RES.~ UNNAMED TRIB 2~ NR LEXINGTON~ MA',
    42.44055,
    -71.25416
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW674'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5867685,
    'NW675',
    '01104420',
    'NERFC',
    'MA',
    '1090001',
    'CAMBRIDGE RES.~ UNNAMED TRIB 3~ NR LEXINGTON~ MA',
    42.42361,
    -71.25416
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW675'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 26325150,
    'NW1003',
    '02098206',
    'SERFC',
    'NC',
    '3030002',
    'HAW RIVER NEAR MONCURE~ NC',
    35.62694,
    -79.05083
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1003'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 166756802,
    'NW1009',
    '02172558',
    'SERFC',
    'SC',
    '3050204',
    'SOUTH FORK EDISTO RIVER ABOVE SPRINGFIELD~SC',
    33.52527,
    -81.40666
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1009'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 166414852,
    'NW1005',
    '08093360',
    'WGRFC',
    'TX',
    '12060202',
    'Aquilla Ck abv Aquilla~ TX',
    31.89805,
    -97.20333
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1005'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6191738,
    'LMTN6',
    '01363556',
    'NERFC',
    'NY',
    '2020006',
    'ESOPUS CREEK NEAR LOMONTVILLE NY',
    41.87933,
    -74.14536
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LMTN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12096273,
    'DOLT2',
    '08449100',
    'WGRFC',
    'TX',
    '13040302',
    'Dolan Ck abv Devils River nr Comstock~ TX',
    29.88833,
    -100.9898
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DOLT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12162504,
    'LNKW3',
    '040869416',
    'NCRFC',
    'WI',
    '4040003',
    'LINCOLN CREEK @ SHERMAN BOULEVARD AT MILWAUKEE  WI',
    43.09713889,
    -87.96725
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNKW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5878903,
    'MTTM3',
    '01105917',
    'NERFC',
    'MA',
    '1090002',
    'MATTAPOISETT RIVER NEAR MATTAPOISETT  MA',
    41.66260446,
    -70.8383696
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MTTM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 17806546,
    'TIJN5',
    '08330600',
    'WGRFC',
    'NM',
    '13020203',
    'TIJERAS ARROYO NR ALBUQUERQUE  NM',
    35.00194444,
    -106.6575
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TIJN5'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8942509,
    'LOGN2',
    '10336740',
    'CNRFC',
    'NV',
    '16050101',
    'LOGAN HOUSE CK NR GLENBROOK  NV',
    39.06657478,
    -119.9354605
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LOGN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13296760,
    'COKW3',
    '05430150',
    'NCRFC',
    'WI',
    '7090001',
    'BADFISH CREEK NEAR COOKSVILLE  WI',
    42.83333785,
    -89.19678199
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'COKW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14771374,
    'ELGI2',
    '05550500',
    'NCRFC',
    'IL',
    '7120006',
    'POPLAR CREEK AT ELGIN  IL',
    42.0261111,
    -88.2555556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ELGI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9755434,
    'GBRN7',
    '02142914',
    'SERFC',
    'NC',
    '3050101',
    'GUM BRANCH NEAR THRIFT  NC',
    35.29944444,
    -80.94638889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GBRN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 9731488,
    'LHCN7',
    '02146470',
    'SERFC',
    'NC',
    '3050103',
    'LITTLE HOPE CR AT SENECA PLACE AT CHARLOTTE  NC',
    35.16444444,
    -80.8530556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LHCN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 10190044,
    'MNLK2',
    '03400800',
    'OHRFC',
    'KY',
    '5130101',
    'MARTINS FORK NEAR SMITH  KY',
    36.75230974,
    -83.2574012
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MNLK2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5867391,
    'SAGM3',
    '01102345',
    'NERFC',
    'MA',
    '1090001',
    'SAUGUS RIVER AT SAUGUS IRONWORKS AT SAUGUS  MA',
    42.4695404,
    -71.0069954
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SAGM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5867393,
    'WCHM3',
    '01102500',
    'NERFC',
    'MA',
    '1090001',
    'ABERJONA RIVER AT WINCHESTER  MA',
    42.4474568,
    -71.1380816
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WCHM3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6128415,
    'PAWR1',
    '01113895',
    'NERFC',
    'RI',
    '1090003',
    'BLACKSTONE R AT ROOSEVELT ST AT PAWTUCKET RI',
    41.88871088,
    -71.38144468
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PAWR1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11689716,
    'WDLM2',
    '01589316',
    'MARFC',
    'MD',
    '2060003',
    'DEAD RUN TRIBUTARY NEAR WOODLAWN  MD',
    39.31891667,
    -76.7506944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WDLM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22336379,
    'CPPM2',
    '01649190',
    'MARFC',
    'MD',
    '2070010',
    'PAINT BRANCH NEAR COLLEGE PARK  MD',
    39.03313889,
    -76.96427778
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPPM2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 11573918,
    'NAHN7',
    '02091000',
    'SERFC',
    'NC',
    '3020203',
    'NAHUNTA SWAMP NEAR SHINE  NC',
    35.48889,
    -77.80611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NAHN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 16035106,
    'NISN1',
    '06463720',
    'MBRFC',
    'NE',
    '10150004',
    'Niobrara River at Mariaville  Nebr.',
    42.78056,
    -99.33972
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NISN1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8520739,
    'BKSV2',
    '02011460',
    'MARFC',
    'VA',
    '2080201',
    'BACK CREEK NEAR SUNRISE  VA',
    38.24528,
    -79.76889
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BKSV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8331928,
    'MCVT2',
    '08031000',
    'WGRFC',
    'TX',
    '12010005',
    'Cow Bayou nr Mauriceville  TX',
    30.18611,
    -93.90833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MCVT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1508121,
    'CYRT2',
    '08068780',
    'WGRFC',
    'TX',
    '12040102',
    'Little Cypress Ck nr Cypress  TX',
    30.01583,
    -95.69722
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CYRT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8520739,
    'SUNV2',
    '02011470',
    'MARFC',
    'VA',
    '2080201',
    'BACK CREEK AT SUNRISE  VA',
    38.19028,
    -79.81194
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SUNV2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2965566,
    'GDRM8',
    '06191000',
    'MBRFC',
    'MT',
    '10070001',
    'Gardner River near Mammoth  YNP',
    44.99234,
    -110.69098
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GDRM8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 3247574,
    'WKLN6',
    '01349810',
    'NERFC',
    'NY',
    '2020005',
    'WEST KILL NEAR WEST KILL NY',
    42.23028,
    -74.39306
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WKLN6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2306737,
    'FOGG1',
    '02343241',
    'SERFC',
    'GA',
    '3130004',
    'CHATTAHOOCHEE R BELOW WFG DAM AB FT. GAINES  GA',
    31.62444,
    -85.06528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FOGG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8834930,
    'CPFN7',
    '02105769',
    'SERFC',
    'NC',
    '3030005',
    'CAPE FEAR R AT LOCK #1 NR KELLY  NC',
    34.40444,
    -78.29361
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CPFN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18634030,
    'GNSA1',
    '02447026',
    'SERFC',
    'AL',
    '3160106',
    'TOMBIGBEE R BLW HEFLIN L&D NR GAINESVILLE  ALA',
    32.84806,
    -88.15611
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GNSA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18229923,
    'TODA1',
    '02465005',
    'SERFC',
    'AL',
    '3160112',
    'BLACK WARRIOR R BL OLIVER L&D NEAR TUSCALOOSA  AL.',
    33.20917,
    -87.59
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TODA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18540820,
    'DLDA1',
    '02467001',
    'SERFC',
    'AL',
    '3160201',
    'TOMBIGBEE RIVER BL DEMOPOLIS L&D NEAR COATOPA AL',
    32.51944,
    -87.87833
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DLDA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14828151,
    'HRCN8',
    '05059300',
    'NCRFC',
    'ND',
    '9020204',
    'SHEYENNE R AB SHEYENNE R DIVERSION NR HORACE  ND',
    46.74639,
    -96.92694
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'HRCN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13463117,
    'LAFI2',
    '05535000',
    'NCRFC',
    'IL',
    '7120003',
    'SKOKIE RIVER AT LAKE FOREST  IL',
    42.2325,
    -87.84528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LAFI2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 1128277,
    'TXST2',
    '07344210',
    'LMRFC',
    'TX',
    '11140302',
    'Sulphur Rv nr Texarkana  TX',
    33.30417,
    -94.15139
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TXST2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 12034339,
    'LNDN7',
    '02152474',
    'SERFC',
    'NC',
    '3050105',
    'FIRST BROAD RIVER AT LAWNDALE  NC',
    35.41528,
    -81.56167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNDN7'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 6501484,
    'RKMG1',
    '02394820',
    'SERFC',
    'GA',
    '3150104',
    'EUHARLEE CREEK AT US 278~ AT ROCKMART~ GA',
    33.99861,
    -85.0525
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RKMG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 19771685,
    'MUEC1',
    '11262900',
    'CNRFC',
    'CA',
    '18040001',
    'MUD SLOUGH NR GUSTINE CA',
    37.2625,
    -120.90556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MUEC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8941685,
    'WDKC1',
    '10336676',
    'CNRFC',
    'CA',
    '16050101',
    'WARD C AT HWY 89 NR TAHOE PINES CA',
    39.13222,
    -120.15667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WDKC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 18239649,
    'CLBA1',
    '02428401',
    'SERFC',
    'AL',
    '3150204',
    'ALABAMA RIVER BEL CLAIB. L&D NR MONROEVILLE  AL.',
    31.61333,
    -87.55056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLBA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14827685,
    'GOLN8',
    '05058980',
    'NCRFC',
    'ND',
    '9020204',
    'SHEYENNE RIVER ON GOL ROAD NEAR KINDRED  ND',
    46.60306,
    -97.03222
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'GOLN8'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8942879,
    'INCN2',
    '10336700',
    'CNRFC',
    'NV',
    '16050101',
    'INCLINE CK NR CRYSTAL BAY  NV',
    39.24028,
    -119.94389
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'INCN2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 22994743,
    'BCAW1',
    '12396500',
    'NWRFC',
    'WA',
    '17010216',
    'PEND OREILLE RIVER BELOW BOX CANYON NEAR IONE  WA',
    48.78111,
    -117.41528
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCAW1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 2306737,
    'FPHA1',
    '023432415',
    'SERFC',
    'AL',
    '3130004',
    'CHATTAHOOCHEE R .36 MI DS WFG DAM NR FT GAINES  GA',
    31.62139,
    -85.06
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'FPHA1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'BCWA3',
    '09502830',
    'CBRFC',
    'AZ',
    NULL,
    'BIG CHINO WASH AT PAULDEN AZ',
    0.0,
    0.0
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'BCWA3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ONCN4',
    '01411320',
    'MARFC',
    'NJ',
    NULL,
    'GREAT EGG HARBOR BAY AT OCEAN CITY NJ',
    39.285833,
    -74.575556
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ONCN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'RUSG1',
    '02388985',
    'SERFC',
    'GA',
    NULL,
    'RUSSELL CRK 0.3 MI DS HEAD LAKE NR DAWSONVILLE GA',
    34.398611,
    -84.056667
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'RUSG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SCKG1',
    '03566700',
    'LMRFC',
    'GA',
    NULL,
    'SOUTH CHICKAMAUGA CREEK AT RINGGOLD GA',
    34.9186,
    -85.1255
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SCKG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'STCG1',
    '02207130',
    'SERFC',
    'GA',
    NULL,
    'STONE MTN CR AT SILVER HILL RD NEAR STONE MTN GA',
    33.826111,
    -84.165278
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'STCG1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'ENOI4',
    '06817300',
    'MBRFC',
    'IA',
    NULL,
    'EAST NODAWAY RIVER AT HIGHWAY 2 NEAR CLARINDA IA',
    40.73,
    -94.98
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'ENOI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TRKI4',
    '06811760',
    'MBRFC',
    'IA',
    NULL,
    'TARKIO RIVER NEAR ELLIOTT IA',
    41.1017,
    -95.1025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TRKI4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WHSI3',
    '03351350',
    'OHRFC',
    'IN',
    NULL,
    'WHITE RIVER AT 16TH STREET AT INDIANAPOLIS IN',
    39.788611,
    -86.1975
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WHSI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WRSI3',
    '03348502',
    'OHRFC',
    'IN',
    NULL,
    'WHITE RIVER NEAR STRAWTOWN IN',
    40.125556,
    -85.9675
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WRSI3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'DRMM4',
    '04044003',
    'NCRFC',
    'MI',
    NULL,
    'DEAD RIVER AT MARQUETTE MI',
    46.5717,
    -87.4103
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'DRMM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MRMM4',
    '04044755',
    'NCRFC',
    'MI',
    NULL,
    'MINERS RIVER NR MUNISING MI',
    46.4878,
    -86.5406
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MRMM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'WBMM4',
    '04044090',
    'NCRFC',
    'MI',
    NULL,
    'WHETSTONE BROOK AT ALTAMONT ST AT MARQUETTE MI',
    46.5389,
    -87.4039
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WBMM4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'TCGM6',
    '02481251',
    'LMRFC',
    'MS',
    NULL,
    'TURKEY CREEK AT AIRPORT ROAD AT GULFPORT MS',
    30.418333,
    -89.099167
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TCGM6'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'LNPN4',
    '01388700',
    'MARFC',
    'NJ',
    NULL,
    'BEAVER DAM BROOK AT LINCOLN PARK NJ',
    40.924722,
    -74.3025
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'LNPN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'MGTN4',
    '01411330',
    'MARFC',
    'NJ',
    NULL,
    'BEACH THOROFARE AT MARGATE NJ',
    39.3375,
    -74.513056
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'MGTN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'PELN4',
    '01367715',
    'NERFC',
    'NJ',
    NULL,
    'WALLKILL R AT SCOTT ROAD AT FRANKLIN NJ',
    41.16,
    -74.68
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'PELN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT NULL,
    'SIGN4',
    '01389010',
    'MARFC',
    'NJ',
    NULL,
    'PASSAIC RIVER AT I-80 AT SINGAC NJ',
    40.8944,
    -74.2661
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'SIGN4'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 940190148,
    'NW1012',
    '06709910',
    'MBRFC',
    'CO',
    '10190002',
    'DUTCH CR AT PLATTE CANYON DRIVE NEAR LITTLETON~ CO',
    39.61,
    -105.03388
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NW1012'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 13083643,
    'CLXW3',
    '05367500',
    'NCRFC',
    'WI',
    '7050007',
    'RED CEDAR RIVER NEAR COLFAX  WI',
    45.05305556,
    -91.7119444
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'CLXW3'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 5591642,
    'NELT2',
    '08107950',
    'WGRFC',
    'TX',
    '12070204',
    'N Elm Ck at Rosebud Rd nr Meeks~ TX',
    31.01307,
    -97.11008
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'NELT2'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 8245886,
    'TLKC1',
    '11525655',
    'CNRFC',
    'CA',
    '18010211',
    'TRINITY R BL LIMEKILN GULCH NR DOUGLAS CITY CA',
    40.67278,
    -122.91944
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'TLKC1'
);

INSERT INTO wres.Feature (
    comid,
    lid,
    gage_id,
    region,
    state,
    huc,
    feature_name,
    latitude,
    longitude
)
SELECT 14705174,
    'WIRW3',
    '05400760',
    'NCRFC',
    'WI',
    NULL,
    'WISCONSIN RIVEAR AT WISCONSIN RAPIDS~ WI',
    44.39219,
    -89.827
WHERE NOT EXISTS (
    SELECT 1
    FROM wres.Feature F
    WHERE F.lid = 'WIRW3'
);
