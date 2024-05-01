package wres.reading.waterml;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.datamodel.time.TimeSeries;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;

/**
 * Tests the {@link WatermlReader}.
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

class WatermlReaderTest
{
    private DataSource fakeSource;
    private String watermlString;

    @BeforeEach
    void setup()
    {
        // Use a fake URI with an NWIS-like string as this is used to trigger the
        // identification of an instantaneous time-scale
        URI fakeUri = URI.create( "https://fake.usgs.gov/nwis/iv" );
        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        this.fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.LEFT,
                                         null );

        this.watermlString =
                """
                        {
                          "name": "ns1:timeSeriesResponseType",
                          "declaredType": "org.cuahsi.waterml.TimeSeriesResponseType",
                          "scope": "javax.xml.bind.JAXBElement$GlobalScope",
                          "value": {
                            "queryInfo": {
                              "queryURL": "http://nwis.waterservices.usgs.gov/nwis/ivendDT=2020-10-09T22%3A00%3A00Z&format=json&parameterCd=00060&sites=03272700%2C03280700&startDT=2020-10-09T01%3A00%3A01Z",
                              "criteria": {
                                "locationParam": "[ALL:03272700, ALL:03280700]",
                                "variableParam": "[00060]",
                                "timeParam": {
                                  "beginDateTime": "2020-10-09T01:00:01.000",
                                  "endDateTime": "2020-10-09T22:00:00.000"
                                },
                                "parameter": []
                              },
                              "note": [
                                {
                                  "value": "[ALL:03272700, ALL:03280700]",
                                  "title": "filter:sites"
                                },
                                {
                                  "value": "[mode=RANGE, modifiedSince=null] interval={INTERVAL[2020-10-09T01:00:01.000Z/2020-10-09T22:00:00.000Z]}",
                                  "title": "filter:timeRange"
                                },
                                {
                                  "value": "methodIds=[ALL]",
                                  "title": "filter:methodId"
                                },
                                {
                                  "value": "2021-01-29T16:22:32.874Z",
                                  "title": "requestDT"
                                },
                                {
                                  "value": "30afc990-624e-11eb-acf5-4cd98f86fad9",
                                  "title": "requestId"
                                },
                                {
                                  "value": "Provisional data are subject to revision. Go to http://waterdata.usgs.gov/nwis/help/?provisional for more information.",
                                  "title": "disclaimer"
                                },
                                {
                                  "value": "nadww02",
                                  "title": "server"
                                }
                              ]
                            },
                            "timeSeries": [
                              {
                                "sourceInfo": {
                                  "siteName": "Sevenmile Creek at Camden OH",
                                  "siteCode": [
                                    {
                                      "value": "03272700",
                                      "network": "NWIS",
                                      "agencyCode": "USGS"
                                    }
                                  ],
                                  "timeZoneInfo": {
                                    "defaultTimeZone": {
                                      "zoneOffset": "-05:00",
                                      "zoneAbbreviation": "EST"
                                    },
                                    "daylightSavingsTimeZone": {
                                      "zoneOffset": "-04:00",
                                      "zoneAbbreviation": "EDT"
                                    },
                                    "siteUsesDaylightSavingsTime": true
                                  },
                                  "geoLocation": {
                                    "geogLocation": {
                                      "srs": "EPSG:4326",
                                      "latitude": 39.6292196,
                                      "longitude": -84.6443964
                                    },
                                    "localSiteXY": []
                                  },
                                  "note": [],
                                  "siteType": [],
                                  "siteProperty": [
                                    {
                                      "value": "ST",
                                      "name": "siteTypeCd"
                                    },
                                    {
                                      "value": "05080002",
                                      "name": "hucCd"
                                    },
                                    {
                                      "value": "39",
                                      "name": "stateCd"
                                    },
                                    {
                                      "value": "39135",
                                      "name": "countyCd"
                                    }
                                  ]
                                },
                                "variable": {
                                  "variableCode": [
                                    {
                                      "value": "00060",
                                      "network": "NWIS",
                                      "vocabulary": "NWIS:UnitValues",
                                      "variableID": 45807197,
                                      "default": true
                                    }
                                  ],
                                  "variableName": "Streamflow, ft&#179;/s",
                                  "variableDescription": "Discharge, cubic feet per second",
                                  "valueType": "Derived Value",
                                  "unit": {
                                    "unitCode": "ft3/s"
                                  },
                                  "options": {
                                    "option": [
                                      {
                                        "name": "Statistic",
                                        "optionCode": "00000"
                                      }
                                    ]
                                  },
                                  "note": [],
                                  "noDataValue": -999999,
                                  "variableProperty": [],
                                  "oid": "45807197"
                                },
                                "values": [
                                  {
                                    "value": [
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T20:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T20:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T20:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T21:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T21:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T21:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T21:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T22:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T22:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T22:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T22:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T23:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T23:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T23:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.22",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-08T23:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T00:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T00:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T00:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T00:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T01:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.15",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T01:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T01:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T01:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T02:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T02:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T02:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T02:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T03:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T03:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T03:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T03:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.14",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T04:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T04:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T04:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T04:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T05:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T05:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T05:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.13",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T05:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.20",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T06:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.20",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T06:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.20",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T06:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T06:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T07:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T07:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T07:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T07:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T08:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T08:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T08:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T08:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.19",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T09:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T09:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T09:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T09:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.25",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T10:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.25",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T10:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.25",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T10:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.25",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T10:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.25",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T11:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T11:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T11:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.18",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T11:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.17",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T12:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.17",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T12:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T12:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.17",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T12:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T13:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T13:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.17",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T13:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T13:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T14:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T14:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T14:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.24",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T14:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T15:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T15:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.23",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T15:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T15:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T16:00:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T16:15:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T16:30:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T16:45:00.000-04:00"
                                      },
                                      {
                                        "value": "1.16",
                                        "qualifiers": [
                                          "P"
                                        ],
                                        "dateTime": "2020-10-09T17:00:00.000-04:00"
                                      }
                                    ],
                                    "qualifier": [
                                      {
                                        "qualifierCode": "P",
                                        "qualifierDescription": "Provisional data subject to revision.",
                                        "qualifierID": 0,
                                        "network": "NWIS",
                                        "vocabulary": "uv_rmk_cd"
                                      }
                                    ],
                                    "qualityControlLevel": [],
                                    "method": [
                                      {
                                        "methodDescription": "",
                                        "methodID": 110552
                                      }
                                    ],
                                    "source": [],
                                    "offset": [],
                                    "sample": [],
                                    "censorCode": []
                                  }
                                ],
                                "name": "USGS:03272700:00060:00000"
                              },
                              {
                                "sourceInfo": {
                                  "siteName": "CUTSHIN CREEK AT WOOTON, KY",
                                  "siteCode": [
                                    {
                                      "value": "03280700",
                                      "network": "NWIS",
                                      "agencyCode": "USGS"
                                    }
                                  ],
                                  "timeZoneInfo": {
                                    "defaultTimeZone": {
                                      "zoneOffset": "-05:00",
                                      "zoneAbbreviation": "EST"
                                    },
                                    "daylightSavingsTimeZone": {
                                      "zoneOffset": "-04:00",
                                      "zoneAbbreviation": "EDT"
                                    },
                                    "siteUsesDaylightSavingsTime": true
                                  },
                                  "geoLocation": {
                                    "geogLocation": {
                                      "srs": "EPSG:4326",
                                      "latitude": 37.1650931,
                                      "longitude": -83.3079564
                                    },
                                    "localSiteXY": []
                                  },
                                  "note": [],
                                  "siteType": [],
                                  "siteProperty": [
                                    {
                                      "value": "ST",
                                      "name": "siteTypeCd"
                                    },
                                    {
                                      "value": "05100202",
                                      "name": "hucCd"
                                    },
                                    {
                                      "value": "21",
                                      "name": "stateCd"
                                    },
                                    {
                                      "value": "21131",
                                      "name": "countyCd"
                                    }
                                  ]
                                },
                                "variable": {
                                  "variableCode": [
                                    {
                                      "value": "00060",
                                      "network": "NWIS",
                                      "vocabulary": "NWIS:UnitValues",
                                      "variableID": 45807197,
                                      "default": true
                                    }
                                  ],
                                  "variableName": "Streamflow, ft&#179;/s",
                                  "variableDescription": "Discharge, cubic feet per second",
                                  "valueType": "Derived Value",
                                  "unit": {
                                    "unitCode": "ft3/s"
                                  },
                                  "options": {
                                    "option": [
                                      {
                                        "name": "Statistic",
                                        "optionCode": "00000"
                                      }
                                    ]
                                  },
                                  "note": [],
                                  "noDataValue": -999999,
                                  "variableProperty": [],
                                  "oid": "45807197"
                                },
                                "values": [
                                  {
                                    "value": [
                                      {
                                        "value": "8.04",
                                        "qualifiers": [
                                          "P",
                                          "e"
                                        ],
                                        "dateTime": "2020-10-09T05:00:00.000-04:00"
                                      },
                                      {
                                        "value": "10.6",
                                        "qualifiers": [
                                          "P",
                                          "e"
                                        ],
                                        "dateTime": "2020-10-09T17:00:00.000-04:00"
                                      }
                                    ],
                                    "qualifier": [
                                      {
                                        "qualifierCode": "e",
                                        "qualifierDescription": "Value has been estimated.",
                                        "qualifierID": 0,
                                        "network": "NWIS",
                                        "vocabulary": "uv_rmk_cd"
                                      },
                                      {
                                        "qualifierCode": "P",
                                        "qualifierDescription": "Provisional data subject to revision.",
                                        "qualifierID": 1,
                                        "network": "NWIS",
                                        "vocabulary": "uv_rmk_cd"
                                      }
                                    ],
                                    "qualityControlLevel": [],
                                    "method": [
                                      {
                                        "methodDescription": "",
                                        "methodID": 59903
                                      }
                                    ],
                                    "source": [],
                                    "offset": [],
                                    "sample": [],
                                    "censorCode": []
                                  }
                                ],
                                "name": "USGS:03280700:00060:00000"
                              }
                            ]
                          },
                          "nil": false,
                          "globalScope": true,
                          "typeSubstituted": false
                        }""";
    }

    @Test
    void testReadObservationsFromFileSourceResultsInTwoTimeSeriesWithExpectedShape() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WaterML file to an in-memory file system
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", "test.json" );
            Path watermlPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( watermlPath ) )
            {
                writer.append( this.watermlString );
            }

            WatermlReader reader = WatermlReader.of();

            try ( InputStream inputStream = new BufferedInputStream( Files.newInputStream( watermlPath ) );
                  Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
            {
                List<TimeSeries<Double>> timeSeries = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                                 .toList();

                assertEquals( 2,
                              timeSeries.size(),
                              "Expected two timeseries." );
                assertAll( () -> assertNotNull( timeSeries.get( 0 )
                                                          .getMetadata(),
                                                "Expected the first timeseries to have metadata." ),
                           () -> assertNotNull( timeSeries.get( 1 )
                                                          .getMetadata(),
                                                "Expected the second timeseries to have metadata." ),
                           () -> assertEquals( 84,
                                               timeSeries.get( 0 )
                                                         .getEvents()
                                                         .size(),
                                               "Expected the first timeseries to have 84 values." ),
                           () -> assertEquals( 2,
                                               timeSeries.get( 1 )
                                                         .getEvents()
                                                         .size(),
                                               "Expected the second timeseries to have 2 values." ) );
            }

            // Clean up
            if ( Files.exists( watermlPath ) )
            {
                Files.delete( watermlPath );
            }
        }
    }
}
