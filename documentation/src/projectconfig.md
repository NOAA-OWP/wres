# Project configuration

This document is for users who wish to run a project using a project
configuration file and asking WRES to execute that project.

## Simple example file

<?xml version="1.0" encoding="UTF-8"?>
<project>
  <inputs>
    <left>
      <type>observations</type>
      <source recursive="true" all="true" format="PI-XML">/media/sf_userprofile/hefs_data/resources/DRRC2SQIN.xml</source>
      <variable>QINE</variable>
    </left>
    <right>
      <type>ensemble forecasts</type>
      <source format="PI-XML">/media/sf_userprofile/hefs_data/resources/forecasts/</source>
      <variable>SQIN</variable>
    </right>
  </inputs>
  <conditions>
    <dates earliest="1980-01-01T00:00" latest="2010-12-23T23:59"/>
    <feature>
        <location lid="DRRC2" name="DOLORES - RICO, BLO"/>
    </feature>
  </conditions>
  <pair>
    <unit>CMS</unit>
  </pair>
  <outputs>
    <metric label="ME">mean_error</metric>
    <metric>correlation_coefficient</metric>
    <metric>brier_skill_score</metric>
    <destination type="graphic"><path>/tmp/</path>
      <graphical width="1920" height="1080">
        <plotType>LEAD_THRESHOLD</plotType>
        </graphical>
    </destination>
  </outputs>
</project>
