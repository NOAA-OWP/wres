# Project configuration

This document is for users who wish to run a project using a project
configuration file and asking WRES to execute that project.

## Simple example file

    <?xml version="1.0" encoding="UTF-8"?>
    <project>

        <inputs>
            <left>
                <type>observations</type>
                <source recursive="true" all="true" format="PI-XML">c:/resources/DRRC2SQIN.xml</source>
                <variable>QINE</variable>
                <features><location lid="DRRC2" /></features>
            </left>
            <right>
                <type>ensemble forecasts</type>
                <source format="PI-XML">c:/resources/forecasts/</source>
                <variable>SQIN</variable>
                <features><location lid="DRRC2" /></features>
            </right>
        </inputs>

        <conditions>
            <dates earliest="1980-01-01T00:00" latest="2010-12-23T23:59"/>
            <features>
                <location lid="DRRC2" />
            </features>
        </conditions>

        <pair>
            <unit>CMS</unit>
            <desiredTimeAggregation>
                <function>avg</function>
                <period>1</period>
                <unit>hour</unit>
            </desiredTimeAggregation>
        </pair>

        <outputs>

            <probabilityThresholds>
                <applyTo>left</applyTo>
                <commaSeparatedValues>0.25,0.5,0.75,0.9</commaSeparatedValues>
                <operator>greater than or equal to</operator>
            </probabilityThresholds>
 
            <metric><name>mean error</name></metric>
            <metric><name>brier score</name></metric>
            <metric><name>reliability diagram</name></metric>
            <metric><name>sample size</name></metric>

            <destination type="numeric">
                <path>c:/output</path>
            </destination>

            <destination type="graphic">
                <path>c:/output</path>
                <graphical width="600" height="400">
                    <plotType>lead threshold</plotType>
                </graphical>
            </destination>

        </outputs>

    </project>

## Project

The contents of &lt;project&gt; declare everything the WRES tool needs to be
able to run a verification on data. The order of the declarations in the project
do not necessarily correspond to the order of execution in the tool. The reasons
for this are to avoid duplication and to allow the system leeway for performance
optimizations. In other words, the &lt;project&gt; declares concrete goals as
well as anything else the tool needs in order to correctly compute those goals.

A project consists of inputs, conditions, pairs, and outputs. For the WRES tool
to successfully read the &lt;project&gt;, the order of the elements in the file
must be consistent. In other words, inputs come first, then conditions, then
pairs, then outputs. The same is true for all elements (e.g. &lt;anElement&gt;)
and attributes (e.g. name="value" inside elements). Despite the format of the
project requiring exact ordering of xml elements, the full project is a single
declaration of desired output and the order of the tags in the project do not
necessarily correspond to order of execution.

### Inputs

The contents of &lt;inputs&gt; declares the universes of input data that the
WRES tool works with to produce the desired outputs.

As of 2017-09-11, the data sources handled by WRES are files and directories
accessible from where the tool is run.

There are two required data sources and one optional. The &lt;left&gt; and
&lt;right&gt; are required, and &lt;baseline&gt; is optional. Left and right
typically correspond to observations and forecasts, respectively (but there is
no restriction on the type of data in any of the three data sources). The
reason for the names "left" and "right" is these are the names typically given
to the sides of a pair.

One can explicitly specify all files with one &lt;source&gt; per file, or for
convenience, one &lt;source&gt; per directory. Warning: if the contents of a
file or directory change, the outcomes of a run of the project will change. In
the case where the inputs do not change at all, the performance of a run of the
project should improve on the second run.

The inputs section is primarily descriptive, in other words, it describes the
input data as it is, rather than prescribing any transformations on the data.
In cases when the data format does not aptly describe the data, this is where
the user must fill in the gaps, so to speak.

### Conditions

The contents of &lt;conditions&gt; declares the conditions desired for pairing.
The pairs that have metrics applied to them will already have been filtered by
these conditions.

### Pairs

The contents of &lt;pair&gt; declares what the the pairs should look like before
metrics are applied to the pairs. The &lt;unit&gt; is required. This is the
desired unit that pairs will have. Both the left and the right data will be
converted to the unit declared, so that the metrics performed are performed
using this unit. The &lt;timeAggregation&gt; is required. This is the desired
time step that pairs will have as well as the way to aggregate to reach this
time step when needed.

### Outputs

The contents of &lt;outputs&gt; declares the kinds and formats of outputs that
should be created. At least one &lt;metric&gt; is required, and at least one
&lt;destination&gt;. The WRES tool supports a number of metrics which are
specified by &lt;name&gt;. A &lt;destination&gt; as of 2017-09-11 must declare
a &lt;path&gt; which is a writeable directory. As of 2017-09-11, the default
destination will be numeric CSV files by feature. When &lt;graphical&gt;
destinations are specified, make sure to use the type="graphical" attribute in
the destination element and then specify the &lt;graphical&gt; after the
&lt;path&gt;.
