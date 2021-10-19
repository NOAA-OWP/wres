# Project configuration

This document is for users who wish to run an evaluation project using WRES.
An example project configuration document for WRES 1.5 follows and is discussed
below the example.

## Example evaluation project configuration

    <?xml version="1.0" encoding="UTF-8"?>
    <project>

        <inputs>
            <left>
                <type>observations</type>
                <source>c:/resources/DRRC2SQIN.xml</source>
                <variable>QINE</variable>
            </left>
            <right>
                <type>ensemble forecasts</type>
                <source>c:/resources/forecasts/</source>
                <variable>SQIN</variable>
            </right>
        </inputs>

        <pair>
            <unit>m^3/s</unit>
            <feature left="DRRC2" right="DRRC2" />
            <dates earliest="1980-01-01T00:00:00Z" latest="2010-12-24T00:00:00Z"/>
            <issuedDates earliest="1980-01-01T00:00:00Z" latest="2010-12-20T00:00:00Z"/>
            <desiredTimeScale>
                <function>mean</function>
                <period>1</period>
                <unit>hours</unit>
            </desiredTimeScale>
        </pair>

        <metrics>
            <thresholds>
                <type>probability</type>
                <applyTo>left</applyTo>
                <commaSeparatedValues>0.25,0.5,0.75,0.9</commaSeparatedValues>
                <operator>greater than or equal to</operator>
            </thresholds>
 
            <metric><name>mean error</name></metric>
            <metric><name>brier score</name></metric>
            <metric><name>reliability diagram</name></metric>
            <metric><name>sample size</name></metric>
        </metrics>

        <outputs>
            <destination type="csv2" />
        </outputs>

    </project>

## Project

The contents of &lt;project&gt; declare everything the WRES software needs to
run an evaluation on data. The order of the declarations in the project
do not necessarily correspond to the order of execution in the tool. The reasons
for this are to avoid duplication and to allow the system leeway for performance
optimizations. In other words, the &lt;project&gt; declares concrete goals as
well as anything else the tool needs in order to correctly compute those goals.

An evaluation project consists of inputs, pairs, and metrics. For the WRES tool
to successfully read the &lt;project&gt;, the order of the elements in the
evaluation document must be consistent. In other words, inputs come first, then
pairs, then outputs. The same is true for all elements (e.g. &lt;anElement&gt;)
and attributes (e.g. name="value" inside elements). Despite the format of the
project requiring exact ordering of xml elements, the full project is a single
declaration of desired output and the order of the tags in the project do not
necessarily correspond to order of execution.

### Inputs

The contents of &lt;inputs&gt; declares the universes of input data that the
WRES tool works with to produce the desired outputs.

As of 2019-02-11, the data sources handled by WRES include files and directories
accessible from where the tool is run as well as convenience sources for several
web APIs.

There are two required data sources and one optional. The &lt;left&gt; and
&lt;right&gt; are required, and &lt;baseline&gt; is optional. Left and right
typically correspond to observations and forecasts, respectively (but there is
no restriction on the type of data in any of the three data sources). The
reason for the names "left" and "right" is these are the names typically given
to the sides of a pair.

One can explicitly specify all data with one &lt;source&gt; per file, or for
convenience, one &lt;source&gt; per directory. Warning: if the contents of a
file or directory change, the outcomes of a run of the evaluation will change.
In the case where the inputs do not change at all, the performance of a run of
the evaluation should improve on the second run.

The inputs section is primarily descriptive, in other words, it describes the
input data as it is, rather than prescribing any transformations on the data.
In cases when the data format does not aptly describe the data, this is where
the user must fill in the gaps, so to speak.

The inputs section also declares conditions that need to apply to a specific
side of a pair and could also independently be specified for another side of a
pair. For example, the exact variable name used in observations may not
perfectly match the forecasts variable name, therefore the variable tag is used
to declare which variable name from the left data will be compared to which
variable name from the right data.

### Pairs

The contents of &lt;pair&gt; declares what the the pairs should look like before
metrics are applied to the pairs. The &lt;unit&gt; is required. This is the
desired unit that pairs will have. Both the left and the right data will be
converted to the unit declared, so that the metrics performed are performed
using this unit. The &lt;desiredTimeScale&gt; is required. This is the desired
time scale that pairs will have as well as the function used to aggregate to
reach this time scale.

Conditions to be applied to both sides of the pairs are declared here as well.
The most common conditions to apply to an evaluation are dates and issuedDates.
The &lt;dates&gt; tag with "earliest" and "latest" attributes applies to valid
datetimes and/or observation datetimes, whereas the &lt;issuedDates&gt; tag with
"earliest" and "latest" attributes applies to forecast issued dates. When WRES
gets observation data from USGS' NWIS service, the dates are required. When WRES
gets forecast data from WRDS AHPS service, the issuedDates are required.

### Metrics

The contents of &lt;outputs&gt; declares the metrics that should be computed. At
least one &lt;metric&gt; is required. The WRES tool supports a number of metrics
which are specified by &lt;name&gt;.

### Outputs

Outputs is where one specifies the format of output. The example above is
sufficient and will produce comma separated data. An output directory will be
created in the system's temp directory when running as described in these docs.
To control the output directory, use Java System Property java.io.tmpdir
at launch, e.g. -Djava.io.tmpdir=/path/to/desired/output and within this
directory a new output directory will be created for each evaluation run.
