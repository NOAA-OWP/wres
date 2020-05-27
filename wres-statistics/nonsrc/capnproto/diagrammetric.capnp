@0x8fb97cd77ecbe959;

using Java = import "java.capnp";
using MetricName = import "metricname.capnp".MetricName;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DiagramMetricOuter");

struct DiagramMetric
{
    # A message that encapsulates a diagram metric, such as a reliability 
    # diagram.

    name @0 :MetricName;    
    # Metric name. For example, if the diagram is the reliability diagram, 
    # which includes a sharpness plot, then the metric name is the reliability 
    # diagram and the metric has three DiagramMetricComponent, one for the 
    # forecast probability, one for the observed relative frequency and one for 
    # the sample size or sharpness.

    components @1 :DiagramMetricComponent;
    # The metric components.

    struct DiagramMetricComponent
    {
        # A diagram component.

        enum DiagramComponentName 
        {
            # Name of a diagram component.

            unknown @0;
            probabilityOfFalseDetection @1;
            probabilityOfDetection @2;
            rankOrder @3;
            forecastProbability @4;
            observedRelativeFrequency @5;
            observedQuantiles @6;
            predictedQuantiles @7;
            sampleSize @8;
        }

        name @0 :DiagramComponentName;               

        units @1 :Text;
        # Units of the component.
        
        optimum @2 :Float64; 
        # Optimum value, where application (e.g., zero for errors).

        minimum @3 :Float64;
        # Lower limit of the component, which can help in consumption (e.g., in 
        # setting plot bounds). When undefined, the statistic data dictates the 
        # limits of the range of the diagram.
        
        maximum @4 :Float64;
        # Upper limit of the component, which can help in consumption (e.g., in 
        # setting plot bounds). When undefined, the statistic data dictates the 
        # limits of the range of the diagram.
    }

}