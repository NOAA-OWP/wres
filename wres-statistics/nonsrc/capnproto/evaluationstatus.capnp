@0xd3e700ffbe8e3709;

using Java = import "capnpJava/java.capnp";
using Timestamp = import "timestamp.capnp".Timestamp;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("EvaluationStatusOuter");

struct EvaluationStatus
{
    # A message that encapsulates the status of an evaluation that has been 
    # started. An evaluation identifier is required to connect an EvaluationStatus 
    # message to an Evaluation message. It is assumed that this identifier is 
    # packaged with the protocol; it is not provided inband. An EvaluationStatus 
    # message may record some or all of the EvaluationStatusEvent associated with an 
    # evaluation at the point of request. For example, it may record only those "new" 
    # events surfaced since the last request.

    evaluationStartTime @0 :Timestamp;
    evaluationEndTime @1 :Timestamp;
     
    enum CompletionStatus
    {
        # Captures the status of an evaluation that has been started.

    	unknown @0;
    	ongoing @1;
    	completeReportedSuccess @2;
    	completeReportedFailure @3;
    }
    
    completionStatus @2 :CompletionStatus;
    
    statusEvents @3 :List(EvaluationStatusEvent);
    # Zero or more caller-facing status events
    
    struct EvaluationStatusEvent
    {
        # A message that encapsulates an evaluation status event, such as
        # a warning to a user.

        enum StatusMessageType
        {   
            # The type of value linked to each box

            undefined @0;
            # Undefined status
            
            error @1;
            # An event that represents an error.

            warn @2;
            # An event that represents a warning.
        
            info @3;
            # A neutral information message.

            debug @4;
            # An event that represents a detailed level of caller-facing 
            # information, as distinct from developer-facing/logging
        }
        
        eventType @0 :StatusMessageType;

        eventMessage @1 :Text;
    }
}