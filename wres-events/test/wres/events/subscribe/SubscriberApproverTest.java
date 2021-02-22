package wres.events.subscribe;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import wres.statistics.generated.Consumer.Format;

/**
 * Tests the {@link SubscriberApprover}.
 * 
 * @author james.brown@hydrosolved.com
 */

class SubscriberApproverTest
{

    @Test
    void testSubscriberApproverIsPermissive()
    {
        // Empty approver is permissive
        SubscriberApprover approver = new SubscriberApprover.Builder().build();

        assertAll( () -> assertTrue( approver.isApproved( Format.CSV, "goober" ) ),
                   () -> assertTrue( approver.isApproved( Format.PNG, "doubleGoober" ) ) );
    }

    @Test
    void testSubscriberApproverApprovesExpectedPngSubscriberAndAnyNetcdfSubscriber()
    {
        SubscriberApprover approver = new SubscriberApprover.Builder().addApprovedSubscriber( Format.PNG, "forReal" )
                                                                      .build();

        assertAll( () -> assertTrue( approver.isApproved( Format.PNG, "forReal" ) ),
                   () -> assertTrue( approver.isApproved( Format.NETCDF, "doubleGoober" ) ) );
    }

    @Test
    void testSubscriberApproverDoesNotApproveMissingProtobufSubscriber()
    {
        SubscriberApprover approver = new SubscriberApprover.Builder().addApprovedSubscriber( Format.PROTOBUF,
                                                                                              "alsoForReal" )
                                                                      .addApprovedSubscriber( Format.PROTOBUF,
                                                                                              "oneMoreForReal" )
                                                                      .build();

        assertAll( () -> assertFalse( approver.isApproved( Format.PROTOBUF, "forReal" ) ),
                   () -> assertTrue( approver.isApproved( Format.PROTOBUF, "alsoForReal" ) ),
                   () -> assertTrue( approver.isApproved( Format.PROTOBUF, "oneMoreForReal" ) ) );
    }

}
