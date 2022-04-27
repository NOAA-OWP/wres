module wres.messages
{
    requires java.base;
    requires com.google.protobuf;
    requires org.slf4j;
    exports wres.messages.generated;
    exports wres.messages;
}
