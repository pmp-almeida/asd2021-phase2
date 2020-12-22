package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptOKMessage extends PaxosMessage {

    public final static short MSG_ID = 105;

    public AcceptOKMessage(int instance, UUID opId, int seqNum) {
        super(MSG_ID, instance, opId, seqNum);
    }

    @Override
    public String toString() {
        return "AcceptOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", seqNum=" + sequenceNumber +
                '}';
    }

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<AcceptOKMessage>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeInt(msg.sequenceNumber);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int sequenceNumber = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            return new AcceptOKMessage(instance, opId, sequenceNumber);
        }
    };
}
