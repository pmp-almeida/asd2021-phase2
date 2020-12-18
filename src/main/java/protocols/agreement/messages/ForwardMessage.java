package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class ForwardMessage extends PaxosMessage {

    public final static short MSG_ID = 107;

    public ForwardMessage(int instance, UUID opId) {
        this(instance, opId, new byte[0]);
    }

    public ForwardMessage(int instance, UUID opId, int seqNum) {
        this(instance, opId, new byte[0], seqNum);
    }

    public ForwardMessage(int instance, UUID opId, byte[] op) {
        this(instance, opId, op, -1);
    }

    public ForwardMessage(int instance, UUID opId, byte[] op, int seqNum) {
        super(MSG_ID, instance, opId, op, seqNum);
    }

    @Override
    public String toString() {
        return "ForwardMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<ForwardMessage> serializer = new ISerializer<ForwardMessage>() {
        @Override
        public void serialize(ForwardMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.sequenceNumber);
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public ForwardMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            int sequenceNumber = in.readInt();
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new ForwardMessage(instance, opId, op, sequenceNumber);
        }
    };
}
