package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PrepareMessage extends PaxosMessage {

    public final static short MSG_ID = 102;

    public PrepareMessage(int instance, UUID opId) {
        this(instance, opId, new byte[0]);
    }

    public PrepareMessage(int instance, UUID opId, int seqNum) {
        this(instance, opId, new byte[0], seqNum);
    }

    public PrepareMessage(int instance, UUID opId, byte[] op) {
        this(instance, opId, op, -1);
    }

    public PrepareMessage(int instance, UUID opId, byte[] op, int seqNum) {
        super(MSG_ID, instance, opId, op, seqNum);
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", seqNum=" + sequenceNumber +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.sequenceNumber);
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            int sequenceNumber = in.readInt();
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new PrepareMessage(instance, opId, op, sequenceNumber);
        }
    };
}
