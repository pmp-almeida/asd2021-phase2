package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PrepareOKMessage extends PaxosMessage {

    public final static short MSG_ID = 103;

    public PrepareOKMessage(int instance, UUID opId) {
        this(instance, opId, new byte[0]);
    }

    public PrepareOKMessage(int instance, UUID opId, int seqNum) {
        this(instance, opId, new byte[0], seqNum);
    }

    public PrepareOKMessage(int instance, UUID opId, byte[] op) {
        this(instance, opId, op, -1);
    }

    public PrepareOKMessage(int instance, UUID opId, byte[] op, int seqNum) {
        super(MSG_ID, instance, opId, op, seqNum);
    }

    @Override
    public String toString() {
        return "PrepareOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", seqNum=" + sequenceNumber +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<PrepareOKMessage>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.sequenceNumber);
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            int sequenceNumber = in.readInt();
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new PrepareOKMessage(instance, opId, op, sequenceNumber);
        }
    };
}
