package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptOKMessage extends PaxosMessage {

    public final static short MSG_ID = 105;

    public AcceptOKMessage(int instance, UUID opId, byte[] op) {
        super(MSG_ID, instance, opId, op);
    }

    @Override
    public String toString() {
        return "AcceptOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<AcceptOKMessage>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new AcceptOKMessage(instance, opId, op);
        }
    };
}
