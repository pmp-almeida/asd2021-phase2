package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PrepareOKMessage extends PaxosMessage {

    public final static short MSG_ID = 103;

    public PrepareOKMessage(int instance, UUID opId, byte[] op) {
        super(MSG_ID, instance, opId, op);
    }

    @Override
    public String toString() {
        return "PrepareOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<PrepareOKMessage>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new PrepareOKMessage(instance, opId, op);
        }
    };
}
