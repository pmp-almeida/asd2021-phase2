package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PaxosMessage extends ProtoMessage {

    protected final UUID opId;
    protected final int instance;
    protected final byte[] op;

    public PaxosMessage(short ID, int instance, UUID opId, byte[] op) {
        super(ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
    }

    public UUID getOpId() {
        return opId;
    }

    public int getInstance() {
        return instance;
    }

    public byte[] getOp() {
        return op;
    }
}
