package protocols.agreement.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.util.UUID;

public class PaxosMessage extends ProtoMessage {

    protected final UUID opId;
    protected final int instance;
    protected final byte[] op;
    protected final int sequenceNumber;

    public PaxosMessage(short ID, int instance, UUID opId, int seqNum) {
        this(ID, instance, opId, new byte[0], seqNum);
    }

    public PaxosMessage(short ID, int instance, UUID opId, byte[] op) {
        this(ID, instance, opId, op, -1);
    }

    public PaxosMessage(short ID, int instance, UUID opId, byte[] op, int seqNum) {
        super(ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.sequenceNumber = seqNum;
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

    public int getSequenceNumber() {
        return sequenceNumber;
    }
}
