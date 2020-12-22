package protocols.agreement;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.timers.MajorityTimer;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Paxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(protocols.agreement.Paxos.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Paxos";

    private Host myself;
    private int joinedInstance;
    private List<Host> membership;

    private int majority;
    private AtomicInteger numPrepareOks, numAcceptOks;

    //Highest PREPARE sequence number
    private int highestPrepareSeqNum;
    //Highest ACCEPT sequence number
    private int highestAcceptSeqNum;
    //Highest ACCEPT value
    private byte[] highestAcceptValue;
    private byte[] tempHighestAcceptValue;

    private final int timeout;

    private Map<UUID, Long> timerIds;
    private Map<Long, UUID> opIds;

    public Paxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;

        majority = 0;
        numPrepareOks = numAcceptOks = new AtomicInteger(0);
        highestPrepareSeqNum = highestAcceptSeqNum = -1;
        highestAcceptValue = tempHighestAcceptValue = new byte[0];

        timerIds = new ConcurrentHashMap<>();
        opIds = new ConcurrentHashMap<>();

        timeout = Integer.parseInt(props.getProperty("majority_timeout", "1000"));

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(MajorityTimer.TIMER_ID, this::uponMajorityTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOKMessage.MSG_ID, AcceptOKMessage.serializer);
        registerMessageSerializer(cId, DecidedMessage.MSG_ID, DecidedMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);
            registerMessageHandler(cId, DecidedMessage.MSG_ID, this::uponDecidedMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    /*--------------------------------- Requests -------------------------------------- */
    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received " + request);
        highestAcceptValue = request.getOperation();
        PaxosMessage msg = new PrepareMessage(request.getInstance(), request.getOpId(), ++highestPrepareSeqNum);
        logger.debug("Sending to: " + membership);
        membership.forEach(h -> sendMessage(msg, h));
        long timerId = setupTimer(new MajorityTimer(), timeout);
        timerIds.put(msg.getOpId(), timerId);
        opIds.put(timerId, msg.getOpId());
    }

    /*--------------------------------- Messages -------------------------------------- */
    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received " + msg);
        if(msg.getSequenceNumber() > highestPrepareSeqNum) {
            highestPrepareSeqNum = msg.getSequenceNumber();
            PaxosMessage msgReply = new PrepareOKMessage(msg.getInstance(), msg.getOpId(), highestAcceptValue, highestAcceptSeqNum);
            sendMessage(msgReply, host);
        }
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received " + msg);
        if(msg.getSequenceNumber() > highestPrepareSeqNum) {
            tempHighestAcceptValue = msg.getOp();
        }
        if(numPrepareOks.incrementAndGet() >= majority) {
            cancelTimer(timerIds.get(msg.getOpId()));
            numPrepareOks.set(0);
            opIds.remove(timerIds.get(msg.getOpId()));
            if(tempHighestAcceptValue.length != 0 && highestAcceptValue != tempHighestAcceptValue) {
                highestAcceptValue = tempHighestAcceptValue;
            }
            PaxosMessage msgReply = new AcceptMessage(msg.getInstance(), msg.getOpId(), highestAcceptValue, highestPrepareSeqNum);
            membership.forEach(h -> sendMessage(msgReply, h));
            long timerId = setupTimer(new MajorityTimer(), timeout);
            timerIds.put(msg.getOpId(), timerId);
            opIds.put(timerId, msg.getOpId());
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received " + msg);
        if(msg.getSequenceNumber() >= highestPrepareSeqNum) {
            highestAcceptSeqNum = msg.getSequenceNumber();
            highestAcceptValue = msg.getOp();
            PaxosMessage msgReply = new AcceptOKMessage(msg.getInstance(), msg.getOpId(), msg.getSequenceNumber());
            sendMessage(msgReply, host);
        }
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received " + msg);
        if(numAcceptOks.incrementAndGet() > majority) {
            cancelTimer(timerIds.get(msg.getOpId()));
            numAcceptOks.set(0);
            PaxosMessage msgReply = new DecidedMessage(msg.getInstance(), msg.getOpId(), highestAcceptValue);
            membership.forEach(h -> sendMessage(msgReply, h));
        }
    }

    private void uponDecidedMessage(DecidedMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received " + msg);
        triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
    }

    /*--------------------------------- Notifications --------------------------------- */
    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
        logger.info("Starting off with majority of: " + verifyMajority());
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.add(request.getReplica());
        logger.info("Added replica. Majority is now: " + verifyMajority());
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
        logger.info("Removed replica. Majority is now: " + verifyMajority());
    }

    /*--------------------------------- Errors ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponMajorityTimeout(MajorityTimer timer, long timerId) {
        UUID opId = opIds.get(timerId);
        PaxosMessage msg = new PrepareMessage(joinedInstance, opId, ++highestPrepareSeqNum);
        logger.debug("Retrying sending to: " + membership);
        membership.forEach(h -> sendMessage(msg, h));
        long tempTimerId = setupTimer(new MajorityTimer(), timeout);
        timerIds.put(msg.getOpId(), tempTimerId);
        opIds.put(tempTimerId, msg.getOpId());
    }

    /*--------------------------------- Utils ---------------------------------------- */
    private int verifyMajority() {
        if (membership.size() <= 2) {
            majority = membership.size();
        } else {
            majority = membership.size() / 2 + 1;
        }
        return majority;
    }
}
