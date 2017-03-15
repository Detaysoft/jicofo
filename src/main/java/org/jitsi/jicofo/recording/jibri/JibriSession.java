package org.jitsi.jicofo.recording.jibri;

import net.java.sip.communicator.impl.protocol.jabber.extensions.jibri.*;
import net.java.sip.communicator.service.protocol.*;
import org.jitsi.eventadmin.*;
import org.jitsi.jicofo.*;
import org.jitsi.osgi.*;
import org.jitsi.protocol.xmpp.*;
import org.jitsi.util.Logger;
import org.jitsi.xmpp.util.*;
import org.jivesoftware.smack.*;
import org.jivesoftware.smack.filter.*;
import org.jivesoftware.smack.packet.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by pdomas on 09/03/2017.
 */
public class JibriSession
    implements PacketFilter,
               PacketListener
{
    /**
     * The class logger which can be used to override logging level inherited
     * from {@link JitsiMeetConference}.
     */
    static private final Logger classLogger
        = Logger.getLogger(JibriSession.class);

    /**
     * The number of times to retry connecting to a Jibri.
     */
    static private final int NUM_RETRIES = 3;

    /**
     * Returns <tt>true> if given <tt>status</tt> precedes the <tt>RETRYING</tt>
     * status or <tt>false</tt> otherwise.
     */
    static private boolean isPreRetryStatus(JibriIq.Status status)
    {
        return JibriIq.Status.ON.equals(status)
            || JibriIq.Status.RETRYING.equals(status);
    }

    /**
     * Returns <tt>true</tt> if given <tt>status</tt> indicates that Jibri is in
     * the middle of starting of the recording process.
     */
    static private boolean isStartingStatus(JibriIq.Status status)
    {
        return JibriIq.Status.PENDING.equals(status)
            || JibriIq.Status.RETRYING.equals(status);
    }

    /**
     * The logger for this instance. Uses the logging level either of the
     * {@link #classLogger} or {@link JitsiMeetConference#getLogger()}
     * whichever is higher.
     */
    private final Logger logger;

    private final JitsiMeetConferenceImpl conference;

    private final OperationSetDirectSmackXmpp xmpp;

    private final JibriDetector jibriDetector;

    private final JibriSessionOwner owner;

    private String currentJibriJid;

    private final String displayName;

    private final boolean isSIP;

    private final String sipAddress;

    private final String streamID;

    /**
     * Helper class that registers for {@link JibriEvent}s in the OSGi context
     * obtained from the {@link FocusBundleActivator}.
     */
    private final JibriEventHandler jibriEventHandler = new JibriEventHandler();

    /**
     * Stores reference to the reply timeout task, so that it can be cancelled
     * once becomes irrelevant.
     */
    private Future<?> timeoutTrigger;

    /**
     * Reference to scheduled {@link Jibri.PendingStatusTimeout}
     */
    private ScheduledFuture<?> pendingTimeoutTask;

    /**
     * Counts retry attempts.
     * FIXME it makes sense to retry as long as there are Jibris available, but
     * currently if one Jibri will not go offline, but keep returning some error
     * JibriDetector may keep selecting it infinitely, as we do not blacklist
     * such instances yet
     */
    private int retryAttempt = 0;

    /**
     * Current Jibri recording status.
     */
    private JibriIq.Status jibriStatus = JibriIq.Status.UNDEFINED;

    /**
     * The global config used by this instance.
     */
    private final JitsiMeetGlobalConfig globalConfig;

    /**
     * Executor service for used to schedule pending timeout tasks.
     */
    private final ScheduledExecutorService scheduledExecutor;

    public JibriSession(JibriSessionOwner owner,
                        JitsiMeetConferenceImpl conference,
                        JitsiMeetGlobalConfig globalConfig,
                        OperationSetDirectSmackXmpp     xmpp,
                        ScheduledExecutorService        scheduledExecutor,
                        JibriDetector jibriDetector,
                        boolean isSIP,
                        String sipAddress,
                        String displayName,
                        String streamID,
                        Logger logLevelDelegate)
    {
        this.owner = owner;
        this.conference = conference;
        this.scheduledExecutor
            = Objects.requireNonNull(scheduledExecutor, "scheduledExecutor");
        this.globalConfig
            = Objects.requireNonNull(globalConfig, "globalConfig");
        this.isSIP = isSIP;
        this.jibriDetector = jibriDetector;
        this.sipAddress = sipAddress;
        this.displayName = displayName;
        this.streamID = streamID;
        this.xmpp = xmpp;
        logger = Logger.getLogger(classLogger,logLevelDelegate);
    }

    public void start()
    {
        xmpp.addPacketHandler(this, this);

        // Try starting Jibri on separate thread with retries
        tryStartRestartJibri(null);
    }

    public XMPPError stop()
    {
        try
        {
            jibriEventHandler.stop(FocusBundleActivator.bundleContext);
        }
        catch (Exception e)
        {
            logger.error("Failed to stop Jibri event handler: " + e, e);
        }

        XMPPError error = null;
        /**
         * When sendStopIQ() succeeds without any errors it will reset the state
         * to "recording stopped", but in case something goes wrong the decision
         * must be made outside of that method.
         */
        boolean stoppedGracefully;
        try
        {
            error = sendStopIQ();
            if (error != null)
            {
                logger.error(
                    "An error response to the stop request: " + error.toXML());
            }
            stoppedGracefully = error == null;
        }
        catch (OperationFailedException e)
        {
            logger.error("Failed to send stop IQ - XMPP disconnected", e);
            stoppedGracefully = false;
        }

        if (!stoppedGracefully) {
            // The instance is going down which means that
            // the JitsiMeetConference is being disposed. We don't want any
            // updates to be sent, but it makes sense to reset the state
            // (and that's what recordingSopped() will do).
            recordingStopped(
                null, false /* do not send any status updates */);
        }
        else
        {
            // FIXME does not care about error response
            setJibriStatus(JibriIq.Status.OFF, null);
        }

        xmpp.removePacketHandler(this);

        return error;
    }

    /**
     * Sends a "stop" command to the current Jibri(if any). If the operation is
     * accepted by Jibri (with a RESULT response) then the instance state will
     * be adjusted to stopped and new recording availability status will be
     * sent. Otherwise the decision whether the instance should go to
     * the stopped state has to be taken outside of this method based on
     * the result returned/Exception thrown.
     *
     * @return XMPPError if Jibri replies with an error or <tt>null</tt> if
     * the recording was stopped gracefully.
     *
     * @throws OperationFailedException if the XMPP connection is broken.
     */
    private XMPPError sendStopIQ()
        throws OperationFailedException
    {
        if (currentJibriJid == null)
            return null;

        JibriIq stopRequest = new JibriIq();

        stopRequest.setType(IQ.Type.SET);
        stopRequest.setTo(currentJibriJid);
        stopRequest.setAction(JibriIq.Action.STOP);

        logger.info("Trying to stop: " + stopRequest.toXML());

        IQ stopReply
            = (IQ) xmpp
            .getXmppConnection()
            .sendPacketAndGetReply(stopRequest);

        logger.info("Stop response: " + IQUtils.responseToXML(stopReply));

        if (stopReply == null)
        {
            return new XMPPError(XMPPError.Condition.request_timeout, null);
        }

        if (IQ.Type.RESULT.equals(stopReply.getType()))
        {
            logger.info(
                recordingOrSIPCall()
                    + " stopped on user request in " + getRoomName());
            recordingStopped(null);
            return null;
        }
        else
        {
            XMPPError error = stopReply.getError();
            if (error == null)
            {
                error
                    = new XMPPError(XMPPError.Condition.interna_server_error);
            }
            return error;
        }
    }

    @Override
    public boolean accept(Packet packet)
    {
        boolean accept = packet instanceof IQ
            && currentJibriJid != null
            && (packet.getFrom().equals(currentJibriJid) ||
                    (packet.getFrom() + "/").startsWith(currentJibriJid));
        //if (!accept)
        //{
          //  logger.info("SESSION REJECTING: " + packet.toXML());
            //logger.info("CURRENT JIBRI: " + currentJibriJid +" from: " + packet.getFrom());
        //}

        return accept;
    }

    /**
     * <tt>JibriIq</tt> processing.
     *
     * {@inheritDoc}
     */
    @Override
    synchronized public void processPacket(Packet packet)
    {
        //if (logger.isDebugEnabled())
        //{
            logger.info(
                "Processing an IQ from Jibri: " + packet.toXML());
        //}

        IQ iq = (IQ) packet;

        String from = iq.getFrom();

        if (iq instanceof JibriIq)
        {
            JibriIq jibriIq = (JibriIq) iq;

            if (currentJibriJid != null && from.equals(currentJibriJid))
                    //(from + "/").startsWith(currentJibriJid)))
            {
                processJibriIqFromJibri(jibriIq);
            }
            else
            {
                logger.warn("Ignored packet: " + iq.toXML());
            }
        }
        else
        {
            // We're processing Jibri response, probably an error
            if (IQ.Type.ERROR.equals(iq.getType()))
            {
                processJibriError(iq.getError());
            }
        }
    }

    private void processJibriIqFromJibri(JibriIq iq)
    {
        if (IQ.Type.RESULT.equals(iq.getType()))
            return;

        // We have something from Jibri - let's update recording status
        JibriIq.Status status = iq.getStatus();
        if (!JibriIq.Status.UNDEFINED.equals(status))
        {
            String roomName = getRoomName();

            logger.info(
                "Updating status from JIBRI: "
                    + iq.toXML() + " for " + roomName);

            // We stop either on "off" or on "failed"
            if ((JibriIq.Status.OFF.equals(status) ||
                JibriIq.Status.FAILED.equals(status))
                && currentJibriJid != null/* This means we're recording */)
            {
                // Make sure that there is XMPPError for eventual ERROR status
                XMPPError error = iq.getError();
                if (JibriIq.Status.FAILED.equals(status) && error == null)
                {
                    error = new XMPPError(
                        XMPPError.Condition.interna_server_error,
                        "Unknown error");
                }
                processJibriError(error);
            }
            else
            {
                // FIXME we don't care ?
                setJibriStatus(status, null);
                //owner.onSessionStateChanged(this, status, null);
            }
        }

        sendResultResponse(iq);
    }

    /**
     * Processes an error received from Jibri.
     */
    private void processJibriError(XMPPError error)
    {
        if (currentJibriJid != null)
        {
            logger.info(currentJibriJid + " failed for room "
                + getRoomName() + " with "
                + (error != null ? error.toXML() : "null"));

            tryStartRestartJibri(error);
        }
        else
        {
            logger.warn("Triggered error while not active: " + error.toXML()
                + " in: " + getRoomName());
        }
    }

    /**
     * Will try to start Jibri recording if {@link #retryAttempt} <
     * {@link #NUM_RETRIES}. If retry limit is exceeded then will fail with
     * the given <tt>error</tt>. If <tt>error</tt> is <tt>null</tt> either
     * "service unavailable"(no Jibri available) or "retry limit exceeded"
     * will be used.
     * @param error optional <tt>XMPPError</tt> to fail with if the retry count
     * limit has been exceeded or there are no more Jibris to try with.
     */
    private void tryStartRestartJibri(XMPPError error)
    {
        if (retryAttempt++ < NUM_RETRIES)
        {
            final String newJibriJid = jibriDetector.selectJibri();
            if (newJibriJid != null)
            {
                startJibri(newJibriJid);
                return;
            }
            else if (error == null)
            {
                // Classify this failure as 'service not available'
                error = new XMPPError(XMPPError.Condition.service_unavailable);
            }
        }
        if (error == null)
        {
            error = new XMPPError(
                        XMPPError.Condition.interna_server_error,
                        "Retry limit exceeded");
        }
        // No more retries, stop either with the error passed as an argument
        // or with one defined here in this method, which will provide more
        // details about the reason
        setJibriStatus(JibriIq.Status.FAILED, error);
        //owner.onSessionStateChanged(this, JibriIq.Status.FAILED, error);
    }

    /**
     * Methods clears {@link #currentJibriJid} which means we're no longer
     * recording nor in contact with any Jibri instance.
     * Refreshes recording status in the room based on Jibri availability.
     *
     * @param error if the recording stopped because of an error it should be
     * passed as an argument here which will result in stopping with
     * the {@link JibriIq.Status#FAILED} status passed to the application.
     */
    private void recordingStopped(XMPPError error)
    {
        recordingStopped(error, true /* send recording status update */);
    }

    /**
     * Methods clears {@link #currentJibriJid} which means we're no longer
     * recording nor in contact with any Jibri instance.
     * Refreshes recording status in the room based on Jibri availability.
     *
     * @param error if the recording stopped because of an error it should be
     * passed as an argument here which will result in stopping with
     * the {@link JibriIq.Status#FAILED} status passed to the application.
     * @param updateStatus <tt>true</tt> if the Jibri availability status
     * broadcast should follow the transition to the stopped state.
     */
    private void recordingStopped(XMPPError error, boolean updateStatus)
    {
        if (isSIP)
        {
            logger.info(
                "Jibri SIP stopped for: "
                    + sipAddress + " in: " + getRoomName());
        }
        else
        {
            logger.info("Recording stopped for: " + getRoomName());
        }

        currentJibriJid = null;
        retryAttempt = 0;
        cancelTimeoutTrigger();

        // First we'll send an error and then follow with availability status
        if (error != null)
        {
            setJibriStatus(JibriIq.Status.FAILED, error);
        }
    }

    private void cancelTimeoutTrigger()
    {
        if (timeoutTrigger != null)
        {
            timeoutTrigger.cancel(false);
            timeoutTrigger = null;
        }
    }

    /**
     * Sends an IQ to the given Jibri instance and asks it to start recording.
     */
    private void startJibri(final String jibriJid)
    {
        final String roomName = getRoomName();

        logger.info(
            "Starting Jibri " + jibriJid
                + (isSIP
                ? ("for SIP address: " + sipAddress)
                : (" for stream ID: " + streamID))
                + " in room: " + roomName);

        final JibriIq startIq = new JibriIq();
        startIq.setTo(jibriJid);
        startIq.setType(IQ.Type.SET);
        startIq.setAction(JibriIq.Action.START);
        startIq.setStreamId(streamID);
        startIq.setSipAddress(sipAddress);
        startIq.setDisplayName(displayName);

        // Insert name of the room into Jibri START IQ
        startIq.setRoom(roomName);

        // Store Jibri JID to make the packet filter accept the response
        currentJibriJid = jibriJid;

        // We're now in PENDING state(waiting for Jibri ON update)
        // Setting PENDING status also blocks from accepting
        // new start requests
        setJibriStatus(isPreRetryStatus(jibriStatus)
            ? JibriIq.Status.RETRYING : JibriIq.Status.PENDING, null);

        // We will not wait forever for the Jibri to start. This method can be
        // run multiple times on retry, so we want to restart the pending
        // timeout each time.
        reschedulePendingTimeout();

        // Clear the old timeout trigger if any
        cancelTimeoutTrigger();
        // Send start IQ on separate thread to not block, the packet processor
        // thread and still be able to detect eventual timeout. The response is
        // processed in processPacket().
        timeoutTrigger = scheduledExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    IQ startReply
                        = (IQ) xmpp.getXmppConnection()
                        .sendPacketAndGetReply(startIq);
                    if (startReply == null)
                    {
                        synchronized (JibriSession.this)
                        {
                            // Trigger request timeout
                            logger.info(
                                "Will trigger timeout in room: " + roomName);
                            processJibriError(
                                new XMPPError(
                                    XMPPError.Condition.request_timeout));
                        }
                    }
                    //else the response will be handled in processPacket()
                    logger.info("GOT start response: " + startReply.toXML());
                }
                catch (Throwable t)
                {
                    logger.error(
                        "Failed to start " + recordingOrSIPCall()
                            +" in " + roomName
                            + (t instanceof OperationFailedException ?
                            " - XMPP connection is broken"
                            : ""), t);
                    recordingStopped(
                        null, false /* do not send status updates */);
                }
            }
        });
    }

    /**
     * Method schedules/reschedules {@link Jibri.PendingStatusTimeout} which will
     * clear recording state after
     * {@link JitsiMeetGlobalConfig#getJibriPendingTimeout()}.
     */
    private void reschedulePendingTimeout()
    {
        if (pendingTimeoutTask != null)
        {
            logger.info("Rescheduling pending timeout task for room: "
                + getRoomName());
            pendingTimeoutTask.cancel(false);
        }

        int pendingTimeout = globalConfig.getJibriPendingTimeout();
        if (pendingTimeout > 0)
        {
            pendingTimeoutTask
                = scheduledExecutor.schedule(
                        new PendingStatusTimeout(),
                        pendingTimeout, TimeUnit.SECONDS);
        }
    }

    private void sendPacket(Packet packet)
    {
        xmpp.getXmppConnection().sendPacket(packet);
    }

    private void sendResultResponse(IQ request)
    {
        sendPacket(IQ.createResultIQ(request));
    }

    private String getRoomName()
    {
        return conference.getRoomName();
    }

    private void setJibriStatus(JibriIq.Status newStatus, XMPPError error)
    {
        jibriStatus = newStatus;

        // Clear "pending" status timeout if we enter state other than "pending"
        if (pendingTimeoutTask != null
            && !JibriIq.Status.PENDING.equals(newStatus))
        {
            pendingTimeoutTask.cancel(false);
            pendingTimeoutTask = null;
        }

        if (JibriIq.Status.ON.equals(newStatus))
        {
            // Reset retry counter
            retryAttempt = 0;
        }

        owner.onSessionStateChanged(this, newStatus, error);
    }

    synchronized private void onJibriOffline(final String jibriJid)
    {
        if (jibriJid.equals(currentJibriJid))
        {
            logger.warn(nickname() + " went offline: " + currentJibriJid
                + " for room: " + getRoomName());

            tryStartRestartJibri(
                new XMPPError(
                    XMPPError.Condition.remote_server_error,
                    nickname() + " disconnected unexpectedly"));
        }
    }

    private String recordingOrSIPCall()
    {
        return this.isSIP ? "SIP call" : "recording";
    }

    private String nickname()
    {
        return this.isSIP ? "SIP Jibri" : "Jibri";
    }

    public String getSipAddress()
    {
        return sipAddress;
    }

    /**
     * Helper class handles registration for the {@link JibriEvent}s.
     */
    private class JibriEventHandler
        extends EventHandlerActivator
    {

        private JibriEventHandler()
        {
            super(new String[]{
                JibriEvent.STATUS_CHANGED, JibriEvent.WENT_OFFLINE});
        }

        @Override
        public void handleEvent(Event event)
        {
            if (!JibriEvent.isJibriEvent(event))
            {
                logger.error("Invalid event: " + event);
                return;
            }

            final JibriEvent jibriEvent = (JibriEvent) event;
            final String topic = jibriEvent.getTopic();
            final boolean isSIP = jibriEvent.isSIP();
            if (JibriSession.this.isSIP != isSIP)
            {
                return;
            }

            switch (topic)
            {
                case JibriEvent.WENT_OFFLINE:
                    onJibriOffline(jibriEvent.getJibriJid());
                    break;
                default:
                    logger.error("Invalid topic: " + topic);
            }
        }
    }

    /**
     * Task scheduled after we have received RESULT response from Jibri and
     * entered PENDING state. Will abort the recording if we do not transit to
     * ON state after {@link JitsiMeetGlobalConfig#getJibriPendingTimeout()}
     * limit is exceeded.
     */
    private class PendingStatusTimeout implements Runnable
    {
        public void run()
        {
            synchronized (JibriSession.this)
            {
                // Clear this task reference, so it won't be
                // cancelling itself on status change from PENDING
                pendingTimeoutTask = null;

                if (isStartingStatus(jibriStatus))
                {
                    logger.error(
                        nickname() + " pending timeout! " + getRoomName());
                    XMPPError error
                        = new XMPPError(
                        XMPPError.Condition.remote_server_timeout);
                    recordingStopped(error);
                }
            }
        }
    }
}
