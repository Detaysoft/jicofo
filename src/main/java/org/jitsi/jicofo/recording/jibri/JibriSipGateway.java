package org.jitsi.jicofo.recording.jibri;

import net.java.sip.communicator.impl.protocol.jabber.extensions.jibri.*;
import net.java.sip.communicator.service.protocol.*;
import org.jitsi.eventadmin.*;
import org.jitsi.jicofo.*;
import org.jitsi.osgi.*;
import org.jitsi.protocol.xmpp.*;
import org.jitsi.protocol.xmpp.util.*;
import org.jitsi.util.*;
import org.jivesoftware.smack.*;
import org.jivesoftware.smack.filter.*;
import org.jivesoftware.smack.packet.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class JibriSipGateway
    implements PacketListener, PacketFilter, JibriSessionOwner
{
    /**
     * The class logger which can be used to override logging level inherited
     * from {@link JitsiMeetConference}.
     */
    static private final Logger classLogger
        = Logger.getLogger(JibriSipGateway.class);

    private final OperationSetDirectSmackXmpp xmpp;

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
     * Recorded <tt>JitsiMeetConferenceImpl</tt>.
     */
    private final JitsiMeetConferenceImpl conference;

    /**
     * The logger for this instance. Uses the logging level either of the
     * {@link #classLogger} or {@link JitsiMeetConference#getLogger()}
     * whichever is higher.
     */
    private final Logger logger;

    /**
     * Jibri detector which notifies about Jibri status changes.
     */
    private final JibriDetector jibriDetector;

    /**
     * Current Jibri recording status.
     */
    //private JibriIq.Status jibriStatus = JibriIq.Status.UNDEFINED;

    /**
     * Meet tools instance used to inject packet extensions to Jicofo's MUC
     * presence.
     */
    private final OperationSetJitsiMeetTools meetTools;


    /**
     * Executor service for used to schedule pending timeout tasks.
     */
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * The global config used by this instance.
     */
    private final JitsiMeetGlobalConfig globalConfig;

    /**
     * Helper class that registers for {@link JibriEvent}s in the OSGi context
     * obtained from the {@link FocusBundleActivator}.
     */
    private final JibriEventHandler jibriEventHandler
        = new JibriEventHandler();

    private Map<String, JibriSession> sipSessions = new HashMap<>();

    /**
     * Creates new instance of <tt>JibriRecorder</tt>.
     * @param conference <tt>JitsiMeetConference</tt> to be recorded by new
     *        instance.
     * @param xmpp XMPP operation set which wil be used to send XMPP queries.
     * @param scheduledExecutor the executor service used by this instance
     * @param globalConfig the global config that provides some values required
     *                     by <tt>JibriRecorder</tt> to work.
     */
    public JibriSipGateway(JitsiMeetConferenceImpl         conference,
                         OperationSetDirectSmackXmpp     xmpp,
                         ScheduledExecutorService        scheduledExecutor,
                         JitsiMeetGlobalConfig           globalConfig)
    {
        this.xmpp = Objects.requireNonNull(xmpp, "xmpp");
        this.conference = Objects.requireNonNull(conference, "conference");
        this.scheduledExecutor
            = Objects.requireNonNull(scheduledExecutor, "scheduledExecutor");
        this.globalConfig = Objects.requireNonNull(globalConfig, "globalConfig");

        jibriDetector
            = conference.getServices().getSipJibriDetector();

        ProtocolProviderService protocolService = conference.getXmppProvider();

        meetTools
            = protocolService.getOperationSet(OperationSetJitsiMeetTools.class);

        logger = Logger.getLogger(classLogger, conference.getLogger());
    }

    /**
     * Starts listening for Jibri updates and initializes Jicofo presence.
     *
     * {@inheritDoc}
     */
    public void init()
    {
        try
        {
            jibriEventHandler.start(FocusBundleActivator.bundleContext);
        }
        catch (Exception e)
        {
            logger.error("Failed to start Jibri event handler: " + e, e);
        }

        updateJibriAvailability();

        xmpp.addPacketHandler(this, this);
    }

    /**
     * {@inheritDoc}
     */
    public void dispose()
    {
        try
        {
            jibriEventHandler.stop(FocusBundleActivator.bundleContext);
        }
        catch (Exception e)
        {
            logger.error("Failed to stop Jibri event handler: " + e, e);
        }

        try
        {
            List<JibriSession> sessions = new ArrayList<>(sipSessions.values());
            for (JibriSession session : sessions)
            {
                session.stop();
            }
        }
        finally
        {
            sipSessions.clear();
        }
    }

    private String getRoomName()
    {
        return conference.getRoomName();
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
            "Processing an IQ: " + packet.toXML());
        //}

        IQ iq = (IQ) packet;

        String from = iq.getFrom();

        JibriIq jibriIq = (JibriIq) iq;

        String roomName = MucUtil.extractRoomNameFromMucJid(from);
        if (roomName == null)
        {
            logger.warn("Could not extract room name from jid:" + from);
            return;
        }

        String actualRoomName = getRoomName();
        if (!actualRoomName.equals(roomName))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Ignored packet from: " + roomName
                    + ", my room: " + actualRoomName
                    + " p: " + packet.toXML());
            }
            return;
        }

        XmppChatMember chatMember = conference.findMember(from);
        if (chatMember == null)
        {
            logger.warn("ERROR chat member not found for: " + from
                + " in " + roomName);
            return;
        }

        processJibriIqFromMeet(jibriIq, chatMember);
    }

    /**
     * Accepts only {@link JibriIq}
     * {@inheritDoc}
     */
    @Override
    public boolean accept(Packet packet)
    {
        // Do not process if it belongs to the recording session
        // FIXME should accept only packets coming from MUC
        return !comesFromSipSession(packet)
            && packet instanceof JibriIq
            && !StringUtils.isNullOrEmpty(((JibriIq)(packet)).getSipAddress());
    }

    private boolean comesFromSipSession(Packet p)
    {
        for (JibriSession session : sipSessions.values())
        {
            if (session.accept(p))
                return true;
        }
        return false;
    }

    /**
     * The method is supposed to update Jibri availability status to OFF if we
     * have any Jibris available or to UNDEFINED if there are no any.
     */
    private void updateJibriAvailability()
    {
        if (jibriDetector.selectJibri() != null)
        {
            setAvailabilityStatus(JibriIq.Status.AVAILABLE);
        }
        else if (jibriDetector.isAnyJibriConnected())
        {
            setAvailabilityStatus(JibriIq.Status.BUSY);
        }
        else
        {
            setAvailabilityStatus(JibriIq.Status.UNDEFINED);
        }
    }

    private void setAvailabilityStatus(JibriIq.Status newStatus)
    {
        setAvailabilityStatus(newStatus, null);
    }

    @Override
    public void onSessionStateChanged(
        JibriSession jibriSession, JibriIq.Status newStatus, XMPPError error)
    {
        if (!sipSessions.values().contains(jibriSession))
        {
            logger.error(
                "onSessionStateChanged for unknown session: " + jibriSession);
            return;
        }

        // FIXME
        boolean sessionStoped
            = JibriIq.Status.FAILED.equals(newStatus) ||
                    JibriIq.Status.OFF.equals(newStatus);

        setJibriStatus(jibriSession, newStatus, error);

        if (sessionStoped)
        {
            String sipAddress = jibriSession.getSipAddress();
            sipSessions.remove(sipAddress);

            logger.info("Removing SIP call: " + sipAddress);

            updateJibriAvailability();
        }
    }

    private void setAvailabilityStatus(JibriIq.Status newStatus, XMPPError error)
    {
        //jibriStatus = newStatus;

        SipGatewayStatus sipGatewayStatus = new SipGatewayStatus();

        sipGatewayStatus.setStatus(newStatus);

        sipGatewayStatus.setError(error);

        logger.info(
            "Publish new SIP JIBRI status: "
                + sipGatewayStatus.toXML() + " in: " + getRoomName());

        ChatRoom2 chatRoom2 = conference.getChatRoom();

        // Publish that in the presence
        if (chatRoom2 != null)
        {
            meetTools.sendPresenceExtension(chatRoom2, sipGatewayStatus);
        }
    }

    private void setJibriStatus(JibriSession session,
                                JibriIq.Status newStatus, XMPPError error)
    {
        SipCallState sipCallState = new SipCallState();

        sipCallState.setState(newStatus);

        sipCallState.setError(error);

        sipCallState.setSipAddress(session.getSipAddress());

        logger.info(
            "Publish new Jibri SIP status for: " + session.getSipAddress()
                + sipCallState.toXML() + " in: " + getRoomName());

        ChatRoom2 chatRoom2 = conference.getChatRoom();

        // Publish that in the presence
        if (chatRoom2 != null)
        {
            Collection<PacketExtension> oldExtension
                = chatRoom2.getPresenceExtensions();
            LinkedList<PacketExtension> toRemove = new LinkedList<>();
            for (PacketExtension ext : oldExtension)
            {
                // Exclude all that do not match
                if (ext instanceof  SipCallState
                    && session.getSipAddress().equals(
                            ((SipCallState)ext).getSipAddress()))
                {
                    toRemove.add(ext);
                }
            }
            ArrayList<PacketExtension> newExt = new ArrayList<>();
            newExt.add(sipCallState);

            chatRoom2.modifyPresence(toRemove, newExt);

            //meetTools.sendPresenceExtension(chatRoom2, sipCallState);
        }
    }

    private void processJibriIqFromMeet(final JibriIq           iq,
                                        final XmppChatMember    sender)
    {
        String senderMucJid = sender.getContactAddress();
        //if (logger.isDebugEnabled())
        //{
        logger.info(
            "Jibri request from " + senderMucJid + " iq: " + iq.toXML());
        //}

        JibriIq.Action action = iq.getAction();
        if (JibriIq.Action.UNDEFINED.equals(action))
            return;

        // verifyModeratorRole sends 'not_allowed' error on false
        if (!verifyModeratorRole(iq))
        {
            logger.warn(
                "Ignored Jibri request from non-moderator: " + senderMucJid);
            return;
        }

        String displayName = iq.getDisplayName();
        String sipAddress = iq.getSipAddress();
        JibriSession jibriSession = sipSessions.get(sipAddress);

        // start ?
        if (JibriIq.Action.START.equals(action) &&
            //JibriIq.Status.OFF.equals(jibriStatus) &&
            jibriSession == null)
        {
            // Store stream ID

            // Proceed if not empty
            if (!StringUtils.isNullOrEmpty(sipAddress))
            {
                // ACK the request immediately to simplify the flow,
                // any error will be passed with the FAILED state
                sendResultResponse(iq);
                jibriSession
                    = new JibriSession(
                            this,
                            conference,
                            globalConfig,
                            xmpp,
                            scheduledExecutor,
                            jibriDetector,
                            false, sipAddress, displayName, null,
                            classLogger);

                sipSessions.put(sipAddress, jibriSession);
                // Try starting Jibri on separate thread with retries
                jibriSession.start();
                return;
            }
            else
            {
                // Bad request - no stream ID
                sendErrorResponse(
                    iq,
                    XMPPError.Condition.bad_request,
                    "Stream ID is empty or undefined");
                return;
            }
        }
        // stop ?
        else if (JibriIq.Action.STOP.equals(action) &&
            jibriSession != null
            // FIXME
            /*(JibriIq.Status.ON.equals(jibriStatus) ||
                isStartingStatus(jibriStatus))*/)
        {
            // XXX FIXME: this is synchronous and will probably block the smack
            // thread that executes processPacket().
            //try
            //{
            XMPPError error = jibriSession.stop();
            sendPacket(
                error == null
                    ? IQ.createResultIQ(iq)
                    : IQ.createErrorResponse(iq, error));
            //}
            //catch (OperationFailedException e)
            //{
            // XXX the XMPP connection is broken
            // This instance shall be disposed soon after
            //logger.error("Failed to send stop IQ - XMPP disconnected", e);
            // FIXME deal with it
            //recordingStopped(null, false /* do not send status update */);
            //}
            return;
        }

        logger.warn(
            "Discarded: " + iq.toXML() + " - nothing to be done, ");
                //+ "recording status:" + jibriSession.getStatus());

        // Bad request
        sendErrorResponse(
            iq, XMPPError.Condition.bad_request,
            "Unable to handle: '" + action);
                //+ "' in state: '" + jibriStatus + "'");
    }

    private boolean verifyModeratorRole(JibriIq iq)
    {
        String from = iq.getFrom();
        ChatRoomMemberRole role = conference.getRoleForMucJid(from);

        if (role == null)
        {
            // Only room members are allowed to send requests
            sendErrorResponse(iq, XMPPError.Condition.forbidden, null);
            return false;
        }

        if (ChatRoomMemberRole.MODERATOR.compareTo(role) < 0)
        {
            // Moderator permission is required
            sendErrorResponse(iq, XMPPError.Condition.not_allowed, null);
            return false;
        }
        return true;
    }

    private void sendPacket(Packet packet)
    {
        xmpp.getXmppConnection().sendPacket(packet);
    }

    private void sendResultResponse(IQ request)
    {
        sendPacket(
            IQ.createResultIQ(request));
    }

    private void sendErrorResponse(IQ request,
                                   XMPPError.Condition condition,
                                   String msg)
    {
        sendPacket(
            IQ.createErrorResponse(
                request,
                new XMPPError(condition, msg)
            )
        );
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
            if (isSIP)
            {
                return;
            }

            switch (topic)
            {
                case JibriEvent.WENT_OFFLINE:
                case JibriEvent.STATUS_CHANGED:
                    synchronized (JibriSipGateway.this)
                    {
                        updateJibriAvailability();
                    }
                    break;
                default:
                    logger.error("Invalid topic: " + topic);
            }
        }
    }
}
