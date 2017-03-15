package org.jitsi.jicofo.recording.jibri;

import net.java.sip.communicator.impl.protocol.jabber.extensions.jibri.*;
import org.jivesoftware.smack.packet.*;

/**
 *
 */
public interface JibriSessionOwner
{
    void onSessionStateChanged(
        JibriSession jibriSession,
        JibriIq.Status newStatus,
        XMPPError error);
}
