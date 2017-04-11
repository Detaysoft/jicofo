/*
 * Jicofo, the Jitsi Conference Focus.
 *
 * Copyright @ 2015 Atlassian Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jitsi.jicofo.recording.jibri;

import net.java.sip.communicator.impl.protocol.jabber.extensions.jibri.*;
import net.java.sip.communicator.util.*;

import org.jitsi.eventadmin.*;
import org.jitsi.jicofo.*;
import org.jitsi.jicofo.xmpp.*;
import org.jitsi.osgi.*;
import org.jitsi.service.configuration.*;

/**
 * <tt>JibriDetector</tt> manages the pool of Jibri instances which exist in
 * the current session. Does that by joining "brewery" room where Jibris connect
 * to and publish their's status in MUC presence. It emits {@link JibriEvent}s
 * to reflect current Jibri's status.
 *
 * @author Pawel Domas
 */
public class JibriDetector
    extends BaseBrewery<JibriStatusPacketExt>
{
    /**
     * The logger
     */
    private static final Logger logger = Logger.getLogger(JibriDetector.class);

    /**
     * The name of config property which provides the name of the MUC room in
     * which all Jibri instances.
     */
    private static final String JIBRI_ROOM_PNAME
        = "org.jitsi.jicofo.jibri.BREWERY";

    /**
     * The reference to the <tt>EventAdmin</tt> service which is used to send
     * {@link JibriEvent}s.
     */
    private final OSGIServiceRef<EventAdmin> eventAdminRef;

    /**
     * Loads the name of Jibri brewery MUC room from the configuration.
     * @param config the instance of <tt>ConfigurationService</tt> which will be
     *        used to read the properties required.
     * @return the name of Jibri brewery or <tt>null</tt> if none configured.
     */
    static public String loadBreweryName(ConfigurationService config)
    {
        return config.getString(JIBRI_ROOM_PNAME);
    }

    /**
     * Creates new instance of <tt>JibriDetector</tt>
     * @param protocolProvider the instance fo <tt>ProtocolProviderHandler</tt>
     *        for Jicofo's XMPP connection.
     * @param jibriBreweryName the name of the Jibri brewery MUC room where all
     *        Jibris will gather.
     */
    public JibriDetector(ProtocolProviderHandler protocolProvider,
                         String jibriBreweryName)
    {
        super(
            protocolProvider,
            jibriBreweryName,
            JibriStatusPacketExt.ELEMENT_NAME,
            JibriStatusPacketExt.NAMESPACE,
            true);

        this.eventAdminRef
            = new OSGIServiceRef<>(
                    FocusBundleActivator.bundleContext, EventAdmin.class);
    }

    /**
     * Selects first idle Jibri which can be used to start recording.
     *
     * @return XMPP address of idle Jibri instance or <tt>null</tt> if there are
     *         no Jibris available currently.
     */
    public String selectJibri()
    {
        for (BrewInstance jibri : instances)
        {
            if (JibriStatusPacketExt.Status.IDLE.equals(
                    jibri.status.getStatus()))
            {
                return jibri.mucJid;
            }
        }
        return null;
    }

    @Override
    protected void onInstanceStatusChanged(
        String mucJid,
        JibriStatusPacketExt presenceExt)
    {
        JibriStatusPacketExt.Status status = presenceExt.getStatus();

        if (JibriStatusPacketExt.Status.UNDEFINED.equals(status))
        {
            notifyInstanceOffline(mucJid);
        }
        else if (JibriStatusPacketExt.Status.IDLE.equals(status))
        {
            notifyJibriStatus(mucJid, true);
        }
        else if (JibriStatusPacketExt.Status.BUSY.equals(status))
        {
            notifyJibriStatus(mucJid, false);
        }
        else
        {
            logger.error(
                "Unknown Jibri status: " + status + " for " + mucJid);
        }
    }

    @Override
    protected void notifyInstanceOffline(String jid)
    {
        logger.info("Jibri " + jid +" went offline");

        EventAdmin eventAdmin = eventAdminRef.get();
        if (eventAdmin != null)
        {
            eventAdmin.postEvent(JibriEvent.newWentOfflineEvent(jid));
        }
        else
            logger.error("No EventAdmin !");
    }

    private void notifyJibriStatus(String jibriJid, boolean available)
    {
        logger.info("Jibri " + jibriJid +" available: " + available);

        EventAdmin eventAdmin = eventAdminRef.get();
        if (eventAdmin != null)
        {
            eventAdmin.postEvent(
                    JibriEvent.newStatusChangedEvent(
                            jibriJid, available));
        }
        else
            logger.error("No EventAdmin !");
    }
}
