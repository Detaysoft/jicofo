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
package org.jitsi.jicofo.jigasi;

import net.java.sip.communicator.impl.protocol.jabber.extensions.colibri.*;
import net.java.sip.communicator.util.*;
import org.jitsi.jicofo.*;
import org.jitsi.jicofo.xmpp.*;
import org.jitsi.service.configuration.*;

/**
 * @author Damian Minkov
 */
public class JigasiDetector
    extends BaseBrewery<ColibriStatsExtension>
{
    /**
     * The logger
     */
    private static final Logger logger = Logger.getLogger(JigasiDetector.class);

    /**
     * The name of config property which provides the name of the MUC room in
     * which all Jigasi instances.
     */
    private static final String JIGASI_ROOM_PNAME
        = "org.jitsi.jicofo.jigasi.BREWERY";

    /**
     * Loads the name of Jigasi brewery MUC room from the configuration.
     * @param config the instance of <tt>ConfigurationService</tt> which will be
     * used to read the properties required.
     * @return the name of Jigasi brewery or <tt>null</tt> if none configured.
     */
    static public String loadBreweryName(ConfigurationService config)
    {
        return config.getString(JIGASI_ROOM_PNAME);
    }

    /**
     * Constructs new JigasiDetector.
     *
     * @param protocolProvider the xmpp protocol provider
     * @param breweryName the room name
     */
    public JigasiDetector(
        ProtocolProviderHandler protocolProvider,
        String breweryName)
    {
        super(protocolProvider,
            breweryName,
            ColibriStatsExtension.ELEMENT_NAME,
            ColibriStatsExtension.NAMESPACE,
            false);
    }

    @Override
    protected void onInstanceStatusChanged(
        String mucJid,
        ColibriStatsExtension status)
    {}

    @Override
    protected void notifyInstanceOffline(String jid)
    {}

    /**
     * Selects the jigasi instance that is less loaded.
     *
     * @return XMPP address of Jigasi instance or <tt>null</tt> if there are
     * no Jigasis available currently.
     */
    public String selectJigasi()
    {
        BrewInstance lessLoadedInstance = null;
        int numberOfParticipants = Integer.MAX_VALUE;
        for (BrewInstance jigasi : instances)
        {
            int currentParticipants
                    = jigasi.status != null ?
                jigasi.status.getAttributeAsInt("participants")
                : 0;
            if (currentParticipants < numberOfParticipants)
            {
                numberOfParticipants = currentParticipants;
                lessLoadedInstance = jigasi;
            }
        }

        return lessLoadedInstance != null ? lessLoadedInstance.mucJid : null;
    }
}
