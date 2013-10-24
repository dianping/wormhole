package com.dp.nebula.wormhole.plugins.reader.sftpreader;

import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.session.ServerSession;

/**
 * Very basic PasswordAuthenticator used for unit tests.
 */
public class MyPasswordAuthenticator implements PasswordAuthenticator {

    public boolean authenticate(String username, String password, ServerSession session) {
        boolean retour = false;

        if ("username".equals(username) && "password".equals(password)) {
            retour = true;
        }

        return retour;
    }
}