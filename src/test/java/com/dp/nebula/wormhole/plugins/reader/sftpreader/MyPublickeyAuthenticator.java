package com.dp.nebula.wormhole.plugins.reader.sftpreader;

import org.apache.sshd.server.PublickeyAuthenticator;
import org.apache.sshd.server.session.ServerSession;

import java.security.PublicKey;

public class MyPublickeyAuthenticator implements PublickeyAuthenticator {
    public boolean authenticate(String s, PublicKey publicKey, ServerSession serverSession) {
        return false;
    }
}