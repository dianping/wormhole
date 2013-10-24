package com.dp.nebula.wormhole.plugins.reader.sftpreader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.command.ScpCommandFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.sftp.SftpSubsystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.nebula.wormhole.common.BufferedLineExchanger;
import com.dp.nebula.wormhole.common.BufferedLineExchangerTest;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.writer.sftpwriter.SftpWriterPeriphery;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Logger;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class SftpReaderTest {
	private SshServer sshd;
    
    private String server = "localhost";
    private int port = 58424;
    private String login = "username";
    private String password = "password";
    private Session session;

    @Before
    public void setUp() throws Exception {
        // Init sftp server stuff
        sshd = SshServer.setUpDefaultServer();
        sshd.setPort(port);
        sshd.setPasswordAuthenticator(new MyPasswordAuthenticator());
        sshd.setPublickeyAuthenticator(new MyPublickeyAuthenticator());
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystem.Factory()));
        sshd.setCommandFactory(new ScpCommandFactory());
        sshd.start();
        
        JSch sch = new JSch();
        sch.setLogger(new Logger() {
            public boolean isEnabled(int i) {
                return false;
            }

            public void log(int i, String s) {
                System.out.println("Log(jsch," + i + "): " + s);
            }
        });
        session = sch.getSession("username", "localhost", port);
        session.setUserInfo(new UserInfo() {
            public String getPassphrase() {
                return null;
            }

            public String getPassword() {
                return "password";
            }

            public boolean promptPassword(String message) {
                return true;
            }

            public boolean promptPassphrase(String message) {
                return false;
            }

            public boolean promptYesNo(String message) {
                return true;
            }

            public void showMessage(String message) {
            }
        });
       
    }

    @After
    public void tearDown() throws Exception {
        sshd.stop();
    }

    @Test
    public void testReader() throws Exception {
        String []data = {"Jim\t21\n","Tom\t22\n","Jack\t23\n"};
        String rootName = "/wormholeTest/read";
        File root = new File(rootName);
        root.mkdirs();
        String sourceName1 = rootName + "/in1.txt";
        String sourceName2 = rootName + "/in2.txt";

        File source1 = new File(sourceName1);
        File source2 = new File(sourceName2);
        session.connect();
        sendFile(sourceName1, data[0]+data[1]+data[2]);
        sendFile(sourceName2, data[0]+data[1]+data[2]);
        session.disconnect();


        SftpReader reader = new SftpReader();
        Map<String,String> params = new HashMap<String,String>();
        params.put(ParamKey.dir, "sftp://username@localhost:58424" + rootName);
        params.put(ParamKey.password, "password");
        params.put(ParamKey.colFilter, "#0,#1");
        
        IParam param = new DefaultParam(params);
        
        SftpDirSplitter splitter = new SftpDirSplitter();
        splitter.init(param);
        List<IParam> list = splitter.split();
        BufferedLineExchanger exchanger=BufferedLineExchangerTest.getLineExchanger();
        List<String> result = new ArrayList<String>();

        for(IParam paramItem:list) {
//        	System.out.println(paramItem.getValue(ParamKey.dir));
            
        	reader.setParam(paramItem);
            reader.init();
            reader.connection();
            reader.read(exchanger);
            reader.finish();
            ILine line = null;
            try{
            	int i = 0;
            	while((line = exchanger.receive()) != null) {
            		result.add(line.getField(0)+"\t"+line.getField(1)+"\n");
            		i++;
            	}
            }catch(Exception e) {
            	//Do nothing
            }
        }
        for(String str:data) {
        	assertTrue(result.contains(str));
        }
        source1.delete();
        source2.delete();
        root.delete();
        assertFalse(source1.exists());
        assertFalse(source2.exists());
        assertFalse(root.exists());
    }
    
    protected void sendFile(String path, String data) throws Exception {
        ChannelSftp c = (ChannelSftp) session.openChannel("sftp");
        c.connect();
        c.put(new ByteArrayInputStream(data.getBytes()), path);
        c.disconnect();
    }
}	
