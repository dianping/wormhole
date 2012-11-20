package com.dp.nebula.wormhole.plugins.writer.sftpwriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.dp.nebula.wormhole.common.DefaultLine;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.reader.sftpreader.MyPasswordAuthenticator;
import com.dp.nebula.wormhole.plugins.reader.sftpreader.MyPublickeyAuthenticator;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Logger;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class sftpWriterTest {
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

    private List<ILine> generateDatas() {
    	List<ILine> datas = new ArrayList<ILine>();
        ILine data = new DefaultLine();
        data.addField("Jim");
        data.addField("21");
        datas.add(data);
        data = new DefaultLine();
        data.addField("Tom");
        data.addField("22");
        datas.add(data);
        data = new DefaultLine();
        data.addField("Jack");
        data.addField("23");
        datas.add(data);
        return datas;
    }
    
    @Test
    public void testWriter() throws Exception {
        String []data = {"Jim\t21\n","Tom\t22\n","Jack\t23\n"};
        List<ILine> datas = generateDatas();

        String rootName = "/wormholeTest/write";
        File root = new File(rootName);
        root.mkdirs();
        
        SftpWriterPeriphery periphery = new SftpWriterPeriphery();
        SftpDirSplitter splitter = new SftpDirSplitter();
        SftpWriter writer = new SftpWriter();
        
        
        Map<String,String> params = new HashMap<String,String>();
        params.put(ParamKey.dir, "sftp://username@localhost:58424" + rootName);
        params.put(ParamKey.password, "password");
        params.put(ParamKey.concurrency, "3");

        IParam param = new DefaultParam(params);
        periphery.prepare(param, null);
        splitter.init(param);
        List<IParam> paramList = splitter.split();
    	BufferedLineExchanger exchanger=BufferedLineExchangerTest.getLineExchanger();
    	for(ILine line:datas) {
    		exchanger.send(line);
    		exchanger.flush();
    	}

        for(IParam oneParam:paramList) {
        	writer.setParam(oneParam);
        	writer.init();
        	writer.connection();
        	writer.write(exchanger);
        	writer.commit();
        	writer.finish();
        }
        assertEquals("Jack\t23\nTom\t22\nJim\t21\n",readFile( rootName+"/part-0"));
        new File(rootName + "/part-0").delete();
        new File(rootName + "/part-1").delete();
        new File(rootName + "/part-2").delete();
        root.delete();
        
//        String sourceName1 = rootName + "/in1.txt";
//        String sourceName2 = rootName + "/in2.txt";
//
//        File source1 = new File(sourceName1);
//        File source2 = new File(sourceName2);
//        session.connect();
//        sendFile(sourceName1, data[0]+data[1]+data[2]);
//        sendFile(sourceName2, data[0]+data[1]+data[2]);
//        session.disconnect();
//
//
//        SftpReader reader = new SftpReader();
//        Map<String,String> params = new HashMap<String,String>();
//        params.put(ParamKey.dir, "sftp://username@localhost:58424" + rootName+"/*1.txt");
//        params.put(ParamKey.password, "password");
//        params.put(ParamKey.colFilter, "#0,#1");
//        
//        IParam param = new DefaultParam(params);
//        
//        SftpDirSplitter splitter = new SftpDirSplitter();
//        splitter.init(param);
//        List<IParam> list = splitter.split();
//        for(IParam paramItem:list) {
//        	System.out.println("!!!!");
//            BufferedLineExchanger exchanger=BufferedLineExchangerTest.getLineExchanger();
//        	reader.setParam(paramItem);
//            reader.init();
//            reader.connection();
//            reader.read(exchanger);
//            reader.finish();
//            ILine line = null;
//            List<String> result = new ArrayList<String>();
//            try{
//            	int i = 0;
//            	while((line = exchanger.receive()) != null) {
//            		result.add(line.getField(0)+"\t"+line.getField(1)+"\n");
//            		i++;
//            	}
//            }catch(Exception e) {
//            	//Do nothing
//            }
//
//            for(String str:data) {
//            	assertTrue(result.contains(str));
//            }
//        }
//        source1.delete();
//        source2.delete();
//        root.delete();
//        assertFalse(source1.exists());
//        assertFalse(source2.exists());
//        assertFalse(root.exists());
    }
   
    protected String readFile(String path) throws Exception {
    	session.connect();
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
        ChannelSftp c = (ChannelSftp) session.openChannel("sftp");
        c.connect();
        c.get(path,out);
        c.disconnect();
    	session.disconnect();
    	new String();
    	return new String(out.toByteArray());
    }
}
