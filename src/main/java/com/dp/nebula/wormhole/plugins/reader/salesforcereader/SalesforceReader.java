package com.dp.nebula.wormhole.plugins.reader.salesforcereader;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.plugins.reader.salesforcereader.SOQLInfo.SOQLFieldInfo;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;

public class SalesforceReader extends AbstractPlugin implements IReader {
	private static final Logger LOG = Logger.getLogger(SalesforceReader.class);

	private final static EncryptionUtil encrypter = new EncryptionUtil();
	private final static String END_POINT = "https://login.salesforce.com/services/Soap/u/27.0";
	private final static int DISCARDED_FIELD_COUNT = 2;

	private final Map<String, DescribeSObjectResult> entityDescribes = new HashMap<String, DescribeSObjectResult>();
	private final Map<String, DescribeGlobalSObjectResult> describeGlobalResults = new HashMap<String, DescribeGlobalSObjectResult>();
	private final Map<String, DescribeRefObject> referenceDescribes = new HashMap<String, DescribeRefObject>();

	private DescribeGlobalResult entityTypes;

	private String username = "";
	private String password = "";
	private String entity = "";
	private String extractionSOQL = "";
	private String encryptionKeyFile = "";
	private PartnerConnection conn;
	private SOQLInfo soqlInfo;
	private QueryResult qr;

	@Override
	public void init() {
		this.username = getParam().getValue(ParamKey.username, this.username);
		this.password = getParam().getValue(ParamKey.password, this.password);
		this.entity = getParam().getValue(ParamKey.entity, this.entity);
		this.extractionSOQL = getParam().getValue(ParamKey.extractionSOQL,
				this.extractionSOQL);
		this.encryptionKeyFile = getParam().getValue(
				ParamKey.encryptionKeyFile, this.encryptionKeyFile);
		if (encryptionKeyFile != null && encryptionKeyFile.length() != 0) {
			try {
				encrypter.setCipherKeyFromFilePath(encryptionKeyFile);
			} catch (IOException e) {
				LOG.error(e);
				throw new WormholeException(
						"Error initialize encrypter with encryptionKeyFile",
						JobStatus.CONF_FAILED.getStatus());
			}
		}
	}

	@Override
	public void connection() {
		boolean loggedIn = login();
		if (loggedIn) {
			try {
				setFieldTypes();
				setFieldReferenceDescribes();
				soqlInfo = new SOQLInfo(extractionSOQL);
				StringBuilder sb = new StringBuilder(500);
				sb.append("selected column names: ");
				for (SOQLFieldInfo fieldInfo : soqlInfo.getSelectedFields()) {
					String daoColumn = fieldInfo.getFieldName();
					if (daoColumn != null) {
						sb.append(daoColumn).append('\t');
					}
				}
				LOG.info(sb.toString());

			} catch (Exception e) {
				throw new WormholeException(e.getMessage(),
						JobStatus.READ_CONNECTION_FAILED.getStatus());
			}
		}

	}

	@Override
	public void finish() {
		try {
			conn.logout();
			LOG.info("Logged out.");
		} catch (ConnectionException ce) {
			LOG.error(ce);
		}
	}

	@Override
	public void read(ILineSender lineSender) {
		try {
			qr = conn.query(extractionSOQL);
			final int size = qr.getSize();
			if (size == 0) {
				LOG.info("None Records Returned");
			} else {
				LOG.info("There are " + size + " records.");
				while (qr.getRecords() != null) {
					final SObject[] sfdcResults = qr.getRecords();
					if (sfdcResults == null) {
						LOG.error("Error No Results");
						return;
					}
					for (int i = 0; i < sfdcResults.length; i++) {
						ILine oneLine = lineSender.createNewLine();
						Iterator<XmlObject> fields = sfdcResults[i].getChildren();
						if (fields == null)
							return;
						int index = 0;
						while (fields.hasNext()) {
							// discrading the ahead two fields
							if (index++ < DISCARDED_FIELD_COUNT){
								fields.next();
								continue;
							}
							
							XmlObject field = fields.next();
							Object newVal = convertFieldValue(field.getValue());
							if (newVal == null){
								newVal = "";
							}
							oneLine.addField(newVal.toString());
						}
						boolean flag = lineSender.send(oneLine);
						if (flag) {
							getMonitor().increaseSuccessLines();
						} else {
							getMonitor().increaseFailedLines();
						}
					}
					if (qr.getDone()){
						break;
					}
					qr = conn.queryMore(qr.getQueryLocator());
				}
			}
			lineSender.flush();
		} catch (ConnectionException e) {
			throw new WormholeException("Error Query extractionSOQL",
					JobStatus.READ_CONNECTION_FAILED.getStatus());
		}

	}

	private Boolean login() {
		password = decryptPassword(password);
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(username);
		config.setPassword(password);
		config.setAuthEndpoint(END_POINT);
		config.setServiceEndpoint(END_POINT);
		try {
			conn = Connector.newConnection(config);
			LoginResult loginResult = conn.login(username, password);
			if (loginResult.getPasswordExpired()) {
				throw new WormholeException("Error Expired Password",
						JobStatus.CONF_FAILED.getStatus());
			}
			conn.setSessionHeader(loginResult.getSessionId());
		} catch (ConnectionException e) {
			LOG.error("Error Login " + e.getMessage());
			throw new WormholeException("Error Login " + e.getMessage(),
					JobStatus.CONF_FAILED.getStatus());
		}
		return true;
	}

	private void setFieldTypes() throws ConnectionException {
		describeSObject(entity);
	}

	private void setFieldReferenceDescribes() throws ConnectionException {
		referenceDescribes.clear();
		entityTypes = conn.describeGlobal();
		if (describeGlobalResults.isEmpty()) {
			for (DescribeGlobalSObjectResult res : entityTypes.getSobjects()) {
				if (res != null) {
					describeGlobalResults.put(res.getName(), res);
				}
			}
		}
		if (describeGlobalResults != null) {
			Field[] entityFields = entityDescribes.get(entity).getFields();
			for (Field entityField : entityFields) {
				// upsert on references (aka foreign keys) is supported only
				// 1. When field has relationship is set and refers to exactly
				// one object
				// 2. When field is either createable or updateable. If neither
				// is true, upsert will never work for that
				// relationship.
				if (entityField.isCreateable() || entityField.isUpdateable()) {
					String relationshipName = entityField.getRelationshipName();
					String[] referenceTos = entityField.getReferenceTo();
					if (referenceTos != null
							&& referenceTos.length == 1
							&& referenceTos[0] != null
							&& relationshipName != null
							&& relationshipName.length() > 0
							&& (entityField.isCreateable() || entityField
									.isUpdateable())) {

						String refEntityName = referenceTos[0];

						// make sure that the object is legal to upsert
						Field[] refObjectFields = describeSObject(refEntityName)
								.getFields();
						Map<String, Field> refFieldInfo = new HashMap<String, Field>();
						for (Field refField : refObjectFields) {
							if (refField.isExternalId()) {
								refField.setCreateable(entityField
										.isCreateable());
								refField.setUpdateable(entityField
										.isUpdateable());
								refFieldInfo.put(refField.getName(), refField);
							}
						}
						if (!refFieldInfo.isEmpty()) {
							DescribeRefObject describe = new DescribeRefObject(
									refEntityName, refFieldInfo);
							referenceDescribes.put(relationshipName, describe);
						}
					}
				}
			}
		}
	}

	private DescribeSObjectResult describeSObject(String entity)
			throws ConnectionException {
		DescribeSObjectResult result = entityDescribes.get(entity);
		if (result == null) {
			result = conn.describeSObject(entity);
			if (result != null) {
				entityDescribes.put(result.getName(), result);
			}
		}
		return result;
	}

	private String decryptPassword(String password) {
		if (password != null && password.length() > 0) {
			try {
				return encrypter.decryptString(password);
			} catch (Exception ex) {
				throw new WormholeException("Error decrypt password" + ex.getMessage(),
						JobStatus.CONF_FAILED.getStatus());
			}
		}
		return password;
	}

	private static final DateFormat DF = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	private Object convertFieldValue(Object fieldVal) {
		if (fieldVal instanceof Calendar) {
			DF.setCalendar((Calendar) fieldVal);
			return DF.format(((Calendar) fieldVal).getTime());
		}

		if (fieldVal instanceof Date) {
			final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			df.setTimeZone(TimeZone.getTimeZone("GMT"));
			return df.format((Date) fieldVal);
		}
		return fieldVal;
	}
}
