package com.dp.nebula.wormhole.plugins.writer.mysqlwriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import org.apache.log4j.Logger;

public class MysqlWriterInputStreamAdapter extends InputStream {
    
    private ILineReceiver receiver = null;
        
	private int lineCounter = 0;
    /* 列分隔符 */
    private static final char SEP = '\t';
    /* 行分隔符 */
    private static final char BREAK = '\n';

    private String encoding = "UTF8";
    
    private ILine line = null;
    /* 从line中获取一行数据暂存数组*/
    private byte buffer[] = null;
    
    private StringBuilder lineBuilder = new StringBuilder(1024 * 1024 * 8);
    
    /* 存放上次余下 */
    private byte[] previous = new byte[1024 * 1024 * 8];
    /* 上次余留数据长度 */
    private int preLen = 0;
    /* 上次余留数据起始偏移 */
    private int preOff = 0;
    
    private String [] addFields;
    
    private String writerID;

	private Logger logger = Logger.getLogger(MysqlWriterInputStreamAdapter.class);
    
    public MysqlWriterInputStreamAdapter(String writerID,ILineReceiver reader, MysqlLoader writer,final String[] fields) {
        super();
        this.writerID = writerID;
        this.receiver = reader;
        this.encoding = writer.getEncoding();
        if(fields == null) {
        	this.addFields = new String[0];
        }
        this.addFields = Arrays.copyOf(fields, fields.length);
    }

    
    @Override
    public int read(byte[] buff, int off, int len) throws IOException {
        if (off < 0 || len < 0 || len > buff.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        
        int total = 0;
        int read = 0;
        while (len > 0) {
            read = this.fetchLine(buff, off, len);
            if (read < 0) {
                break;
            }
            off += read;
            len -= read;
            total += read;
        }
        
        if (total == 0) {
          return (-1);
        }

        return total;
    }

	private void buildString(ILine line) {
		lineBuilder.setLength(0);
		String field;
	    int num = line.getFieldNum();
	    int len =0;
	    if(addFields == null ) {
	    	len = num;
	    }
	    else {
	    	len = addFields.length;
	    }
	    for (int i = 0,j = 0;i < len; i++) {
	    	if(addFields!=null && addFields[i] != null) {
	    		field = addFields[i];
	    	}
	    	else if(j < num){
	    		field = line.getField(j);
	    		j++;
	    	}
	    	else {
	    		logger.error(writerID + ": fields number is less than column number");
	    		throw new WormholeException("MysqlWriter: Fields number is less than column number ",JobStatus.WRITE_FAILED.getStatus(),writerID);
	    	}
	        if (null != field) {
	        	StringBuilder sb = new StringBuilder();
	        	char[] characters = field.toCharArray();
	        	for(int k = 0; k < characters.length; k++) {
	        		if(characters[k] == '\\') {
	        			sb.append("\\\\");
	        		}
	        		else if(characters[k] == '\r') {
	        			sb.append("\\r");
	        		}
	        		else if(characters[k] == '\n') {
	        			sb.append("\\n");
	        		}
	        		else if(characters[k] == SEP) {
	        			sb.append("\\t");
	        		}
	        		else {
	        			sb.append(characters[k]);
	        		}
	        	}
	            lineBuilder.append(sb.toString());
	        } else {
	            lineBuilder.append("\\N");
	        }
	        if(i < len - 1) {
	        	lineBuilder.append(SEP);
	        } else {
	            lineBuilder.append(BREAK);
	        }
	    }
	}
    
    private int fetchLine(byte[] buff, int off, int len) throws UnsupportedEncodingException {
        /* it seems like I am doing C coding. */
        int ret = 0;
        /* record current fetch len */
        int currLen;
        
        /* 查看上次是否有剩余 */
        if (this.preLen > 0) {
            currLen = Math.min(this.preLen, len);
            System.arraycopy(this.previous, this.preOff, buff, off, currLen);
            this.preOff += currLen;
            this.preLen -= currLen;
            off += currLen;
            len -= currLen;
            ret += currLen;
            
            /* 如果buff比较小，上次余下的数据 */
            if (this.preLen > 0) {
                return ret;
            }
        }
        
        /* 本次读数据的逻辑 */
        int lineLen;
        int lineOff = 0;
        line = this.receiver.receive();
        /* line为空，表明数据已全部读完 */
        if (line == null) {
            if (ret == 0) {
                return (-1);
            }
            return ret;
        }

		this.lineCounter++;
        this.buildString(line);
        this.buffer = lineBuilder.toString().getBytes(this.encoding);
        lineLen = this.buffer.length;
        currLen = Math.min(lineLen, len);
        System.arraycopy(this.buffer, 0, buff, off, currLen);
        len -= currLen;
        lineOff +=currLen;
        lineLen -= currLen;
        ret += currLen;
        /* len > 0 表明这次fetchLine还没有将buff填充完毕, buff有剩佄1�7 留作下次填充 */
        if (len > 0) {
            return ret;
        }
        
        /* 该buffer已经不够放一个line，因此把line的内容保存下来，供下丄1�7次fetch使用 
         * 这里的假设是previous绝对够容纳一个line */
        /* fix bug: */
        if (lineLen > this.previous.length) {
            this.previous = new byte[lineLen << 1];
        }
        System.arraycopy(this.buffer, lineOff, this.previous, 0, lineLen);
        this.preOff = 0;
        this.preLen = lineLen;
        return (ret);
    }
    
    @Override
    public int read() throws IOException {
        /*
         * 注意: 没有实现read()
         * */
        throw new IOException("Read() is not supported");
    }
    
	public int getLineNumber() {
		return this.lineCounter;
	}
}
