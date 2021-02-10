package com.bfxy.canal.base;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

/**
 *	CanalTest
 */
public class CanalTest {

	/**
	 * 	1. 	连接Canal服务器
	 * 	2.	向Master请求dump协议
	 * 	3.	把发送过来的binlog进行解析
	 * 	4.	最后做实际的处境处理...发送到MQ Print...
	 * @param args
	 */
	public static void main(String[] args) {
		
		CanalConnector connector = CanalConnectors.newSingleConnector(
				new InetSocketAddress("192.168.11.221", 11111), 
				"example", "root", "root");
		
		int batchSize = 1000; //	拉取数据量
		int emptyCount = 0;
		try {
			//	连接我们的canal服务器
			connector.connect();
			//	订阅什么内容? 什么库表的内容？？
			connector.subscribe(".*\\..*");
			//	出现问题直接进行回滚操作
			connector.rollback();
			
			int totalEmptyCount = 1200;
			
			while(emptyCount < totalEmptyCount) {
				Message message = connector.getWithoutAck(batchSize);
				//	用于处理完数据后进行ACK提交动作
				long batchId = message.getId();
				int size = message.getEntries().size();
				if(batchId == -1 || size == 0) {
					emptyCount++;
					System.err.println("empty count: " + emptyCount);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// ignore..
					}
				} else {
					emptyCount = 0;
					System.err.printf("message[batchId=%s, size=%s] \n", batchId, size);
					//	处理解析数据
					printEnrty(message.getEntries());
				}
				//	确认提交处理后的数据
				connector.ack(batchId);
			}
			System.err.println("empty too many times, exit");
			
		} finally {
			//	关闭连接
			connector.disconnect();
		}
		
	}

	private static void printEnrty(List<Entry> entries) {
		for(Entry entry : entries) {
			// 	如果EntryType 当前处于事务的过程中 那就不能处理
			if(entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
				continue;
			}
			
			//	rc里面包含很多信息：存储数据库、表、binlog
			RowChange rc = null;
			try {
				//	二进制的数据
				rc = RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {
				throw new RuntimeException("parser error!");
			}
			EventType eventType = rc.getEventType();
			
			System.err.println(String.format("binlog[%s:%s], name[%s,%s], eventType : %s", 
					entry.getHeader().getLogfileName(),
					entry.getHeader().getLogfileOffset(),
					entry.getHeader().getSchemaName(),
					entry.getHeader().getTableName(),
					eventType));
			
			
			//	真正的对数据进行处理
			for(RowData rd : rc.getRowDatasList()) {
				if(eventType == EventType.DELETE) {
					//	delete操作 BeforeColumnsList
					List<Column> deleteList = rd.getBeforeColumnsList();
					printColumn(deleteList);
					
				} else if(eventType == EventType.INSERT) {
					//	insert操作AfterColumnsList
					List<Column> insertList = rd.getAfterColumnsList();
					printColumn(insertList);
				} 
				//	update
				else {
					List<Column> updateBeforeList = rd.getBeforeColumnsList();
					printColumn(updateBeforeList);
					List<Column> updateAfterList = rd.getAfterColumnsList();
					printColumn(updateAfterList);
				}
			}
		}
	}
	
	private static void printColumn(List<Column> columns) {
		for(Column column: columns) {
			System.err.println(column.getName() 
					+ " : " 
					+ column.getValue() 
					+ ", update = " 
					+ column.getUpdated());
		}
	}
	
	
}
