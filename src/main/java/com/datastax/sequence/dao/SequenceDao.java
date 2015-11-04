package com.datastax.sequence.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SequenceDao {
		
	private Session session;
	
	private static String keyspaceName = "datastax_sequence_demo";
	private static String seqtable = keyspaceName + ".sequence";

	private String UPDATE_SEQUENCE = "update " + seqtable+ " set sequence = ? where id = 'sequence' if sequence = ?";
	private String READ_FROM_SEQUENCE = "select sequence from " + seqtable + " where id = 'sequence'";
	
	
	private PreparedStatement update;
	private PreparedStatement read;
	
	public SequenceDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)
				.build();
		
		this.session = cluster.connect();

		this.update = session.prepare(UPDATE_SEQUENCE);
		this.read = session.prepare(READ_FROM_SEQUENCE);		
	}

	public int read(){
		ResultSet resultSet = session.execute(read.bind());
		Row row = resultSet.one();
		if (row == null ){
			return 1;
		}else{
			return row.getInt("sequence");
		}
	}
	
	public boolean update(int newSequence, int oldSequence){
		ResultSet resultSet = this.session.execute(update.bind(newSequence, oldSequence));		
		if (resultSet != null){
			Row row = resultSet.one();
			return row.getBool(0);
		}
		return true;
	}
}
