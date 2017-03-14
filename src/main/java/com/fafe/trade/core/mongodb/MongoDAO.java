package com.fafe.trade.core.mongodb;

import java.io.File;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import com.fafe.core.StockMarket;
import com.fafe.core.properties.ConfigLoader;
import com.fafe.core.properties.ConfigProperty;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class MongoDAO {
	private static ConfigLoader cl;
	private static MongoClient mongo;
	public static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	private List<String> DATES = new LinkedList<String>();
	private final StockMarket stockMkt;

	public MongoDAO(StockMarket mkt) throws Exception {
		cl = new ConfigLoader();
		
		this.stockMkt = mkt;
		mongo = new MongoClient(cl.getStrProperty(ConfigProperty.MONGO_HOST), Integer.parseInt(cl.getStrProperty(ConfigProperty.MONGO_PORT)));

		populateDate();
		
		if(Boolean.parseBoolean(cl.getStrProperty(ConfigProperty.MONGO_AUTH))){
			System.out.println("MongoDB Authentication required");
			if(!getTradeDB().authenticate(cl.getStrProperty(ConfigProperty.MONGO_USER), cl.getStrProperty(ConfigProperty.MONGO_PWD).toCharArray())){
				throw new Exception("Login failed");
			} else{
				System.out.println("User authenticated");
			}
		}else{
			System.out.println("MongoDB Authentication not required");
		}
			
	}

	private void populateDate() {

		String dateFile = cl.getStrProperty(ConfigProperty.DATE_PATH) + stockMkt.getAcr() + "_dates.txt";
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(dateFile));
			while (scanner.hasNextLine()) {
				DATES.add(scanner.nextLine());
			}
		} catch (Exception e) {
			System.err.println("Cannot read the file " + dateFile);
			DATES = null;
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
	}

	private MongoClient getClient() throws UnknownHostException {
		return mongo;
	}

	private DB getTradeDB() throws UnknownHostException {
		return getClient().getDB(cl.getStrProperty(ConfigProperty.MONGO_DB));
	}

	public Date getCurrentDate() throws Exception {
		DBCollection table = getTradeDB().getCollection(stockMkt.getAcr().toLowerCase() + "_date");

		DBObject wrk_date = table.findOne();
		System.out.println("current date " + wrk_date.get("wrk_date"));

		return sdf.parse((String) wrk_date.get("wrk_date"));
	}

	public void updateDate() throws Exception {
		String currentDate = sdf.format(getCurrentDate());
		int indexOfCurrentDate = DATES.indexOf(currentDate);
		String nextDate = DATES.get(indexOfCurrentDate + 1);

		System.out.println("Found the next date " + nextDate);

		DBCollection table = getTradeDB().getCollection(stockMkt.getAcr().toLowerCase() + "_date");
		DBObject wrk_date = table.findOne();

		BasicDBObject updateObj = new BasicDBObject();
		updateObj.append("$set", new BasicDBObject().append("wrk_date", nextDate));

		table.update(wrk_date, updateObj);
		System.out.println("Current date update to " + nextDate);
	}

	public Date getAdjustedDate(int days) throws Exception {
		return sdf.parse(DATES.get(DATES.indexOf(sdf.format(getCurrentDate())) + days));
	}

	public static void main(String[] args) {
		MongoDAO dao;
		try {
			dao = new MongoDAO(StockMarket.NYSE);
			dao.getCurrentDate();
			dao.updateDate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
