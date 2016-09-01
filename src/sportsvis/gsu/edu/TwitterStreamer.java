package sportsvis.gsu.edu;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.HashtagEntity;
import twitter4j.URLEntity;

public class TwitterStreamer
{
	public static void main(String[] args)
	{
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		final DBConnector db = new DBConnector();
        db.connet();
		
        int totalN = 0;
        
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
            	
                                //status.get
            	
            	//System.out.println(totalN++  +  ":");
                String statusJson = TwitterObjectFactory.getRawJSON(status);
                //System.out.println(statusJson.length());
                long tweet_id = status.getId();
                String usrname = status.getUser().getScreenName();
                long uid = status.getUser().getId();
                String text = status.getText();
                //String date = status.getCreatedAt().toString();
                int favorCount = status.getFavoriteCount();
                HashtagEntity[] hashTags = status.getHashtagEntities();
                String hashTagStr = "";
                for (HashtagEntity s : hashTags)
                {
                	hashTagStr += s.getText() + " ";
                }
                //JSON String to JSONObject
				//JSONObject JSON_complete = new JSONObject(statusJson);
				//System.out.println("@:" + JSON_complete.getJSONObject("user"));
				//SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // your template here
				//java.util.Date dateStr = formatter.parse(date);
				//java.sql.Date dateDB = new java.sql.Date(dateStr.getTime());
				String sql = "INSERT INTO `SportVis`.`test` (`Time`, `TweetJson`, "
						   + "`text`, `date`,`screenname`,`tweet_id`,`lang`,`is_rt`,`is_retweeted`"
						   + ",`rt_count`, `like_count`, `twitter_uid`, `hashtags`, `source`,`urls`"
						   + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
				//if (status.getRetweetCount() == 0) return;
				//System.out.println("-----------start---------");
				
				//System.out.println("@" + status.getUser().getScreenName() + " - :" + status.getCreatedAt() + " = " + status.getText());

				java.util.Date dt = status.getCreatedAt();

				java.text.SimpleDateFormat sdf = 
				     new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				/*
				for (HashtagEntity hashtag : status.getHashtagEntities()) {
			        System.out.println("HashTag:" + hashtag.getText());
			    }*/
				
				String urlStr = "";
				for (URLEntity url : status.getURLEntities()) {
					urlStr += url.getDisplayURL() + ";";
			    }
				
				/*
				System.out.println("source:" + status.getSource());
				
				
				System.out.println("how many contributors:"+status.getContributors().length);
				
				System.out.println("RT#:" + status.getRetweetCount());
				
				System.out.println("tweetid:" + status.getId() );
				
				System.out.println("userid:" + status.getUser().getId());
				
				System.out.println("username:" + status.getUser().getName());
				
				System.out.println("" + status.getRetweetedStatus());
				
				System.out.println("-----------end---------");*/
				String currentTime = sdf.format(dt);
				
				/**
				 * "INSERT INTO `SportVis`.`test` (`Time`, `TweetJson`, "
						   + "`text`, `date`,`screenname`,`tweet_id`,`lang`,`is_rt`,`is_retweeted`"
						   + ",`rt_count`, `like_count`, `twitter_uid`, `hashtags`, `source`"
						   + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
				 * **/
				db.prepare(sql);
				db.setTimeStamp(1, getCurrentTimeStamp());
				db.setString(2, statusJson);
				db.setString(3, text);
				db.setString(4, currentTime);
				db.setString(5, usrname);
				db.setLong(6, tweet_id);
				db.setString(7, status.getLang());
				db.setBoolean(8, status.isRetweet()? true : false);
				db.setBoolean(9, status.isRetweeted()? true : false);
				db.setInt(10, status.getRetweetCount());
				db.setInt(11, status.getFavoriteCount());
				db.setLong(12,uid);
				db.setString(13, hashTagStr);
				db.setString(14, status.getSource().replaceAll("\\<.*?>",""));
				db.setString(15, urlStr);
				db.updateDB();
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                //System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        
        FilterQuery fq = new FilterQuery();
        String tracks[] = {"#WednesdayWisdom", "#DeadlineDay", "#GirlsInTheHouse3", "ISIS", "CUBA", "#IGetDepressedWhen"};
        //fq.track(keyWords);
        twitterStream.addListener(listener);
        //twitterStream.f
        fq.track(tracks);
        twitterStream.filter(fq);
		
	}
	
	public static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}
}
